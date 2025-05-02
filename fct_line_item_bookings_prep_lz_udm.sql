{{ config(
    snowflake_warehouse=warehouse("L"),
    tags=["canonical_prep"]
    ) }}

WITH
--calculate first payment approved date for each invoice_id,
--flag invoice id's that have a third party gateway payment
first_payment_approved AS (
	SELECT
		invoice_id,
		MIN(transaction_settled_at_utc) AS invoice_first_payment_approved_at_utc,
		MIN(CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', transaction_settled_at_utc)) AS invoice_first_payment_approved_at_pt,
		invoice_first_payment_approved_at_utc::DATE AS invoice_first_payment_approved_date_utc,
		invoice_first_payment_approved_at_pt::DATE AS invoice_first_payment_approved_date_pt
	FROM {{ ref('fct_payment_transaction_hist_prep_udm') }}
	WHERE transaction_status = 'Approved'
		AND transaction_type = 'Payment'
	GROUP BY 1
),

line_item_hist AS (
	SELECT *
	FROM (
		SELECT
			flih.line_item_hist_id,
			flih.line_item_id,
			flih.invoice_id,
			flih.workspace_id,
			flih.cart_line_item_id,
			flih.subscription_id,
			fpa.invoice_first_payment_approved_at_utc,
			fpa.invoice_first_payment_approved_date_utc,
			fpa.invoice_first_payment_approved_at_pt,
			fpa.invoice_first_payment_approved_date_pt,
			COALESCE(flih.is_cancelled,FALSE) AS is_cancelled,
			flih.line_item_updated_at_utc,
			flih.line_item_updated_at_pt,
			CONVERT_TIMEZONE('UTC','America/Los_Angeles',COALESCE(flih.line_item_updated_at_utc,flih.line_item_created_at_utc)) AS record_updated_at_pt, --needed to account for missing updated_at_pt dates, in additions to source data issues where updated_at_pt is after FPA
			record_updated_at_pt::DATE AS record_updated_date_pt,
			CASE
				WHEN fpa.invoice_first_payment_approved_at_pt IS NULL
					THEN '9999-12-31'
				ELSE GREATEST(fpa.invoice_first_payment_approved_at_pt,COALESCE(MIN(flih.line_item_updated_at_pt) OVER (PARTITION BY flih.invoice_id),'1900-01-01'))
			END AS original_invoice_fpa_cuttoff_at_pt,
			flih.price_point_id,
			flih.user_id,
			flih.line_item_created_at_utc,
			flih.line_item_created_at_pt,
			COALESCE(flih.amount,0) AS amount,
			pp.is_filing_fee,
			CASE WHEN SUM(flih.amount) OVER (PARTITION BY flih.invoice_id) > 0 THEN FALSE ELSE TRUE END AS is_free_order_id,
			flih.is_current_record,
			LAG(is_cancelled) OVER (PARTITION BY flih.line_item_id ORDER BY record_updated_at_pt) AS previous_status,
			LAG(flih.amount) OVER (PARTITION BY flih.line_item_id ORDER BY record_updated_at_pt) AS previous_price,
			flih.cart_price_point_id,
			flih.root_price_point_id,
			flih.source_cart_line_item_id,
			flih.source_invoice_id,
			flih.source_line_item_id,
			flih.source_subscription_id,
			flih.source_user_id,
			flih.source_system
		FROM {{ ref('fct_line_item_hist_prep_lz_udm')}} flih
		LEFT JOIN first_payment_approved fpa --left join FPA, so we don't filter out free basic llc invoices (see qualify condition below)
			ON fpa.invoice_id = flih.invoice_id
		LEFT JOIN
			{{ source('udm','price_point') }} pp
			ON flih.price_point_id = pp.price_point_id
	)
	WHERE
		--only interested in invoice item hist records WHERE cancelled status changes OR price changes
		(is_cancelled <> previous_status OR previous_status IS NULL) --only show rows WHERE the cancelled status isn't the same AS the previous hist record
		OR (amount <> previous_price OR previous_price IS NULL) --only show rows WHERE the previous price isn't the same AS the previous hist record
),

--add row expired AND started timestamps, later used to figure out what was in effect before AND after first approved date.
add_started_and_expired_at AS (
	SELECT
		*,
		record_updated_at_pt AS record_started_at_pt,
		LEAD(record_updated_at_pt, 1)
			OVER (PARTITION BY line_item_id ORDER BY record_updated_at_pt ASC) AS record_expires_at_pt
	FROM line_item_hist
),

classify_line_items AS (
	SELECT
		t.*,
		t.record_started_at_pt::DATE AS record_started_date_pt,
		CASE
			WHEN t.is_cancelled = TRUE AND t.is_current_record = TRUE
				THEN t.record_started_at_pt:: DATE
			ELSE GREATEST(t.record_started_at_pt:: DATE, t.record_expires_at_pt:: DATE -1)
		END AS record_expires_date_pt,
		CASE
			WHEN t.is_filing_fee = TRUE
				THEN 0
			ELSE t.amount
		END AS extended_price_without_ff,
		CASE
			WHEN t.line_item_created_at_pt <= t.original_invoice_fpa_cuttoff_at_pt
				THEN TRUE
			ELSE FALSE
		END AS is_initial_line_item,      --TRUE if line_item created before first approved payment date
		CASE
			WHEN t.original_invoice_fpa_cuttoff_at_pt BETWEEN t.record_started_at_pt AND COALESCE(t.record_expires_at_pt, '9999-12-31')
				THEN TRUE
			ELSE FALSE
		END AS is_counted_in_initial_gbnf, --flag the hist records that count toward the initial gbnf AS TRUE
		CASE
			WHEN COALESCE(t.record_started_at_pt, '9999-12-31') > t.original_invoice_fpa_cuttoff_at_pt
				THEN TRUE
			ELSE FALSE
		END AS is_after_fpa_date
	--flag the hist records that count toward additions/subtractions after the first approved payment date AS TRUE
	FROM add_started_and_expired_at t
),

invoice_level_totals
AS (
	SELECT
		invoice_id,
		invoice_first_payment_approved_at_pt,
		record_updated_date_pt,
		SUM(CASE
			WHEN is_cancelled = TRUE AND cli.is_initial_line_item = TRUE AND
				cli.is_counted_in_initial_gbnf = FALSE AND cli.is_after_fpa_date = TRUE
				THEN cli.amount
			ELSE 0
		END)                                                    AS initial_invoice_cancelled_amount,
		SUM(CASE
			WHEN is_cancelled = FALSE AND cli.is_initial_line_item = TRUE AND
				cli.is_counted_in_initial_gbnf = TRUE AND cli.is_after_fpa_date = FALSE
				THEN cli.amount
			ELSE 0
		END)                                                    AS initial_invoice_amount,
		SUM(CASE
			WHEN is_cancelled = TRUE AND is_after_fpa_date = TRUE THEN cli.amount
			ELSE 0
		END)                                                    AS cancelled_invoice_amount,
		SUM(CASE
			WHEN is_cancelled = FALSE AND is_after_fpa_date = TRUE THEN cli.amount
			ELSE 0
		END)                                                    AS additional_invoice_amount
	FROM classify_line_items cli
	GROUP BY 1, 2, 3
),

discounts
AS (
	SELECT *
	FROM (
		SELECT
			fdh.discount_hist_id,
			fdh.discount_id,
			fdh.invoice_line_id AS line_item_id,
			fdh.invoice_id,
			COALESCE(dlih.cart_line_item_id,fdh.invoice_line_id) AS cart_line_item_id, --use the cart_line_item_id if it exists for CP1, else use the invoice_line_id for CP2. Will need to be revisited in UDM.
			COALESCE(fpa.invoice_first_payment_approved_at_pt,'9999-12-31') AS invoice_first_payment_approved_at_pt,
			fdh.discount_type_id,
			fdh.discount_type,
			fdh.discount_apply_type_id,
			fdh.discount_apply_type,
			fdh.discount_amount,
			fdh.is_cancelled,
			fdh.expires_at_utc,
			fdh.discount_hist_updated_at_utc,
			CONVERT_TIMEZONE('UTC','America/Los_Angeles',fdh.discount_hist_updated_at_utc) AS discount_hist_updated_at_pt,
			discount_hist_updated_at_pt::DATE                                               AS discount_hist_updated_date_pt,
			fdh.discount_created_at_utc,
			CONVERT_TIMEZONE('UTC','America/Los_Angeles',fdh.discount_created_at_utc)      AS discount_created_at_pt,
			discount_created_at_pt::DATE                                                    AS discount_created_date_pt,
			LAG(fdh.is_cancelled)
				OVER (PARTITION BY fdh.discount_id ORDER BY fdh.discount_hist_updated_at_utc) AS previous_is_cancelled,
			LAG(fdh.discount_amount)
				OVER (PARTITION BY fdh.discount_id ORDER BY fdh.discount_hist_updated_at_utc) AS previous_discount_amount
		FROM {{ ref('fct_discount_hist_udm') }} fdh
		LEFT JOIN first_payment_approved fpa
			ON fpa.invoice_id = fdh.invoice_id
		LEFT JOIN {{ ref('dim_line_item_hierarchy_udm') }} dlih
			ON fdh.invoice_line_id = dlih.line_item_id
	)
	--only interested in order discounts WHERE cancelled status changes OR discount amount changes
	WHERE (previous_is_cancelled <> is_cancelled OR previous_is_cancelled IS NULL)
		OR (previous_discount_amount <> discount_amount OR previous_discount_amount IS NULL)
),

--add row expired AND started timestamps, later used to figure out what was active before AND after first approved date.
add_started_and_expired_at_discount
AS (
	SELECT
		*,
		discount_hist_updated_at_pt AS record_started_at_pt,
		LEAD(discount_hist_updated_at_pt, 1)
			OVER (PARTITION BY discount_id ORDER BY discount_hist_updated_at_pt) AS record_expires_at_pt
	FROM discounts
),

classify_discounts
AS (
	SELECT
		*,
		CASE
			WHEN d.discount_created_at_pt <= d.invoice_first_payment_approved_at_pt THEN TRUE
			ELSE FALSE
		END AS is_initial_discount,        --TRUE if discount was created before the first payment approved date
		CASE
			WHEN COALESCE(d.discount_hist_updated_at_pt, '9999-12-31') > d.invoice_first_payment_approved_at_pt
				THEN TRUE
			ELSE FALSE
		END AS is_after_fpa_date           --flag the hist records that count toward additions/subtractions after the first approved payment date AS TRUE
	FROM add_started_and_expired_at_discount d
),

--calculate bookings that occurred prior to fpa date, excluding order items cancelled prior to fpa date
initial_bookings_line_item
AS (
	SELECT
		cli.invoice_id,
		cli.line_item_id,
		cli.user_id,
		cli.workspace_id,
		cli.invoice_first_payment_approved_at_pt,
		--need record_updated_at_pt for basic llc line items, where there isn't an invoice first approved payment date
		COALESCE(cli.invoice_first_payment_approved_at_pt,cli.record_updated_at_pt)               AS booking_at_pt,
		IFF(cli.is_cancelled = TRUE, 'Cancelled Initial Line Item Booking Before FPA', 'Initial Line Item Booking Before FPA') AS booking_category_detail,
		IFF(cli.is_cancelled = TRUE, 'Cancelled Line Item Booking', 'Line Item Booking') AS booking_category,
		cli.is_after_fpa_date,
		cli.cart_line_item_id,
		cli.subscription_id,
		cli.price_point_id,
		SUM(ilt.initial_invoice_cancelled_amount) AS initial_invoice_cancelled_amount,
		SUM(ilt.initial_invoice_amount) AS initial_invoice_amount,
		SUM(CASE
			WHEN cli.is_filing_fee = TRUE AND is_cancelled = TRUE THEN -cli.amount
			--for cp1 since these are based on hist records, we want to capture the negative amount for cancellations. Refunds / store credits are all captured through the store credits table for CP2.
			WHEN cli.is_filing_fee = TRUE AND is_cancelled = FALSE THEN cli.amount
			ELSE 0
		END)                                                            AS filing_fee,
		0                                                                              AS initial_discount_amount,
		0                                                                              AS additional_discount_amount,
		SUM(CASE
			WHEN cli.is_cancelled = FALSE AND is_counted_in_initial_gbnf = TRUE
				THEN extended_price_without_ff
			ELSE 0
		END)                                                           AS initial_gbnf,
		SUM(CASE
			WHEN cli.is_cancelled = FALSE AND is_counted_in_initial_gbnf = TRUE
				THEN amount --include filing_fees for gb / nb
			ELSE 0
		END)                                                           AS initial_gb,
		initial_gb                                                                     AS gb,
		initial_gb                                                                     AS nb, --gb and nb are the same for bookings before FPA date
		initial_gbnf                                                                   AS gbnf,
		initial_gbnf                                                                   AS nbnf --gbnf and nbnf are the same for bookings before FPA date
	FROM classify_line_items cli
	LEFT JOIN invoice_level_totals ilt
		ON ilt.invoice_id = cli.invoice_id
		AND ilt.record_updated_date_pt = cli.record_updated_date_pt
	WHERE cli.is_after_fpa_date = FALSE
	GROUP BY ALL
),

--calculate bookings that occurred after the first approved payment date
additional_bookings_line_item
AS (
	SELECT
		cli.invoice_id,
		cli.line_item_id,
		cli.user_id,
		cli.workspace_id,
		cli.invoice_first_payment_approved_at_pt,
		cli.record_updated_at_pt                                                           AS booking_at_pt,
		CASE
			WHEN cli.is_cancelled = TRUE AND cli.is_initial_line_item = TRUE
				THEN 'Cancelled Initial Line Item Booking After FPA'
			WHEN cli.is_cancelled = TRUE AND cli.is_initial_line_item = FALSE
				THEN 'Cancelled Additional Line Item Booking After FPA'
			WHEN cli.is_cancelled = FALSE
				THEN 'Additional Line Item Booking After FPA'
		END                   AS booking_category_detail,
		CASE
			WHEN cli.is_cancelled = TRUE
				THEN 'Cancelled Line Item Booking'
			WHEN cli.is_cancelled = FALSE
				THEN 'Line Item Booking'
		END                                        AS booking_category,
		cli.is_after_fpa_date,
		cli.cart_line_item_id,
		cli.subscription_id,
		cli.price_point_id,
		ilt.cancelled_invoice_amount,
		ilt.additional_invoice_amount,
		0                                                                                 AS initial_gbnf,
		0 AS initial_gb,
		SUM(CASE
			WHEN cli.is_filing_fee = TRUE AND is_cancelled = TRUE
				THEN -cli.amount
			WHEN cli.is_filing_fee = TRUE AND is_cancelled = FALSE
				THEN cli.amount
			ELSE 0
		END)                                                               AS filing_fee,
		0                                                                                 AS initial_discount_amount,
		0                                                                                 AS additional_discount_amount,
		SUM(CASE
			WHEN ilt.cancelled_invoice_amount > ilt.additional_invoice_amount
				THEN 0
			WHEN ilt.additional_invoice_amount >= ilt.cancelled_invoice_amount AND cli.is_cancelled = TRUE
				THEN extended_price_without_ff * -1
			WHEN ilt.additional_invoice_amount >= ilt.cancelled_invoice_amount AND cli.is_cancelled = FALSE
				THEN extended_price_without_ff
		END)                                  AS gbnf,
		SUM(CASE
			WHEN is_cancelled = TRUE
				THEN -cli.extended_price_without_ff -- subtract FROM NBNF for cancelled items
			WHEN is_cancelled = FALSE
				THEN cli.extended_price_without_ff --add to nbnf for additional bookings
			ELSE 0
		END)                                                               AS nbnf,
		SUM(CASE
			WHEN ilt.cancelled_invoice_amount > ilt.additional_invoice_amount
				THEN 0
			WHEN ilt.additional_invoice_amount >= ilt.cancelled_invoice_amount AND cli.is_cancelled = TRUE
				THEN cli.amount * -1
			WHEN ilt.additional_invoice_amount >= ilt.cancelled_invoice_amount AND cli.is_cancelled = FALSE
				THEN cli.amount
		END)                                         AS gb, --same as gbnf but use amount to include filing fees
		SUM(CASE
			WHEN is_cancelled = TRUE
				THEN -cli.amount -- subtract FROM NBNF for cancelled items
			WHEN is_cancelled = FALSE
				THEN cli.amount --add to nbnf for additional bookings
			ELSE 0
		END)                                                               AS nb --same as nbnf but use amount to include filing fees
	FROM classify_line_items cli
	LEFT JOIN invoice_level_totals ilt
		ON ilt.invoice_id = cli.invoice_id
		AND cli.record_updated_date_pt = ilt.record_updated_date_pt
	WHERE cli.is_after_fpa_date = TRUE
		AND cli.is_counted_in_initial_gbnf = FALSE
	GROUP BY ALL
),

initial_line_item_discounts
AS (
	SELECT
		cd.invoice_id,
		cd.line_item_id,
		NULL                                                                                     AS user_id,
		NULL                                                                                  AS workspace_id,
		fpa.invoice_first_payment_approved_at_pt,
		COALESCE(fpa.invoice_first_payment_approved_at_pt,cd.discount_hist_updated_at_pt)    AS booking_at_pt,
		CASE
			WHEN cd.is_cancelled = TRUE THEN 'Cancelled Initial Line Item Discount Before FPA'
			ELSE 'Initial Line Item Discount Before FPA'
		END                                  AS booking_category_detail,
		CASE
			WHEN cd.is_cancelled = TRUE THEN 'Cancelled Line Item Discount'
			ELSE 'Line Item Discount'
		END                                                     AS booking_category,
		cd.is_after_fpa_date,
		cd.cart_line_item_id,
		NULL                                                                                       AS subscription_id,
		NULL                                                                                       AS price_point_id,
		SUM(CASE
			WHEN cd.is_cancelled = TRUE THEN cd.discount_amount
			ELSE -cd.discount_amount
		END)                                                                                       AS initial_gbnf,
		0                                                                                          AS filing_fee,
		initial_gbnf                                                                               AS initial_discount_amount,
		0                                                                                          AS additional_discount_amount,
		--line item discounts prior to FPA date are treated the same for initial_gb, gbnf, nbnf, gb and nb
		initial_gbnf AS initial_gb,
		initial_gbnf AS gbnf,
		initial_gbnf AS nbnf,
		initial_gbnf AS gb,
		initial_gbnf AS nb
	FROM classify_discounts cd
	LEFT JOIN first_payment_approved fpa
		ON fpa.invoice_id = cd.invoice_id
	WHERE
		cd.is_initial_discount = TRUE
		AND cd.is_after_fpa_date = FALSE
		AND cd.line_item_id IS NOT NULL
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),

additional_line_item_discounts
AS (
	SELECT
		cd.invoice_id,
		cd.line_item_id,
		NULL                                                                                     AS user_id,
		NULL                                                                                AS workspace_id,
		fpa.invoice_first_payment_approved_at_pt,
		cd.discount_hist_updated_at_pt                                                     AS booking_at_pt,
		CASE
			WHEN cd.is_initial_discount = TRUE AND cd.is_cancelled = TRUE THEN 'Cancelled Initial Line Item Discount After FPA'
			WHEN cd.is_initial_discount = FALSE AND cd.is_cancelled = TRUE THEN 'Cancelled Additional Line Item Discount After FPA'
			WHEN cd.is_initial_discount = TRUE AND cd.is_cancelled = FALSE THEN 'Initial Line Item Discount After FPA'
			WHEN cd.is_initial_discount = FALSE AND cd.is_cancelled = FALSE THEN 'Additional Line Item Discount After FPA'
			ELSE NULL
		END                                                                      AS booking_category_detail,
		CASE
			WHEN cd.is_cancelled = TRUE THEN 'Cancelled Line Item Discount'
			WHEN cd.is_cancelled = FALSE THEN 'Line Item Discount'
		END                        AS booking_category,
		cd.is_after_fpa_date,
		cd.cart_line_item_id,
		NULL                                                                                       AS subscription_id,
		NULL                                                                                       AS price_point_id,
		0                                                                                          AS initial_gbnf,
		0                                                                                          AS initial_gb,
		0                                                                                          AS filing_fee,
		0                                                                                          AS initial_discount_amount,
		--for discounts created after FPA date, if it is cancelled show positive amount, ELSE show negative amount
		SUM(CASE
			WHEN booking_category = 'Cancelled Line Item Discount' THEN cd.discount_amount
			WHEN booking_category = 'Line Item Discount' THEN -cd.discount_amount
			ELSE 0
		END)                                                                        AS additional_discount_amount,
		SUM(CASE
			WHEN booking_category_detail = 'Cancelled Initial Line Item Discount After FPA' THEN cd.discount_amount
			ELSE 0
		END)                                                                        AS gbnf,
		--if an initial discount AND cancelled after fpa date, THEN add back in to GBNF ELSE 0
		--discount after fpa date does not decrease GBNF
		additional_discount_amount AS nbnf,
		gbnf AS gb, --additional discounts treated the same for gb
		additional_discount_amount AS nb --additional discounts treated the same for nb
	FROM classify_discounts cd
	LEFT JOIN first_payment_approved fpa
		ON fpa.invoice_id = cd.invoice_id
	WHERE cd.is_after_fpa_date = TRUE
		AND cd.line_item_id IS NOT NULL
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
),

combined_bookings
AS (
	SELECT
		invoice_id,
		line_item_id,
		cart_line_item_id,
		subscription_id,
		user_id,
		workspace_id,
		price_point_id,
		invoice_first_payment_approved_at_pt,
		booking_at_pt,
		booking_category_detail,
		booking_category,
		is_after_fpa_date,
		initial_gbnf,
		initial_gb,
		filing_fee,
		initial_discount_amount,
		additional_discount_amount,
		gbnf,
		nbnf,
		gb,
		nb
	FROM initial_bookings_line_item
	UNION ALL
	SELECT
		invoice_id,
		line_item_id,
		cart_line_item_id,
		subscription_id,
		user_id,
		workspace_id,
		price_point_id,
		invoice_first_payment_approved_at_pt,
		booking_at_pt,
		booking_category_detail,
		booking_category,
		is_after_fpa_date,
		initial_gbnf,
		initial_gb,
		filing_fee,
		initial_discount_amount,
		additional_discount_amount,
		gbnf,
		nbnf,
		gb,
		nb
	FROM additional_bookings_line_item
	UNION ALL
	SELECT
		invoice_id,
		line_item_id,
		cart_line_item_id,
		subscription_id,
		user_id,
		workspace_id,
		price_point_id,
		invoice_first_payment_approved_at_pt,
		booking_at_pt,
		booking_category_detail,
		booking_category,
		is_after_fpa_date,
		initial_gbnf,
		initial_gb,
		filing_fee,
		initial_discount_amount,
		additional_discount_amount,
		gbnf,
		nbnf,
		gb,
		nb
	FROM initial_line_item_discounts
	UNION ALL
	SELECT
		invoice_id,
		line_item_id,
		cart_line_item_id,
		subscription_id,
		user_id,
		workspace_id,
		price_point_id,
		invoice_first_payment_approved_at_pt,
		booking_at_pt,
		booking_category_detail,
		booking_category,
		is_after_fpa_date,
		initial_gbnf,
		initial_gb,
		filing_fee,
		initial_discount_amount,
		additional_discount_amount,
		gbnf,
		nbnf,
		gb,
		nb
	FROM additional_line_item_discounts
),

--this step populates the price_point_id, user_id, workspace_id, subscription_id for discount records, coalescing the leading and lagging value.
-- Renewals don't have orders, so the subscription is used to join to integration.udr.subscriptions to populate the workspace_id in fct_line_item_hist_prep_lz
-- in rare cases, there are more than 1 price_point_ids for the same line_item. Mostly affecting legacy records, but this is why we can't simply join in based on line item
-- Sometimes a discount is the first record for a specific line_item, which is why we need the lead() function.
combined_bookings_add_line_item_dimensions
AS(
	SELECT
		invoice_id,
		line_item_id,
		cart_line_item_id,
		COALESCE(
			subscription_id,
			LAG(subscription_id) IGNORE NULLS OVER( PARTITION BY line_item_id ORDER BY booking_at_pt ASC),
			LEAD(subscription_id) IGNORE NULLS OVER( PARTITION BY line_item_id ORDER BY booking_at_pt ASC)
		) AS subscription_id,
		COALESCE(
			user_id,
			LAG(user_id) IGNORE NULLS OVER( PARTITION BY line_item_id ORDER BY booking_at_pt ASC),
			LEAD(user_id) IGNORE NULLS OVER( PARTITION BY line_item_id ORDER BY booking_at_pt ASC)
		) AS user_id,
		COALESCE(
			workspace_id,
			LAG(workspace_id) IGNORE NULLS OVER( PARTITION BY line_item_id ORDER BY booking_at_pt ASC),
			LEAD(workspace_id) IGNORE NULLS OVER( PARTITION BY line_item_id ORDER BY booking_at_pt ASC)
		) AS workspace_id,
		COALESCE(
			price_point_id,
			LAG(price_point_id) IGNORE NULLS OVER( PARTITION BY line_item_id ORDER BY booking_at_pt ASC),
			LEAD(price_point_id) IGNORE NULLS OVER( PARTITION BY line_item_id ORDER BY booking_at_pt ASC)
		) AS price_point_id,
		invoice_first_payment_approved_at_pt,
		booking_at_pt,
		booking_category_detail,
		booking_category,
		is_after_fpa_date,
		initial_gbnf,
		initial_gb,
		filing_fee,
		initial_discount_amount,
		additional_discount_amount,
		gbnf,
		nbnf,
		gb,
		nb
	FROM combined_bookings
),

--calculate the initial invoice level discounts
initial_discount_invoice_level
AS (
	SELECT
		cd.invoice_id,
		fpa.invoice_first_payment_approved_at_pt,
		cd.discount_hist_updated_at_pt,
		cd.discount_apply_type_id,
		SUM(CASE
			WHEN cd.is_cancelled = TRUE
				THEN cd.discount_amount --When discount is cancelled before fpa date add discount amount back in to gbnf
			ELSE -cd.discount_amount
		END) AS gbnf_discount_amount, --otherwise subtract discount amount FROM gbnf
		SUM(CASE
			WHEN cd.is_cancelled = TRUE THEN cd.discount_amount
			ELSE -cd.discount_amount
		END) AS nbnf_discount_amount
	FROM classify_discounts cd
	LEFT JOIN first_payment_approved fpa
		ON fpa.invoice_id = cd.invoice_id
	WHERE
		cd.is_initial_discount = TRUE
		AND cd.is_after_fpa_date = FALSE
		AND cd.line_item_id IS NULL
	GROUP BY 1,2,3,4
),

--calculate the additional invoice level discounts
additional_discount_invoice_level
AS (
	SELECT
		cd.invoice_id,
		fpa.invoice_first_payment_approved_at_pt,
		cd.discount_hist_updated_at_pt,
		cd.discount_apply_type_id,
		SUM(CASE
			WHEN cd.is_cancelled = TRUE AND cd.is_initial_discount = TRUE
				THEN cd.discount_amount
			ELSE 0
		END)                                                   AS gbnf_discount_amount,
		-- if the discount was an initial discount and cancelled then add back into GBNF
		SUM(CASE
			WHEN cd.is_cancelled = TRUE
				THEN cd.discount_amount
			ELSE -cd.discount_amount
		END)                                 AS nbnf_discount_amount,
		DIV0(
			SUM(cd.discount_amount),
			SUM(SUM(cd.discount_amount)) OVER (PARTITION BY cd.invoice_id)
		) AS perc_additional_invoice_discount
	--calculate the additional invoice level percentage for each invoice_id and discount updated date
	--this percentage is used later to re-allocate invoice level discounts to the correct discount date
	--this step is not necessary for the initial invoice level discounts since those are just assigned to the fpa date
	FROM classify_discounts cd
	LEFT JOIN first_payment_approved fpa
		ON fpa.invoice_id = cd.invoice_id
	WHERE cd.is_after_fpa_date = TRUE
		AND cd.line_item_id IS NULL
	GROUP BY 1,2,3,4
),

--calculate line item percentages for allocating invoice level discounts
--left in additional unused columns for troubleshooting if necessary
calculate_item_percentages
AS (
	SELECT
		cb.invoice_id,
		cb.line_item_id,
		cb.cart_line_item_id,
		cb.subscription_id,
		cb.user_id,
		cb.workspace_id,
		cb.price_point_id,
		pp.product_type,
		pp.product_name,
		idil.discount_apply_type_id as initial_discount_apply_type_id,
		adil.discount_apply_type_id as additional_discount_apply_type_id,
		cb.line_item_initial_gbnf,
		cb.line_item_initial_nbnf,
		cb.line_item_initial_gb,
		idil.gbnf_discount_amount AS initial_gbnf_discount_invoice_level_agg,
		idil.nbnf_discount_amount AS initial_nbnf_discount_invoice_level_agg,
		adil.gbnf_discount_amount AS additional_gbnf_discount_invoice_level_agg,
		adil.nbnf_discount_amount AS additional_nbnf_discount_invoice_level_agg,
		--calculate the line item spread percentages based on discount_apply_type_id
		CASE 
			WHEN idil.discount_apply_type_id = 1 AND pp.product_type = 'Product' THEN
				DIV0(CASE WHEN cb.line_item_initial_gbnf > 0 THEN cb.line_item_initial_gbnf ELSE 0 END, 
					 SUM(CASE WHEN cb.line_item_initial_gbnf > 0 AND pp.product_type = 'Product' THEN cb.line_item_initial_gbnf ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			-- For type 2, use gb instead of gbnf to include fees in the allocation
			WHEN idil.discount_apply_type_id = 2 THEN
				DIV0(CASE WHEN cb.line_item_initial_gb > 0 THEN cb.line_item_initial_gb ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_initial_gb > 0 THEN cb.line_item_initial_gb ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			WHEN idil.discount_apply_type_id = 3 AND pp.product_type <> 'Filing Fee' THEN
				DIV0(CASE WHEN cb.line_item_initial_gbnf > 0 THEN cb.line_item_initial_gbnf ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_initial_gbnf > 0 AND pp.product_type <> 'Filing Fee' THEN cb.line_item_initial_gbnf ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			WHEN idil.discount_apply_type_id = 4 AND (pp.product_type = 'Product' OR pp.product_name LIKE '%Standard Shipping%') THEN
				DIV0(CASE WHEN cb.line_item_initial_gbnf > 0 THEN cb.line_item_initial_gbnf ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_initial_gbnf > 0 AND (pp.product_type = 'Product' OR pp.product_name LIKE '%Standard Shipping%') THEN cb.line_item_initial_gbnf ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			ELSE 0
		END AS gbnf_initial_perc,
		CASE 
			WHEN idil.discount_apply_type_id = 1 AND pp.product_type = 'Product' THEN
				DIV0(CASE WHEN cb.line_item_initial_nbnf > 0 THEN cb.line_item_initial_nbnf ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_initial_nbnf > 0 AND pp.product_type = 'Product' THEN cb.line_item_initial_nbnf ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			-- For type 2, use nb instead of nbnf to include fees in the allocation
			WHEN idil.discount_apply_type_id = 2 THEN
				DIV0(CASE WHEN cb.line_item_initial_nb > 0 THEN cb.line_item_initial_nb ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_initial_nb > 0 THEN cb.line_item_initial_nb ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			WHEN idil.discount_apply_type_id = 3 AND pp.product_type <> 'Filing Fee' THEN
				DIV0(CASE WHEN cb.line_item_initial_nbnf > 0 THEN cb.line_item_initial_nbnf ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_initial_nbnf > 0 AND pp.product_type <> 'Filing Fee' THEN cb.line_item_initial_nbnf ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			WHEN idil.discount_apply_type_id = 4 AND (pp.product_type = 'Product' OR pp.product_name LIKE '%Standard Shipping%') THEN
				DIV0(CASE WHEN cb.line_item_initial_nbnf > 0 THEN cb.line_item_initial_nbnf ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_initial_nbnf > 0 AND (pp.product_type = 'Product' OR pp.product_name LIKE '%Standard Shipping%') THEN cb.line_item_initial_nbnf ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			ELSE 0
		END AS nbnf_initial_perc,
		CASE 
			WHEN adil.discount_apply_type_id = 1 AND pp.product_type = 'Product' THEN
				DIV0(CASE WHEN cb.line_item_additional_gbnf > 0 THEN cb.line_item_additional_gbnf ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_additional_gbnf > 0 AND pp.product_type = 'Product' THEN cb.line_item_additional_gbnf ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			-- For type 2, use gb instead of gbnf to include fees in the allocation
			WHEN adil.discount_apply_type_id = 2 THEN
				DIV0(CASE WHEN cb.line_item_additional_gb > 0 THEN cb.line_item_additional_gb ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_additional_gb > 0 THEN cb.line_item_additional_gb ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			WHEN adil.discount_apply_type_id = 3 AND pp.product_type <> 'Filing Fee' THEN
				DIV0(CASE WHEN cb.line_item_additional_gbnf > 0 THEN cb.line_item_additional_gbnf ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_additional_gbnf > 0 AND pp.product_type <> 'Filing Fee' THEN cb.line_item_additional_gbnf ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			WHEN adil.discount_apply_type_id = 4 AND (pp.product_type = 'Product' OR pp.product_name LIKE '%Standard Shipping%') THEN
				DIV0(CASE WHEN cb.line_item_additional_gbnf > 0 THEN cb.line_item_additional_gbnf ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_additional_gbnf > 0 AND (pp.product_type = 'Product' OR pp.product_name LIKE '%Standard Shipping%') THEN cb.line_item_additional_gbnf ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			ELSE 0
		END AS gbnf_additional_perc,
		CASE 
			WHEN adil.discount_apply_type_id = 1 AND pp.product_type = 'Product' THEN
				DIV0(CASE WHEN cb.line_item_additional_nbnf > 0 THEN cb.line_item_additional_nbnf ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_additional_nbnf > 0 AND pp.product_type = 'Product' THEN cb.line_item_additional_nbnf ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			-- For type 2, use nb instead of nbnf to include fees in the allocation
			WHEN adil.discount_apply_type_id = 2 THEN
				DIV0(CASE WHEN cb.line_item_additional_nb > 0 THEN cb.line_item_additional_nb ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_additional_nb > 0 THEN cb.line_item_additional_nb ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			WHEN adil.discount_apply_type_id = 3 AND pp.product_type <> 'Filing Fee' THEN
				DIV0(CASE WHEN cb.line_item_additional_nbnf > 0 THEN cb.line_item_additional_nbnf ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_additional_nbnf > 0 AND pp.product_type <> 'Filing Fee' THEN cb.line_item_additional_nbnf ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			WHEN adil.discount_apply_type_id = 4 AND (pp.product_type = 'Product' OR pp.product_name LIKE '%Standard Shipping%') THEN
				DIV0(CASE WHEN cb.line_item_additional_nbnf > 0 THEN cb.line_item_additional_nbnf ELSE 0 END,
					 SUM(CASE WHEN cb.line_item_additional_nbnf > 0 AND (pp.product_type = 'Product' OR pp.product_name LIKE '%Standard Shipping%') THEN cb.line_item_additional_nbnf ELSE 0 END) OVER (PARTITION BY cb.invoice_id))
			ELSE 0
		END AS nbnf_additional_perc,
		--spread the initial and additional invoice level discounts against the line_item percentages
		gbnf_initial_perc * idil.gbnf_discount_amount AS gbnf_initial_discount_invoice_level,
		nbnf_initial_perc * idil.nbnf_discount_amount AS nbnf_initial_discount_invoice_level,
		gbnf_additional_perc * adil.gbnf_discount_amount AS gbnf_additional_discount_invoice_level,
		nbnf_additional_perc * adil.nbnf_discount_amount AS nbnf_additional_discount_invoice_level
	FROM (SELECT
		invoice_id,
		line_item_id,
		cart_line_item_id,
		subscription_id,
		user_id,
		workspace_id,
		price_point_id,
		SUM(CASE WHEN is_after_fpa_date = FALSE THEN gbnf ELSE 0 END)                   AS line_item_initial_gbnf,
		SUM(CASE WHEN is_after_fpa_date = FALSE THEN nbnf ELSE 0 END)                   AS line_item_initial_nbnf,
		SUM(CASE WHEN is_after_fpa_date = FALSE THEN gb ELSE 0 END)                     AS line_item_initial_gb,
		SUM(CASE WHEN is_after_fpa_date = FALSE THEN nb ELSE 0 END)                     AS line_item_initial_nb,
		--use both gbnf and nbnf values before and after fpa date to calculate item spread percentages for additional gbnf/nbnf
		--this is important for cancelled discounts after fpa date, we need to know how to spread invoice level discounts where positive GBNF and NBNF only exist before fpa date
		SUM(gbnf)                                                                       AS line_item_additional_gbnf,
		SUM(nbnf)                                                                       AS line_item_additional_nbnf,
		SUM(gb)                                                                         AS line_item_additional_gb,
		SUM(nb)                                                                         AS line_item_additional_nb
	FROM combined_bookings_add_line_item_dimensions
	GROUP BY ALL) cb
	LEFT JOIN (SELECT
		invoice_id,
		SUM(gbnf_discount_amount) AS gbnf_discount_amount,
		SUM(nbnf_discount_amount) AS nbnf_discount_amount,
		MAX(discount_apply_type_id) AS discount_apply_type_id
	FROM initial_discount_invoice_level
	GROUP BY  ALL) idil
		ON idil.invoice_id = cb.invoice_id
	LEFT JOIN (SELECT
		invoice_id,
		SUM(gbnf_discount_amount) AS gbnf_discount_amount,
		SUM(nbnf_discount_amount) AS nbnf_discount_amount,
		MAX(discount_apply_type_id) AS discount_apply_type_id
	FROM additional_discount_invoice_level
	GROUP BY ALL) adil
		ON adil.invoice_id = cb.invoice_id
	LEFT JOIN {{ ref('dim_product_price_point_udm') }} pp
		ON cb.price_point_id = pp.price_point_id
),

initial_discount_invoice_level_allocated
AS (
	SELECT
		cip.invoice_id,
		cip.line_item_id,
		cip.cart_line_item_id,
		cip.subscription_id,
		cip.user_id,
		cip.workspace_id,
		cip.price_point_id,
		idil.invoice_first_payment_approved_at_pt,
		COALESCE(idil.invoice_first_payment_approved_at_pt,idil.discount_hist_updated_at_pt)  AS booking_at_pt,
		CASE
			WHEN cip.gbnf_initial_discount_invoice_level <= 0 OR cip.nbnf_initial_discount_invoice_level <= 0
				THEN 'Initial Invoice Level Discount Before FPA'
			WHEN cip.gbnf_initial_discount_invoice_level > 0 OR cip.nbnf_initial_discount_invoice_level > 0
				THEN 'Cancelled Initial Invoice Level Discount Before FPA'
			ELSE NULL
		END AS booking_category_detail,
		CASE
			WHEN cip.gbnf_initial_discount_invoice_level <= 0 OR cip.nbnf_initial_discount_invoice_level <= 0
				THEN 'Invoice Level Discount'
			WHEN cip.gbnf_initial_discount_invoice_level > 0 OR cip.nbnf_initial_discount_invoice_level > 0
				THEN 'Cancelled Invoice Level Discount'
			ELSE NULL
		END AS booking_category,
		cip.gbnf_initial_discount_invoice_level    AS initial_gbnf,
		0                                        AS filing_fee,
		cip.gbnf_initial_discount_invoice_level    AS initial_discount_amount,
		0                                        AS additional_discount_amount,
		cip.gbnf_initial_discount_invoice_level    AS gbnf,
		cip.nbnf_initial_discount_invoice_level    AS nbnf
	FROM (SELECT
		invoice_id,
		line_item_id,
		cart_line_item_id,
		subscription_id,
		user_id,
		workspace_id,
		price_point_id,
		SUM(gbnf_initial_discount_invoice_level) AS gbnf_initial_discount_invoice_level,
		SUM(nbnf_initial_discount_invoice_level) AS nbnf_initial_discount_invoice_level
	FROM calculate_item_percentages
	GROUP BY ALL) cip
	LEFT JOIN
		(
			SELECT
				invoice_id,
				invoice_first_payment_approved_at_pt,
				discount_hist_updated_at_pt
			FROM initial_discount_invoice_level
		) idil
		ON cip.invoice_id = idil.invoice_id
	HAVING gbnf <> 0
		OR nbnf <> 0
),

--assign the invoice level discounts that have been spread across the line items back to the discount updated dates using the perc_additional_order_discount
additional_discount_invoice_level_allocated
AS (
	SELECT
		cip.invoice_id,
		cip.line_item_id,
		cip.cart_line_item_id,
		cip.subscription_id,
		cip.user_id,
		cip.workspace_id,
		cip.price_point_id,
		adil.invoice_first_payment_approved_at_pt,
		adil.discount_hist_updated_at_pt                                      AS booking_at_pt,
		CASE
			WHEN (cip.gbnf_additional_discount_invoice_level = 0 AND cip.nbnf_additional_discount_invoice_level > 0)
				THEN 'Cancelled Additional Invoice Level Discount After FPA'
			WHEN cip.gbnf_additional_discount_invoice_level > 0
				THEN 'Cancelled Initial Invoice Level Discount After FPA'
			WHEN cip.nbnf_additional_discount_invoice_level < 0
				THEN 'Additional Invoice Level Discount After FPA'
			ELSE NULL
		END                                                             AS booking_category_detail,
		CASE
			WHEN (cip.gbnf_additional_discount_invoice_level > 0 OR cip.nbnf_additional_discount_invoice_level > 0)
				THEN 'Cancelled Invoice Level Discount'
			WHEN cip.nbnf_additional_discount_invoice_level < 0
				THEN 'Invoice Level Discount'
			ELSE NULL
		END                                                             AS booking_category,
		0                                                                              AS initial_gbnf,
		0                                                                              AS filing_fee,
		0                                                                              AS initial_discount_amount,
		adil.perc_additional_invoice_discount * cip.nbnf_additional_discount_invoice_level AS additional_discount_amount,
		adil.perc_additional_invoice_discount * cip.gbnf_additional_discount_invoice_level AS gbnf,
		adil.perc_additional_invoice_discount * cip.nbnf_additional_discount_invoice_level AS nbnf
	FROM (SELECT
		invoice_id,
		line_item_id,
		cart_line_item_id,
		subscription_id,
		user_id,
		workspace_id,
		price_point_id,
		SUM(gbnf_additional_discount_invoice_level) AS gbnf_additional_discount_invoice_level,
		SUM(nbnf_additional_discount_invoice_level) AS nbnf_additional_discount_invoice_level
	FROM calculate_item_percentages cip
	GROUP BY ALL) cip
	LEFT JOIN
		(SELECT
			invoice_id,
			invoice_first_payment_approved_at_pt,
			discount_hist_updated_at_pt,
			perc_additional_invoice_discount
		FROM additional_discount_invoice_level) adil
		ON adil.invoice_id = cip.invoice_id
	HAVING gbnf <> 0
		OR nbnf <> 0
),

cp2_store_credit
AS(
	SELECT
		sc.invoice_id,
		sc.invoice_line_id AS line_item_id,
		lip.cart_line_item_id,
		lip.subscription_id,
		lip.user_id,
		lip.workspace_id,
		lip.price_point_id,
		fpa.invoice_first_payment_approved_at_pt,
		CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', event_occurred_at_utc) AS event_occurred_at_pt,
		CASE WHEN event_occurred_at_pt <= fpa.invoice_first_payment_approved_at_pt THEN fpa.invoice_first_payment_approved_at_pt ELSE event_occurred_at_pt END AS booking_at_pt,
		CASE WHEN event_occurred_at_pt <= fpa.invoice_first_payment_approved_at_pt THEN 'Line Item Credit Before FPA' ELSE 'Line Item Credit After FPA' END AS booking_category_detail,
		'Line Item Credit' AS booking_category,
		SUM(CASE
			WHEN booking_at_pt <= invoice_first_payment_approved_at_pt AND p.is_filing_fee = FALSE
				THEN sc.amount * -1
			ELSE 0
		END)                                                           AS initial_gbnf,
		SUM(CASE
			WHEN booking_at_pt <= invoice_first_payment_approved_at_pt AND p.is_filing_fee = TRUE
				THEN sc.amount * -1
			ELSE 0
		END)                                                           AS initial_gb,
		0 AS filing_fee,
		0 AS initial_discount_amount,
		0 AS additional_discount_amount,
		initial_gbnf AS gbnf,
		SUM(CASE WHEN p.is_filing_fee = FALSE THEN sc.amount * -1 ELSE 0 END) AS nbnf,
		initial_gb AS gb,
		SUM(sc.amount * -1) AS nb
	FROM
		{{ source('udm', 'store_credits')}} sc
	LEFT JOIN {{ ref('fct_line_item_prep_lz_udm')}} lip
		ON sc.invoice_line_id = lip.line_item_id
	LEFT JOIN first_payment_approved fpa
		ON fpa.invoice_id = sc.invoice_id
	LEFT JOIN {{ ref('dim_product_price_point_udm')}} p
		ON p.price_point_id = lip.price_point_id
	WHERE sc.event_type = 'credit issued'
		AND sc.source_id = '4f9e2b5e-c45e-434b-b2b2-126b5a251a9c' --only relevant for CP2 at the moment, opportunity to combined CP1 and CP2 in the future, which would require re-working how discounts are handled for CP1
	GROUP BY ALL
),

final_union
AS (
	SELECT
		invoice_id,
		line_item_id,
		cart_line_item_id,
		subscription_id,
		user_id,
		workspace_id,
		price_point_id,
		invoice_first_payment_approved_at_pt,
		booking_at_pt,
		booking_category_detail,
		booking_category,
		initial_gbnf,
		initial_gb,
		filing_fee,
		initial_discount_amount,
		additional_discount_amount,
		gbnf,
		nbnf,
		gb,
		nb
	FROM combined_bookings_add_line_item_dimensions
	UNION ALL
	SELECT
		invoice_id,
		line_item_id,
		cart_line_item_id,
		subscription_id,
		user_id,
		workspace_id,
		price_point_id,
		invoice_first_payment_approved_at_pt,
		booking_at_pt,
		booking_category_detail,
		booking_category,
		initial_gbnf,
		initial_gbnf AS initial_gb, --initial_gb same as initial_gbnf for allocated discounts (don't allocate discounts to filing fees)
		filing_fee,
		initial_discount_amount,
		additional_discount_amount,
		gbnf,
		nbnf,
		gbnf AS gb, --gbnf same as gb for allocated discounts (don't allocate discounts to filing fees)
		nbnf AS nb  --nbnf same as nb for allocated discounts (don't allocate discounts to filing fees)
	FROM initial_discount_invoice_level_allocated
	UNION ALL
	SELECT
		invoice_id,
		line_item_id,
		cart_line_item_id,
		subscription_id,
		user_id,
		workspace_id,
		price_point_id,
		invoice_first_payment_approved_at_pt,
		booking_at_pt,
		booking_category_detail,
		booking_category,
		initial_gbnf,
		initial_gbnf AS initial_gb, --initial_gb same as initial_gbnf for allocated discounts (don't allocate discounts to filing fees)
		filing_fee,
		initial_discount_amount,
		additional_discount_amount,
		gbnf,
		nbnf,
		gbnf AS gb, --gbnf same as gb for allocated discounts (don't allocate discounts to filing fees)
		nbnf AS nb --nbnf same as nb for allocated discounts (don't allocate discounts to filing fees)
	FROM additional_discount_invoice_level_allocated
	UNION ALL
	SELECT
		invoice_id,
		line_item_id,
		cart_line_item_id,
		subscription_id,
		user_id,
		workspace_id,
		price_point_id,
		invoice_first_payment_approved_at_pt,
		booking_at_pt,
		booking_category_detail,
		booking_category,
		initial_gbnf,
		initial_gb,
		filing_fee,
		initial_discount_amount,
		additional_discount_amount,
		gbnf,
		nbnf,
		gb,
		nb
	FROM cp2_store_credit
),

final_grouped
AS(
	SELECT
		invoice_id,
		line_item_id,
		cart_line_item_id,
		subscription_id,
		user_id,
		workspace_id,
		price_point_id,
		invoice_first_payment_approved_at_pt,
		booking_at_pt,
		booking_category_detail,
		booking_category,
		CASE WHEN booking_category = 'Cancelled Line Item Booking'
				THEN TRUE
			ELSE FALSE
		END AS is_cancelled_booking, --needed for GOIV/GOV calculations
		CASE WHEN booking_category ILIKE 'cancelled%' THEN TRUE ELSE FALSE END AS is_cancelled, --needed for GOIV/GOV calculations
		CASE WHEN booking_at_pt >= invoice_first_payment_approved_at_pt THEN TRUE ELSE FALSE END AS is_booking_after_fpa_date, --needed for GOIV/GOV calculations
		SUM(COALESCE(initial_gbnf,0))::NUMBER(15,2) AS initial_gbnf,
		SUM(COALESCE(initial_gb,0))::NUMBER(15,2) AS initial_gb,
		SUM(COALESCE(filing_fee,0))::NUMBER(15,2) AS filing_fee,
		SUM(COALESCE(initial_discount_amount,0))::NUMBER(15,2) AS initial_discount_amount,
		SUM(COALESCE(additional_discount_amount,0))::NUMBER(15,2) AS additional_discount_amount,
		SUM(COALESCE(gbnf,0))::NUMBER(15,2) AS gbnf,
		SUM(COALESCE(nbnf,0))::NUMBER(15,2) AS nbnf,
		SUM(COALESCE(gb,0))::NUMBER(15,2) AS gb,
		SUM(COALESCE(nb,0))::NUMBER(15,2) AS nb
	FROM final_union
	GROUP BY ALL
),

add_goiv_gov_eligibility
AS(
	SELECT
		fg.*,
		LAST_VALUE(fg.is_cancelled_booking) OVER (PARTITION BY fg.line_item_id, fg.is_booking_after_fpa_date ORDER BY fg.booking_at_pt ASC) AS goiv_cancellation_indicator,
		--         calculate the is_cancelled status for the last line_item record partitioned by is_after_fpa_date
		CASE
			WHEN fg.is_cancelled = TRUE --don't want a goiv flag on a cancelled line item record
				THEN FALSE
			WHEN (goiv_cancellation_indicator = TRUE AND fg.is_booking_after_fpa_date = FALSE AND fg.invoice_first_payment_approved_at_pt IS NOT NULL) --don't want to flag any line items with a GOIV flag before FPA date if the line item was cancelled before the FPA date
				THEN FALSE
			-------look up basic llc config (i.e. source_price_point_id = 7820) and flag line items as eligible if they meet the criteria
			WHEN pp.source_price_point_id = '7820' AND SUM(fg.gb) OVER (PARTITION BY fg.invoice_id) = 0 AND fg.invoice_first_payment_approved_at_pt IS NULL -- if free basic LLC invoice and meets prior criteria, flag line item as eligible
				THEN TRUE
			WHEN pp.source_price_point_id = '7820' AND SUM(fg.gb) OVER (PARTITION BY fg.invoice_id) > 0 AND fg.invoice_first_payment_approved_at_pt IS NOT NULL -- non-free basic LLC invoice, flag line item as eligible if has FPA date
				THEN TRUE
			WHEN (fg.gb) > 0 AND fg.invoice_first_payment_approved_at_pt IS NOT NULL
				THEN TRUE -- remaining line items with GB > 0 flag as eligible if FPA date
			ELSE FALSE
		END AS is_goiv_eligible_line_item,
		CASE
			WHEN fg.booking_category = 'Line Item Booking' AND fg.line_item_id = fg.cart_line_item_id --want to flag non-cancelled cart line items as gov eligible
				THEN TRUE
			ELSE FALSE
		END AS is_gov_eligible_line_item
	FROM final_grouped fg
	LEFT JOIN {{ ref('dim_product_price_point_udm') }} pp
		ON fg.price_point_id = pp.price_point_id
),

add_goiv
AS (
	SELECT
		*,
		CASE
			WHEN
				--first check that the line item is eligible (i.e. not_cancelled, not-cancelled prior to fpa, basic_llc, or booking amount > 0
				-- if eligible, return the first booking instance for that line item
				is_goiv_eligible_line_item = TRUE
				AND (ROW_NUMBER() OVER (PARTITION BY line_item_id, is_goiv_eligible_line_item ORDER BY booking_at_pt ASC)) = 1
				THEN 1
			ELSE 0
		END AS gross_order_item_volume
	FROM add_goiv_gov_eligibility
),

add_gov
AS (
	SELECT
		*,
		CASE
			WHEN
				--first check that the line item is GOV eligible (i.e. a cart line_item booking)
				-- secondly, check that there is at least one line item with a goiv of 1
				-- if that criteria is met, return the first line item instance and populate with gov = 1
				is_gov_eligible_line_item = TRUE
				AND SUM(gross_order_item_volume) OVER (PARTITION BY invoice_id) > 0
				AND (ROW_NUMBER() OVER (PARTITION BY line_item_id, is_gov_eligible_line_item ORDER BY booking_at_pt ASC)) = 1
				THEN 1
			ELSE 0
		END AS gross_order_volume
	FROM add_goiv
),

third_party_payment_gateway
AS (
	SELECT
		COALESCE(order_id,invoice_id) AS invoice_id,
		SUM(CASE
			WHEN is_third_party_payment THEN 1
			ELSE 0
		END)                                                    AS ct_third_party_gateway_payments,
		CASE WHEN ct_third_party_gateway_payments > 0 THEN TRUE ELSE FALSE END AS invoice_has_third_party_gateway_payment
	--flag invoice as having third party gateway payment, if one approved payment went through a third party gateway
	FROM {{ ref('fct_payment_transaction_udm') }}
	WHERE transaction_status = 'Approved'
	GROUP BY 1
)

SELECT
	{{ dbt_utils.surrogate_key(['ag.line_item_id', 'ag.booking_at_pt', 'ag.booking_category_detail','ag.booking_category', 'ag.price_point_id'])}} AS booking_id,
	ag.invoice_id,
	ag.line_item_id,
	ag.cart_line_item_id,
	ag.subscription_id,
	ag.user_id,
	ag.workspace_id,
	ag.price_point_id,
	ag.invoice_first_payment_approved_at_pt,
	ag.booking_at_pt,
	ag.booking_category_detail,
	ag.booking_category,
	ag.gross_order_volume,
	ag.gross_order_item_volume,
	ag.initial_gbnf::NUMBER (15, 2) AS initial_gbnf,
	ag.initial_gb::NUMBER (15, 2) AS initial_gb,
	ag.filing_fee::NUMBER (15, 2) AS filing_fee,
	ag.initial_discount_amount::NUMBER (15, 2) AS initial_discount_amount,
	ag.additional_discount_amount::NUMBER (15, 2) AS additional_discount_amount,
	COALESCE(tppg.invoice_has_third_party_gateway_payment, FALSE) AS invoice_has_third_party_gateway_payment, --want to show false in cases where fpa date is null for basic llc orders
	ag.gbnf::NUMBER (15, 2) AS gbnf,
	ag.nbnf::NUMBER (15, 2) AS nbnf,
	ag.gb::NUMBER (15, 2) AS gb,
	ag.nb::NUMBER (15, 2) AS nb,
	lip.cart_price_point_id,
	lip.root_price_point_id,
    lip.source_fulfillment_id,
	lip.source_cart_line_item_id,
	lip.source_invoice_id,
	lip.source_line_item_id,
	lip.source_subscription_id,
	lip.source_user_id,
    lip.source_root_price_point_id,
    lip.source_cart_price_point_id,
	lip.source_system
FROM add_gov ag
LEFT JOIN third_party_payment_gateway tppg
	ON ag.invoice_id = tppg.invoice_id --join on invoice_id if it exists, otherwise join on invoice_id for renewals
LEFT JOIN {{ ref('fct_line_item_prep_lz_udm')}} lip --join back in source cart, root fields etc
	ON ag.line_item_id = lip.line_item_id
