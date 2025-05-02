from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.sensors.sql import SqlSensor
from airflow.utils.state import State
from airflow.providers.slack.notifications.slack import send_slack_notification

from plugins.config.config import KubernetesConfig, SnowflakeConfig, AWSConfig
from plugins.notifications.lz_notifiers import send_error_notification
from plugins.operators.lz_dbt_operators import (
    DbtCommandOperator,
)

from plugins.flows.dbt import dbt_elementary_alerts_and_docs_sites

# Generate Configs.
# dbt for the dbt orchestration
dbt_config = KubernetesConfig("dbt")
snowflake_config = SnowflakeConfig("transform")
snowflake_default_args = snowflake_config.default_args
aws_config = AWSConfig()
aws_conn_id = aws_config.attribute("aws_conn_id", "default")

og_default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

default_args = KubernetesConfig.merge_default_args(
    og_default_args, dbt_config.default_args
)

with DAG(
    "dbt_daily_run",
    schedule_interval="15 8 * * *",  # 8:15am UTC (12:15am PST / 1:15am PDT)
    catchup=False,
    tags=["dbt","P1"],
    default_args=default_args,
) as dag:
    dag.doc_md = dedent(
        """
    # DBT Orchestration Job:
    The dbt production run which runs daily at 8:15am UTC (12:15am PST / 1:15am PDT). This job runs and test all models in the project using the 
    `dbt build` command. 
    """
    )

    lz_data_ready_check = SqlSensor(
        task_id="lz_data_ready_check",
        poke_interval=60 * 5,  # Query every 5 minutes
        timeout=60
        * 60
        * 3,  # Quit the job after 3 hours, 3:15AM/PST 4:15AM PDT, retry will try again for another 3 hours
        soft_fail=False,  # Sets task to failed instead of skipped on failure
        mode="reschedule",
        success=lambda x: x == 1,
        sql="""SELECT 
                    (max(dtlastupdated)::DATE>=convert_timezone('UTC', 'America/Los_Angeles', '{{ data_interval_end }}')::DATE)::INT as HAS_TODAY 
                FROM RAW.LZDATA_DBO.ORDERITEM;""",  # Table should have records past midnight pacific if ready for daily run
        conn_id=snowflake_default_args.get("snowflake_conn_id"),
    )

    dbt_build = DbtCommandOperator(
        task_id="dbt_daily_build",
        command="build",
        command_args="--exclude tag:meta tag:q4h tag:hourly tag:sqlsensor tag:ga4_raw tag:development tag:lz_domain tag:metric_store source:pipkins+ source:call_center+ source:intercom+ source:intercom_activity_logs+",
        on_failure_callback=send_error_notification(incident_level="P1"),
        on_retry_callback=send_slack_notification
            (slack_conn_id="dunkel_barks",
            text="Daily DBT build up for retry",
            channel="#dunkel-alerts",),
        on_success_callback=send_slack_notification
            (slack_conn_id="dunkel_barks",
            text ="Daily DBT build successful",
            channel ="#daily-dag-success",),
    )

    alerts_and_doc_site = dbt_elementary_alerts_and_docs_sites(dag=dag)

    lz_data_ready_check >> dbt_build >> alerts_and_doc_site
