import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta, date
import logging

logger = logging.getLogger(__name__)

dag_id = "D_Create_crypto_idc_day"
tags = ["D_Create", "crypto"]


def _task_failure_alert(context):
    from airflow_modules import airflow_env_variables

    sys.path.append(airflow_env_variables.DWH_SCRIPT)
    import pytz
    from common.utils import send_line_message

    jst = pytz.timezone("Asia/Tokyo")
    ts_now = datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S")

    message = "{} [Failed]{}\nAirflow Dags: {}".format(ts_now, ",".join(tags), dag_id)
    send_line_message(message)


def _send_warning_notification(optional_message=None):
    from airflow_modules import airflow_env_variables

    sys.path.append(airflow_env_variables.DWH_SCRIPT)
    import pytz
    from common.utils import send_line_message

    jst = pytz.timezone("Asia/Tokyo")
    ts_now = datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S")
    message = "{} [WARNING]{}\nAirflow Dags: {}".format(ts_now, ",".join(tags), dag_id)
    if optional_message:
        message += "\n\n" + optional_message
    send_line_message(message)


args = {"owner": "airflow", "retries": 5, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="Create mart tables for crypto price indicators",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    tags=tags,
    default_args=args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    from airflow_modules import airflow_env_variables

    sys.path.append(airflow_env_variables.DWH_SCRIPT)
    from common import env_variables

    load_raw_table = SparkSubmitOperator(
        task_id="process_candle_data_for_ingestion",
        application="{}/pyspark/D_Create_crypto_idc_day_001.py".format(
            airflow_env_variables.QUERY_SCRIPT
        ),
        conn_id="spark_conn",
        application_args=[
            env_variables.SPARK_MASTER_HOST,
            env_variables.SPARK_MASTER_PORT,
            env_variables.HIVE_METASTORE_HOST,
            env_variables.HIVE_METASTORE_PORT,
        ],
    )

    spark_sql_job = SparkSqlOperator(
        sql="select count(*) from crypto_raw.candles_day",
        master="spark://{}:{}".format(
            env_variables.SPARK_MASTER_HOST, env_variables.SPARK_MASTER_PORT
        ),
        conf="spark.hadoop.hive.metastore.uris=thrift://{}:{}".format(
            env_variables.HIVE_METASTORE_HOST, env_variables.HIVE_METASTORE_PORT
        ),
        task_id="spark_sql_job",
    )

    dag_end = DummyOperator(task_id="dag_end")

    (dag_start >> load_raw_table >> spark_sql_job >> dag_end)
