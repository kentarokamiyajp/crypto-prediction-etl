import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

dag_id = "OT_Create_crude_oil_indicator_day"
tags = ["onetime", "create", "oil"]


def _task_failure_alert(context):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "ERROR")


def _init_hive_mart_table(query_file):
    from airflow_modules import trino_operation

    with open(query_file, "r") as f:
        query = f.read()

    logger.info("RUN QUERY")
    logger.info(query)
    trino_operation.run(query)


args = {"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="Create mart tables for crude oil price indicators",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    concurrency=1,  # can run N tasks at the same time
    max_active_runs=1,  # can run N DAGs at the same time
    tags=tags,
    default_args=args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    from airflow_modules import airflow_env_variables

    sys.path.append(airflow_env_variables.DWH_SCRIPT)
    from common import env_variables
    from airflow_modules import airflow_env_variables

    query_dir = "{}/trino".format(airflow_env_variables.QUERY_SCRIPT_HOME)
    query_script = "OT_Create_crude_oil_ind_day_001"
    init_hive_mart_table = PythonOperator(
        task_id="init_hive_mart_table",
        python_callable=_init_hive_mart_table,
        op_kwargs={"query_file": f"{query_dir}/{query_script}.sql"},
    )

    spark_script = "OT_Create_crude_oil_ind_day_001"
    create_crude_oil_indicators = SparkSubmitOperator(
        task_id="create_crude_oil_indicators",
        application="{}/pyspark/{}.py".format(airflow_env_variables.QUERY_SCRIPT_HOME, spark_script),
        conf={
            "spark.eventLog.dir": "hdfs://{}:{}{}".format(
                env_variables.HISTORY_SERVER_HOST,
                env_variables.HISTORY_SERVER_POST,
                env_variables.HISTORY_LOG_HOME,
            ),
            "spark.eventLog.enabled": "true",
        },
        conn_id="spark_conn",
        application_args=[
            env_variables.SPARK_MASTER_HOST,
            env_variables.SPARK_MASTER_PORT,
            env_variables.HIVE_METASTORE_HOST,
            env_variables.HIVE_METASTORE_PORT,
        ],
    )

    dag_end = DummyOperator(task_id="dag_end")

    (dag_start >> init_hive_mart_table >> create_crude_oil_indicators >> dag_end)
