import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta, date
import logging

logger = logging.getLogger(__name__)

dag_id = "D_Create_crypto_indicator_day"
tags = ["daily", "create", "crypto"]


def _task_failure_alert(context):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "ERROR")


def _delete_from_hive_mart_table(query_file, update_N_months_from):
    from airflow_modules import trino_operation

    with open(query_file, "r") as f:
        query_script = f.read()

    # Delete data from <update_N_months_from> month ago to the current month
    today = date.today()
    first_day_of_curr_month = today.replace(day=1)
    for i in range(update_N_months_from + 1):
        # Delete one-month data
        query = query_script.replace("${target_date}", str(first_day_of_curr_month))
        logger.info("RUN QUERY")
        logger.info(query)
        trino_operation.run(query)

        # Get the day when one day before <first_day_of_curr_month>
        last_day_of_prev_month = first_day_of_curr_month - timedelta(days=1)
        # Get the 1st day of the previous month.
        first_day_of_curr_month = last_day_of_prev_month.replace(day=1)


def _run_query_to_hive_mart_table(query_file):
    from airflow_modules import trino_operation

    with open(query_file, "r") as f:
        query = f.read()

    logger.info("RUN QUERY")
    logger.info(query)
    trino_operation.run(query)


args = {"owner": "airflow", "retries": 3, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="Create mart tables for crypto price indicators",
    schedule_interval="30 1 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    concurrency=1,  # can run N tasks at the same time
    max_active_runs=1,  # can run N DAGs at the same time
    tags=tags,
    default_args=args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    wait_for_D_Load_crypto_candles_day = ExternalTaskSensor(
        task_id="wait_for_D_Load_crypto_candles_day",
        external_dag_id="D_Load_crypto_candles_day",
        external_task_id="dag_end",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        check_existence=True,
        poke_interval=60,
        execution_delta=timedelta(minutes=30),
        mode="reschedule",
        timeout=1800,
    )

    from airflow_modules import airflow_env_variables

    sys.path.append(airflow_env_variables.DWH_SCRIPT)
    from common import env_variables

    # Use data from N months ago to calculate indicators
    use_N_months_data_to_calculate_indicator = -3
    # Update/Insert indicator from <update_N_months_from> months ago.
    update_N_months_from = 1
    spark_create_indicators = SparkSubmitOperator(
        task_id="create_indicators",
        application="{}/pyspark/D_Create_crypto_ind_day_001.py".format(
            airflow_env_variables.QUERY_SCRIPT_HOME
        ),
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
            str(use_N_months_data_to_calculate_indicator),
            str(update_N_months_from),
        ],
    )

    from airflow_modules import airflow_env_variables

    query_dir = "{}/trino".format(airflow_env_variables.QUERY_SCRIPT_HOME)
    delete_from_hive_mart_table = PythonOperator(
        task_id="delete_from_hive_mart_table",
        python_callable=_delete_from_hive_mart_table,
        op_kwargs={
            "query_file": f"{query_dir}/D_Create_crypto_ind_day_001.sql",
            "update_N_months_from": update_N_months_from,
        },
    )

    insert_into_hive_mart_table = PythonOperator(
        task_id="insert_into_hive_mart_table",
        python_callable=_run_query_to_hive_mart_table,
        op_kwargs={"query_file": f"{query_dir}/D_Create_crypto_ind_day_002.sql"},
    )

    delete_hive_wrk_table = PythonOperator(
        task_id="delete_hive_wrk_table",
        python_callable=_run_query_to_hive_mart_table,
        op_kwargs={"query_file": f"{query_dir}/D_Create_crypto_ind_day_003.sql"},
    )

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> wait_for_D_Load_crypto_candles_day
        >> spark_create_indicators
        >> delete_from_hive_mart_table
        >> insert_into_hive_mart_table
        >> delete_hive_wrk_table
        >> dag_end
    )
