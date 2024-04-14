import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, date
import logging

logger = logging.getLogger(__name__)

dag_id = "D_Create_indicator_day"
tags = ["daily", "create", "indicator", "pyspark"]


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
    description="Create mart tables for crude oil price indicators",
    schedule_interval="15 16 * * 5",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    concurrency=5,  # can run N tasks at the same time
    max_active_runs=1,  # can run N DAGs at the same time
    tags=tags,
    default_args=args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    ##############################################
    # Task Group to wait for previous tasks finish
    ##############################################
    _allowed_states = ["success"]
    _failed_states = ["failed", "skipped"]
    _check_existence = True
    _poke_interval = 10
    _execution_delta = timedelta(minutes=5)
    _mode = "reschedule"
    _timeout = 7200

    wait_prev_tasks = ExternalTaskSensor(
        task_id="wait_prev_tasks",
        external_dag_id="D_Check_trunk_load_end",
        external_task_id="dag_end",
        allowed_states=_allowed_states,
        failed_states=_failed_states,
        check_existence=_check_existence,
        poke_interval=_poke_interval,
        execution_delta=_execution_delta,
        mode=_mode,
        timeout=_timeout,
    )

    ##############################################
    # Task Group to create indicator for each feature
    ##############################################
    from airflow_modules import airflow_env_variables

    sys.path.append(airflow_env_variables.DWH_SCRIPT)
    from common import env_variables

    # Use data from N months ago to calculate indicators
    use_N_months_data_to_calculate_indicator = -3

    # Update/Insert indicator from <update_N_months_from> months ago.
    update_N_months_from = 1

    # spark configuration
    spark_conf = {
        "spark.eventLog.dir": "hdfs://{}:{}{}".format(
            env_variables.HISTORY_SERVER_HOST,
            env_variables.HISTORY_SERVER_POST,
            env_variables.HISTORY_LOG_HOME,
        ),
        "spark.eventLog.enabled": "true",
    }

    spark_conn_id = "spark_conn"
    pyspark_pool = "pyspark_pool"

    spark_application_args = [
        env_variables.SPARK_MASTER_HOST,
        env_variables.SPARK_MASTER_PORT,
        env_variables.HIVE_METASTORE_HOST,
        env_variables.HIVE_METASTORE_PORT,
        str(use_N_months_data_to_calculate_indicator),
        str(update_N_months_from),
    ]

    with TaskGroup(
        "spark_create_indicator", tooltip="Create indicators for each feature"
    ) as spark_create_indicator:
        create_crude_oil_indicator = SparkSubmitOperator(
            task_id="create_crude_oil_indicator",
            application="{}/pyspark/D_Create_crude_oil_ind_day_001.py".format(
                airflow_env_variables.QUERY_SCRIPT_HOME
            ),
            conf=spark_conf,
            conn_id=spark_conn_id,
            application_args=spark_application_args,
            pool=pyspark_pool,
        )

        create_crypto_indicator = SparkSubmitOperator(
            task_id="create_crypto_indicator",
            application="{}/pyspark/D_Create_crypto_ind_day_001.py".format(
                airflow_env_variables.QUERY_SCRIPT_HOME
            ),
            conf=spark_conf,
            conn_id=spark_conn_id,
            application_args=spark_application_args,
            pool=pyspark_pool,
        )

        create_forex_indicator = SparkSubmitOperator(
            task_id="create_forex_indicator",
            application="{}/pyspark/D_Create_forex_rate_ind_day_001.py".format(
                airflow_env_variables.QUERY_SCRIPT_HOME
            ),
            conf=spark_conf,
            conn_id=spark_conn_id,
            application_args=spark_application_args,
            pool=pyspark_pool,
        )

        create_gold_indicator = SparkSubmitOperator(
            task_id="create_gold_indicator",
            application="{}/pyspark/D_Create_gold_price_ind_day_001.py".format(
                airflow_env_variables.QUERY_SCRIPT_HOME
            ),
            conf=spark_conf,
            conn_id=spark_conn_id,
            application_args=spark_application_args,
            pool=pyspark_pool,
        )

        create_natural_gas_indicator = SparkSubmitOperator(
            task_id="create_natural_gas_indicator",
            application="{}/pyspark/D_Create_natural_gas_price_ind_day_001.py".format(
                airflow_env_variables.QUERY_SCRIPT_HOME
            ),
            conf=spark_conf,
            conn_id=spark_conn_id,
            application_args=spark_application_args,
            pool=pyspark_pool,
        )

        create_stock_index_indicator = SparkSubmitOperator(
            task_id="create_stock_index_indicator",
            application="{}/pyspark/D_Create_stock_index_value_ind_day_001.py".format(
                airflow_env_variables.QUERY_SCRIPT_HOME
            ),
            conf=spark_conf,
            conn_id=spark_conn_id,
            application_args=spark_application_args,
            pool=pyspark_pool,
        )

        [
            create_crude_oil_indicator,
            create_crypto_indicator,
            create_forex_indicator,
            create_gold_indicator,
            create_natural_gas_indicator,
            create_stock_index_indicator,
        ]

    ##############################################
    # Task Group to delete past data in hive mart
    ##############################################
    from airflow_modules import airflow_env_variables

    query_dir = "{}/trino".format(airflow_env_variables.QUERY_SCRIPT_HOME)
    trino_pool = "trino_pool"

    with TaskGroup("delete_past_data", tooltip="Delete past data in hive mart") as delete_past_data:
        delete_past_crude_oil_data = PythonOperator(
            task_id="delete_past_crude_oil_data",
            python_callable=_delete_from_hive_mart_table,
            op_kwargs={
                "query_file": f"{query_dir}/D_Create_crude_oil_ind_day_001.sql",
                "update_N_months_from": update_N_months_from,
            },
            pool=trino_pool,
        )

        delete_past_crypto_data = PythonOperator(
            task_id="delete_past_crypto_data",
            python_callable=_delete_from_hive_mart_table,
            op_kwargs={
                "query_file": f"{query_dir}/D_Create_crypto_ind_day_001.sql",
                "update_N_months_from": update_N_months_from,
            },
            pool=trino_pool,
        )

        delete_past_forex_data = PythonOperator(
            task_id="delete_past_forex_data",
            python_callable=_delete_from_hive_mart_table,
            op_kwargs={
                "query_file": f"{query_dir}/D_Create_forex_ind_day_001.sql",
                "update_N_months_from": update_N_months_from,
            },
            pool=trino_pool,
        )

        delete_past_gold_data = PythonOperator(
            task_id="delete_past_gold_data",
            python_callable=_delete_from_hive_mart_table,
            op_kwargs={
                "query_file": f"{query_dir}/D_Create_gold_ind_day_001.sql",
                "update_N_months_from": update_N_months_from,
            },
            pool=trino_pool,
        )

        delete_past_natural_gas_data = PythonOperator(
            task_id="delete_past_natural_gas_data",
            python_callable=_delete_from_hive_mart_table,
            op_kwargs={
                "query_file": f"{query_dir}/D_Create_natural_gas_ind_day_001.sql",
                "update_N_months_from": update_N_months_from,
            },
            pool=trino_pool,
        )

        delete_past_stock_index_data = PythonOperator(
            task_id="delete_past_stock_index_data",
            python_callable=_delete_from_hive_mart_table,
            op_kwargs={
                "query_file": f"{query_dir}/D_Create_stock_index_ind_day_001.sql",
                "update_N_months_from": update_N_months_from,
            },
            pool=trino_pool,
        )

        [
            delete_past_crude_oil_data,
            delete_past_crypto_data,
            delete_past_forex_data,
            delete_past_gold_data,
            delete_past_natural_gas_data,
            delete_past_stock_index_data,
        ]

    ##############################################
    # Task Group to insert updated indicator data to hive mart
    ##############################################
    with TaskGroup(
        "insert_updated_indicator", tooltip="Insert updated indicator data to hive mart"
    ) as insert_updated_indicator:
        insert_crude_oil_indicator = PythonOperator(
            task_id="insert_crude_oil_indicator",
            python_callable=_run_query_to_hive_mart_table,
            op_kwargs={"query_file": f"{query_dir}/D_Create_crude_oil_ind_day_002.sql"},
            pool=trino_pool,
        )

        insert_crypto_indicator = PythonOperator(
            task_id="insert_crypto_indicator",
            python_callable=_run_query_to_hive_mart_table,
            op_kwargs={"query_file": f"{query_dir}/D_Create_crypto_ind_day_002.sql"},
            pool=trino_pool,
        )

        insert_forex_indicator = PythonOperator(
            task_id="insert_forex_indicator",
            python_callable=_run_query_to_hive_mart_table,
            op_kwargs={"query_file": f"{query_dir}/D_Create_forex_ind_day_002.sql"},
            pool=trino_pool,
        )

        insert_gold_indicator = PythonOperator(
            task_id="insert_gold_indicator",
            python_callable=_run_query_to_hive_mart_table,
            op_kwargs={"query_file": f"{query_dir}/D_Create_gold_ind_day_002.sql"},
            pool=trino_pool,
        )

        insert_natural_gas_indicator = PythonOperator(
            task_id="insert_natural_gas_indicator",
            python_callable=_run_query_to_hive_mart_table,
            op_kwargs={"query_file": f"{query_dir}/D_Create_natural_gas_ind_day_002.sql"},
            pool=trino_pool,
        )

        insert_stock_index_indicator = PythonOperator(
            task_id="insert_stock_index_indicator",
            python_callable=_run_query_to_hive_mart_table,
            op_kwargs={"query_file": f"{query_dir}/D_Create_stock_index_ind_day_002.sql"},
            pool=trino_pool,
        )

        [
            insert_crude_oil_indicator,
            insert_crypto_indicator,
            insert_forex_indicator,
            insert_gold_indicator,
            insert_natural_gas_indicator,
            insert_stock_index_indicator,
        ]

    ##############################################
    # Task Group to delete WRK table from hive
    ##############################################
    with TaskGroup("delete_wrk_table", tooltip="Delete WRK table") as delete_wrk_table:
        delete_crude_oil_wrk_table = PythonOperator(
            task_id="delete_crude_oil_wrk_table",
            python_callable=_run_query_to_hive_mart_table,
            op_kwargs={"query_file": f"{query_dir}/D_Create_crude_oil_ind_day_003.sql"},
            pool=trino_pool,
        )

        delete_crypto_wrk_table = PythonOperator(
            task_id="delete_crypto_wrk_table",
            python_callable=_run_query_to_hive_mart_table,
            op_kwargs={"query_file": f"{query_dir}/D_Create_crypto_ind_day_003.sql"},
            pool=trino_pool,
        )

        delete_forex_wrk_table = PythonOperator(
            task_id="delete_forex_wrk_table",
            python_callable=_run_query_to_hive_mart_table,
            op_kwargs={"query_file": f"{query_dir}/D_Create_forex_ind_day_003.sql"},
            pool=trino_pool,
        )

        delete_gold_wrk_table = PythonOperator(
            task_id="delete_gold_wrk_table",
            python_callable=_run_query_to_hive_mart_table,
            op_kwargs={"query_file": f"{query_dir}/D_Create_gold_ind_day_003.sql"},
            pool=trino_pool,
        )

        delete_natural_gas_wrk_table = PythonOperator(
            task_id="delete_natural_gas_wrk_table",
            python_callable=_run_query_to_hive_mart_table,
            op_kwargs={"query_file": f"{query_dir}/D_Create_natural_gas_ind_day_003.sql"},
            pool=trino_pool,
        )

        delete_stock_index_wrk_table = PythonOperator(
            task_id="delete_stock_index_wrk_table",
            python_callable=_run_query_to_hive_mart_table,
            op_kwargs={"query_file": f"{query_dir}/D_Create_stock_index_ind_day_003.sql"},
            pool=trino_pool,
        )

        [
            delete_crude_oil_wrk_table,
            delete_crypto_wrk_table,
            delete_forex_wrk_table,
            delete_gold_wrk_table,
            delete_natural_gas_wrk_table,
            delete_stock_index_wrk_table,
        ]

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> wait_prev_tasks
        >> spark_create_indicator
        >> delete_past_data
        >> insert_updated_indicator
        >> delete_wrk_table
        >> dag_end
    )
