import sys
from datetime import timedelta, date
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

sys.path.append("/opt/airflow")
from conf.dag_common import default_args, default_task_sensor_args
from common_functions.notification import send_line_notification
from common_functions.trino_operation import run_query, run_query_by_replacing_params
from dwh_modules.common import env_variables


def _prepare_replacement_params(update_N_months_from: int) -> list:
    """Run a query for each month based on ${update_N_months_from}
    e.g.,
        today='2024-06-30', update_N_months_from = 3
        ${replacement_params} will contain '2024-06-01', '2024-05-01', '2024-04-01', '2024-03-01'.

    Args:
        update_N_months_from (int): From current month to N months before

    Returns:
        list: list of param (-> "${target_date}") and value (-> '20yy-MM-01') pairs
    """
    replacement_params = []

    today = date.today()
    first_day_of_curr_month = today.replace(day=1)
    for _ in range(update_N_months_from + 1):
        replacement_params.append({"${target_date}": str(first_day_of_curr_month)})

        # Get the day when one day before <first_day_of_curr_month>
        last_day_of_prev_month = first_day_of_curr_month - timedelta(days=1)
        # Get the 1st day of the previous month.
        first_day_of_curr_month = last_day_of_prev_month.replace(day=1)

    return replacement_params


# Common Variables
TRINO_ETL_SCRIPTS_HOME = "{}/trino".format(Variable.get("ETL_SCRIPTS_HOME"))
TRINO_POOL = "trino_pool"

PYSPARK_ETL_SCRIPTS_HOME = "{}/pyspark".format(Variable.get("ETL_SCRIPTS_HOME"))
SPARK_CONN_ID = "spark_conn"
SPARK_POOL = "pyspark_pool"

DAG_ID = "D_Create_indicator_day"
TAGS = ["daily", "create", "indicator", "pyspark"]

with DAG(
    DAG_ID,
    tags=TAGS,
    description="Create mart tables for crude oil price indicators",
    schedule_interval="15 16 * * 5",
    on_failure_callback=lambda context: send_line_notification(
        context=context, dag_id=DAG_ID, tags=TAGS, type="ERROR"
    ),
    catchup=False,
    concurrency=1,  # can run N tasks at the same time
    max_active_runs=1,  # can run N DAGs at the same time
    default_args=default_args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    ##############################################
    # Task Group to wait for previous tasks finish
    ##############################################
    default_task_sensor_args["timeout"] = 7200
    wait_prev_tasks = ExternalTaskSensor(
        task_id="wait_prev_tasks",
        external_dag_id="D_Check_trunk_load_end",
        external_task_id="dag_end",
        execution_delta=timedelta(minutes=5),
        **default_task_sensor_args,
    )

    ##############################################
    # Task Group to create indicator for each feature (save the indicator data into wrk tables)
    ##############################################
    # Use data from N months ago to calculate indicators
    N_months_data_to_calculate_indicator = 3

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

    spark_application_args = [
        Variable.get("DWH_SCRIPT"),
        str(N_months_data_to_calculate_indicator),
        str(update_N_months_from),
    ]

    with TaskGroup(
        "spark_create_indicator", tooltip="Create indicators for each feature"
    ) as spark_create_indicator:
        create_crude_oil_indicator = SparkSubmitOperator(
            task_id="create_crude_oil_indicator",
            application=f"{PYSPARK_ETL_SCRIPTS_HOME}/D_Create_crude_oil_ind_day_001.py",
            conf=spark_conf,
            conn_id=SPARK_CONN_ID,
            application_args=spark_application_args,
            pool=SPARK_POOL,
        )

        create_crypto_indicator = SparkSubmitOperator(
            task_id="create_crypto_indicator",
            application=f"{PYSPARK_ETL_SCRIPTS_HOME}/D_Create_crypto_ind_day_001.py",
            conf=spark_conf,
            conn_id=SPARK_CONN_ID,
            application_args=spark_application_args,
            pool=SPARK_POOL,
        )

        create_forex_indicator = SparkSubmitOperator(
            task_id="create_forex_indicator",
            application=f"{PYSPARK_ETL_SCRIPTS_HOME}/D_Create_forex_rate_ind_day_001.py",
            conf=spark_conf,
            conn_id=SPARK_CONN_ID,
            application_args=spark_application_args,
            pool=SPARK_POOL,
        )

        create_gold_indicator = SparkSubmitOperator(
            task_id="create_gold_indicator",
            application=f"{PYSPARK_ETL_SCRIPTS_HOME}/D_Create_gold_price_ind_day_001.py",
            conf=spark_conf,
            conn_id=SPARK_CONN_ID,
            application_args=spark_application_args,
            pool=SPARK_POOL,
        )

        create_natural_gas_indicator = SparkSubmitOperator(
            task_id="create_natural_gas_indicator",
            application=f"{PYSPARK_ETL_SCRIPTS_HOME}/D_Create_natural_gas_price_ind_day_001.py",
            conf=spark_conf,
            conn_id=SPARK_CONN_ID,
            application_args=spark_application_args,
            pool=SPARK_POOL,
        )

        create_stock_index_indicator = SparkSubmitOperator(
            task_id="create_stock_index_indicator",
            application=f"{PYSPARK_ETL_SCRIPTS_HOME}/D_Create_stock_index_value_ind_day_001.py",
            conf=spark_conf,
            conn_id=SPARK_CONN_ID,
            application_args=spark_application_args,
            pool=SPARK_POOL,
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
    replacement_params = _prepare_replacement_params(update_N_months_from)
    with TaskGroup("delete_past_data", tooltip="Delete past data in hive mart") as delete_past_data:
        delete_past_crude_oil_data = PythonOperator(
            task_id="delete_past_crude_oil_data",
            python_callable=run_query_by_replacing_params,
            op_kwargs={
                "query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_crude_oil_ind_day_001.sql",
                "replacement_params": replacement_params,
            },
            pool=TRINO_POOL,
        )

        delete_past_crypto_data = PythonOperator(
            task_id="delete_past_crypto_data",
            python_callable=run_query_by_replacing_params,
            op_kwargs={
                "query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_crypto_ind_day_001.sql",
                "replacement_params": replacement_params,
            },
            pool=TRINO_POOL,
        )

        delete_past_forex_data = PythonOperator(
            task_id="delete_past_forex_data",
            python_callable=run_query_by_replacing_params,
            op_kwargs={
                "query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_forex_ind_day_001.sql",
                "replacement_params": replacement_params,
            },
            pool=TRINO_POOL,
        )

        delete_past_gold_data = PythonOperator(
            task_id="delete_past_gold_data",
            python_callable=run_query_by_replacing_params,
            op_kwargs={
                "query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_gold_ind_day_001.sql",
                "replacement_params": replacement_params,
            },
            pool=TRINO_POOL,
        )

        delete_past_natural_gas_data = PythonOperator(
            task_id="delete_past_natural_gas_data",
            python_callable=run_query_by_replacing_params,
            op_kwargs={
                "query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_natural_gas_ind_day_001.sql",
                "replacement_params": replacement_params,
            },
            pool=TRINO_POOL,
        )

        delete_past_stock_index_data = PythonOperator(
            task_id="delete_past_stock_index_data",
            python_callable=run_query_by_replacing_params,
            op_kwargs={
                "query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_stock_index_ind_day_001.sql",
                "replacement_params": replacement_params,
            },
            pool=TRINO_POOL,
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
            python_callable=run_query,
            op_kwargs={
                "query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_crude_oil_ind_day_002.sql"
            },
            pool=TRINO_POOL,
        )

        insert_crypto_indicator = PythonOperator(
            task_id="insert_crypto_indicator",
            python_callable=run_query,
            op_kwargs={"query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_crypto_ind_day_002.sql"},
            pool=TRINO_POOL,
        )

        insert_forex_indicator = PythonOperator(
            task_id="insert_forex_indicator",
            python_callable=run_query,
            op_kwargs={"query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_forex_ind_day_002.sql"},
            pool=TRINO_POOL,
        )

        insert_gold_indicator = PythonOperator(
            task_id="insert_gold_indicator",
            python_callable=run_query,
            op_kwargs={"query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_gold_ind_day_002.sql"},
            pool=TRINO_POOL,
        )

        insert_natural_gas_indicator = PythonOperator(
            task_id="insert_natural_gas_indicator",
            python_callable=run_query,
            op_kwargs={
                "query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_natural_gas_ind_day_002.sql"
            },
            pool=TRINO_POOL,
        )

        insert_stock_index_indicator = PythonOperator(
            task_id="insert_stock_index_indicator",
            python_callable=run_query,
            op_kwargs={
                "query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_stock_index_ind_day_002.sql"
            },
            pool=TRINO_POOL,
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
            python_callable=run_query,
            op_kwargs={
                "query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_crude_oil_ind_day_003.sql"
            },
            pool=TRINO_POOL,
        )

        delete_crypto_wrk_table = PythonOperator(
            task_id="delete_crypto_wrk_table",
            python_callable=run_query,
            op_kwargs={"query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_crypto_ind_day_003.sql"},
            pool=TRINO_POOL,
        )

        delete_forex_wrk_table = PythonOperator(
            task_id="delete_forex_wrk_table",
            python_callable=run_query,
            op_kwargs={"query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_forex_ind_day_003.sql"},
            pool=TRINO_POOL,
        )

        delete_gold_wrk_table = PythonOperator(
            task_id="delete_gold_wrk_table",
            python_callable=run_query,
            op_kwargs={"query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_gold_ind_day_003.sql"},
            pool=TRINO_POOL,
        )

        delete_natural_gas_wrk_table = PythonOperator(
            task_id="delete_natural_gas_wrk_table",
            python_callable=run_query,
            op_kwargs={
                "query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_natural_gas_ind_day_003.sql"
            },
            pool=TRINO_POOL,
        )

        delete_stock_index_wrk_table = PythonOperator(
            task_id="delete_stock_index_wrk_table",
            python_callable=run_query,
            op_kwargs={
                "query_file": f"{TRINO_ETL_SCRIPTS_HOME}/D_Create_stock_index_ind_day_003.sql"
            },
            pool=TRINO_POOL,
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
