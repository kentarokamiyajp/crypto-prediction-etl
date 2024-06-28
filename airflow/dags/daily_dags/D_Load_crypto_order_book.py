import sys
import itertools
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

sys.path.append("/opt/airflow")
from conf.dag_common import default_args
from common_functions.notification import send_line_notification
from common_functions.trino_operation import run_query_by_replacing_params


# Common Variables
TARGET_TABLE = "order_book"
DAYS_DELETE_FROM = 9
TRINO_ETL_HOME = "{}/trino".format(Variable.get("ETL_SCRIPTS_HOME"))

DAG_ID = f"D_Load_crypto_{TARGET_TABLE}"
TAGS = ["daily", "load", "crypto"]

with DAG(
    DAG_ID,
    tags=TAGS,
    description="Load order book data collected by Kafka producer",
    schedule_interval="0 18 * * 5",
    on_failure_callback=lambda context: send_line_notification(
        context=context, dag_id=DAG_ID, tags=TAGS, type="ERROR"
    ),
    catchup=False,
    concurrency=1,  # can run N tasks at the same time
    max_active_runs=1,  # can run N DAGs at the same time
    default_args=default_args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    # Delete past data from hive before ingesting new data
    replacement_params = [{"${N}": str(-i)} for i in range(0, DAYS_DELETE_FROM + 1)]
    delete_past_data_from_hive = PythonOperator(
        task_id="delete_past_data_from_hive",
        python_callable=run_query_by_replacing_params,
        op_kwargs={
            "query_file": f"{TRINO_ETL_HOME}/D_Load_crypto_{TARGET_TABLE}_001.sql",
            "replacement_params": replacement_params,
        },
    )

    # Check if the past data is deleted correctly
    replacement_params = [{"${N}": str(-DAYS_DELETE_FROM)}]
    hive_deletion_check = PythonOperator(
        task_id="hive_deletion_check",
        python_callable=run_query_by_replacing_params,
        op_kwargs={
            "query_file": f"{TRINO_ETL_HOME}/D_Load_crypto_{TARGET_TABLE}_002.sql",
            "replacement_params": replacement_params,
        },
    )

    # Ingest data from Cassandra to Hive
    symbols_to_load = ["BTC_USDT", "ETH_USDT"]
    N_days_range = [i for i in range(0, DAYS_DELETE_FROM + 1)]
    replacement_params = [
        {"${symbol}": symbol, "${N}": str(-N_days)}
        for symbol, N_days in itertools.product(symbols_to_load, N_days_range)
    ]
    ingest_from_cassandra_to_hive = PythonOperator(
        task_id="load_from_cassandra_to_hive",
        python_callable=run_query_by_replacing_params,
        op_kwargs={
            "query_file": f"{TRINO_ETL_HOME}/D_Load_crypto_{TARGET_TABLE}_003.sql",
            "replacement_params": replacement_params,
        },
    )

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> delete_past_data_from_hive
        >> hive_deletion_check
        >> ingest_from_cassandra_to_hive
        >> dag_end
    )
