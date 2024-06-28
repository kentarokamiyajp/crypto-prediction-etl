import sys
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

sys.path.append("/opt/airflow")
from conf.dag_common import default_args
from common_functions.notification import send_line_notification
from common_functions.cassandra_operation import batch_insertion_from_xcom, check_latest_dt
from common_functions.yahoofinancials_operation import (
    get_yahoofinancials_data,
    process_yahoofinancials_data,
)
from common_functions.trino_operation import run_query_by_replacing_params

# Common Variables
CASSANDRA_KEYSPACE_NAME = "gas"
TARGET_TABLE = "natural_gas_price_day"
DAYS_DELETE_FROM = 10
TRINO_ETL_HOME = "{}/trino".format(Variable.get("ETL_SCRIPTS_HOME"))

DAG_ID = f"D_Load_{TARGET_TABLE}"
TAGS = ["daily", "load", "gas"]

with DAG(
    DAG_ID,
    tags=TAGS,
    description="Load Natural Gas price data",
    schedule_interval="0 16 * * 5",
    on_failure_callback=lambda context: send_line_notification(
        context=context, dag_id=DAG_ID, tags=TAGS, type="ERROR"
    ),
    catchup=False,
    concurrency=1,  # can run N tasks at the same time
    max_active_runs=1,  # can run N DAGs at the same time
    default_args=default_args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    # Get price data via YahooFinancials
    get_natural_gas_price = PythonOperator(
        task_id="get_natural_gas_price",
        python_callable=get_yahoofinancials_data,
        op_kwargs={
            "symbols": ["NG=F"],
            "load_from_days": DAYS_DELETE_FROM,
            "interval": "daily",
        },
        do_xcom_push=True,
        pool="yfinance_pool",
    )

    # Process data to input it into Cassandra
    process_natural_gas_price = PythonOperator(
        task_id="process_natural_gas_price_for_ingestion",
        python_callable=process_yahoofinancials_data,
        op_kwargs={"task_id_for_xcom_pull": "get_natural_gas_price"},
        provide_context=True,
        do_xcom_push=True,
    )

    # Insert data into Cassandra
    query = f""" \
        INSERT INTO {TARGET_TABLE} \
            (id, low, high, open, close, volume, adjclose, currency, \
            unixtime_create, dt_create_utc, tz_gmtoffset, ts_insert_utc) \
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    insert_data_to_cassandra = PythonOperator(
        task_id="insert_natural_gas_price_data_to_cassandra",
        python_callable=batch_insertion_from_xcom,
        op_kwargs={
            "task_id_for_xcom_pull": "process_natural_gas_price_for_ingestion",
            "keyspace": CASSANDRA_KEYSPACE_NAME,
            "query": query,
            "batch_size": 100,
        },
        provide_context=True,
    )

    # Check if the latest data exists in Cassandra
    check_latest_date = PythonOperator(
        task_id="check_latest_dt_existence",
        python_callable=check_latest_dt,
        op_kwargs={
            "dag_id": DAG_ID,
            "tags": TAGS,
            "keyspace": CASSANDRA_KEYSPACE_NAME,
            "table_name": TARGET_TABLE,
            "target_id_value": "NG=F",
            "target_market_to_check_opening": "NYSE",
        },
    )

    # Delete past data from hive before ingesting new data
    replacement_params = [{"${N}": str(-i)} for i in range(0, DAYS_DELETE_FROM + 1)]
    delete_past_data_from_hive = PythonOperator(
        task_id="delete_past_data_from_hive",
        python_callable=run_query_by_replacing_params,
        op_kwargs={
            "query_file": f"{TRINO_ETL_HOME}/D_Load_{TARGET_TABLE}_001.sql",
            "replacement_params": replacement_params,
        },
    )

    # Check if the past data is deleted correctly
    replacement_params = [{"${N}": str(-DAYS_DELETE_FROM)}]
    hive_deletion_check = PythonOperator(
        task_id="hive_deletion_check",
        python_callable=run_query_by_replacing_params,
        op_kwargs={
            "query_file": f"{TRINO_ETL_HOME}/D_Load_{TARGET_TABLE}_002.sql",
            "replacement_params": replacement_params,
        },
    )

    # Ingest data from Cassandra to Hive
    replacement_params = [{"${N}": str(-DAYS_DELETE_FROM)}]
    ingest_from_cassandra_to_hive = PythonOperator(
        task_id="load_from_cassandra_to_hive",
        python_callable=run_query_by_replacing_params,
        op_kwargs={
            "query_file": f"{TRINO_ETL_HOME}/D_Load_{TARGET_TABLE}_003.sql",
            "replacement_params": replacement_params,
        },
    )

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> get_natural_gas_price
        >> process_natural_gas_price
        >> insert_data_to_cassandra
        >> check_latest_date
        >> delete_past_data_from_hive
        >> hive_deletion_check
        >> ingest_from_cassandra_to_hive
        >> dag_end
    )
