import sys
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

sys.path.append("/opt/airflow")
from conf.dag_common import default_args
from common_functions.notification import send_line_notification
from common_functions.cassandra_operation import batch_insertion_from_xcom, check_latest_dt
from common_functions.trino_operation import run_query_by_replacing_params
from common_functions.poloniex_operation import get_candle_minute_data, process_candle_data


# Common Variables
CASSANDRA_KEYSPACE_NAME = "crypto"
TARGET_TABLE = "candles_minute"
DAYS_DELETE_FROM = 10
TRINO_ETL_HOME = "{}/trino".format(Variable.get("ETL_SCRIPTS_HOME"))

DAG_ID = f"D_Load_crypto_{TARGET_TABLE}"
TAGS = ["daily", "load", "crypto"]

with DAG(
    DAG_ID,
    tags=TAGS,
    description="Load candles minute data daily",
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

    # Get crypto candle data via Poloniex API
    custom_assets = []  # You can add more assets if you want
    get_candle_data_via_poloniex = PythonOperator(
        task_id="get_candle_minute",
        python_callable=get_candle_minute_data,
        pool="poloniex_pool",
        op_kwargs={
            "custom_assets": custom_assets,
            "load_from_days": DAYS_DELETE_FROM,
        },
        do_xcom_push=True,
    )

    # Process data to input it into Cassandra
    process_poloniex_candle_data = PythonOperator(
        task_id="process_candle_data_for_ingestion",
        python_callable=process_candle_data,
        op_kwargs={"task_id_for_xcom_pull": "get_candle_minute"},
        provide_context=True,
        do_xcom_push=True,
    )

    # Insert data into Cassandra
    query = f""" \
    INSERT INTO {TARGET_TABLE} \
        (id,low,high,open,close,amount,quantity,buyTakerAmount, \
        buyTakerQuantity,tradeCount,ts,weightedAverage,interval, \
        startTime,closeTime,dt_create_utc,ts_create_utc,ts_insert_utc) \
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    insert_data_to_cassandra = PythonOperator(
        task_id="insert_candle_data_to_cassandra",
        python_callable=batch_insertion_from_xcom,
        op_kwargs={
            "task_id_for_xcom_pull": "process_candle_data_for_ingestion",
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
            "target_id_value": "BTC_USDT",
        },
    )

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
    replacement_params = [{"${N}": str(-DAYS_DELETE_FROM)}]
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
        >> get_candle_data_via_poloniex
        >> process_poloniex_candle_data
        >> insert_data_to_cassandra
        >> check_latest_date
        >> delete_past_data_from_hive
        >> hive_deletion_check
        >> ingest_from_cassandra_to_hive
        >> dag_end
    )
