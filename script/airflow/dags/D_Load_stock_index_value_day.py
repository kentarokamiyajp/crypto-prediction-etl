import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta, date
import logging

logger = logging.getLogger(__name__)

dag_id = "D_Load_stock_index_value_day"
tags = ["D_Load", "stock_index"]


def _task_failure_alert(context):
    from airflow_modules import send_notification

    send_notification(dag_id, tags, "ERROR")


def _send_warning_notification(optional_message=None):
    from airflow_modules import send_notification

    send_notification(dag_id, tags, "WARNING", optional_message)


def _get_stock_index_value():
    from airflow_modules import yahoofinancials_operation, utils
    import time

    tickers = [
        "^NDX",
        "^DJI",
        "^DJT",
        "^DJU",
        "^BANK",
        "^IXCO",
        "^NBI",
        "^NDXT",
        "^INDS",
        "^INSR",
        "^OFIN",
        "^IXTC",
        "^TRAN",
        "^NYY",
        "^NYI",
        "^NY",
        "^NYL",
        "^XMI",
        "^OEX",
        "^GSPC",
        "^HSI",
        "^FCHI",
        "^BVSP",
        "^N225",
        "^RUA",
        "^XAX",
    ]

    interval = "daily"

    # how many days ago you want to get.
    target_days = 7

    # seconds of one day
    seconds_of_one_day = 60 * 60 * 24
    period = seconds_of_one_day * target_days

    # to this time to get the past data
    to_ts = time.time()

    # from this time to get the past data
    from_ts = to_ts - period

    from_date = utils.get_dt_from_unix_time(from_ts)
    to_date = utils.get_dt_from_unix_time(to_ts)

    logger.info("Load from {} to {}".format(from_date, to_date))

    return yahoofinancials_operation.get_data_from_yahoofinancials(
        tickers, interval, from_date, to_date
    )


def _process_stock_index_value(ti):
    from airflow_modules import utils

    stock_index_value = ti.xcom_pull(task_ids="get_stock_index_value")
    return utils.process_yahoofinancials_data(stock_index_value)


def _insert_data_to_cassandra(ti):
    from airflow_modules import cassandra_operation

    keyspace = "stock"
    table_name = "stock_index_day"
    stock_index_value = ti.xcom_pull(task_ids="process_stock_index_value_for_ingestion")

    query = f"""
    INSERT INTO {table_name} (id,low,high,open,close,volume,adjclose,currency,dt_unix,dt,tz_gmtoffset,ts_insert_utc)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cassandra_operation.insert_data(keyspace, stock_index_value, query)


def _check_latest_dt():
    import pytz
    from airflow_modules import cassandra_operation, utils

    # check if the expected data is inserted.
    keyspace = "stock"
    table_name = "stock_index_day"
    target_index = "^NDX"
    tz = pytz.timezone("EST")  # Time zone for NYSE
    prev_date_ts = datetime.now(tz) - timedelta(days=1)
    prev_date = date(prev_date_ts.year, prev_date_ts.month, prev_date_ts.day).strftime(
        "%Y-%m-%d"
    )

    query = f"""
    select count(*) from {table_name} where dt = '{prev_date}' and id = '{target_index}'
    """
    count = cassandra_operation.check_latest_dt(keyspace, query).one()[0]

    # If there is no data for prev-day even on the market holiday, exit with error.
    market = "NYSE"
    if int(count) == 0 and utils.is_makert_open(prev_date, market):
        warning_message = "There is no data for prev_date ({}, asset:{})".format(
            prev_date, target_index
        )
        logger.warn(warning_message)
        _send_warning_notification(warning_message)


def _load_from_cassandra_to_hive(query_file):
    from airflow_modules import trino_operation

    with open(query_file, "r") as f:
        query = f.read()
    trino_operation.run(query)


args = {"owner": "airflow", "retries": 5, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="Load nasdaq-100 data",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    tags=tags,
    default_args=args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    get_stock_index_value = PythonOperator(
        task_id="get_stock_index_value",
        python_callable=_get_stock_index_value,
        do_xcom_push=True,
    )

    process_stock_index_value = PythonOperator(
        task_id="process_stock_index_value_for_ingestion",
        python_callable=_process_stock_index_value,
        do_xcom_push=True,
    )

    insert_data_to_cassandra = PythonOperator(
        task_id="insert_candle_data_to_cassandra",
        python_callable=_insert_data_to_cassandra,
    )

    check_latest_dt = PythonOperator(
        task_id="check_latest_dt_existance", python_callable=_check_latest_dt
    )

    from airflow_modules import airflow_env_variables

    query_dir = "{}/trino".format(airflow_env_variables.QUERY_SCRIPT)
    load_from_cassandra_to_hive = PythonOperator(
        task_id="load_from_cassandra_to_hive",
        python_callable=_load_from_cassandra_to_hive,
        op_kwargs={"query_file": f"{query_dir}/D_Load_stock_index_value_day_001.sql"},
    )

    trigger = TriggerDagRunOperator(
        task_id="trigger_dagrun", trigger_dag_id="D_Load_natural_gas_price_day"
    )

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> get_stock_index_value
        >> process_stock_index_value
        >> insert_data_to_cassandra
        >> check_latest_dt
        >> load_from_cassandra_to_hive
        >> trigger
        >> dag_end
    )
