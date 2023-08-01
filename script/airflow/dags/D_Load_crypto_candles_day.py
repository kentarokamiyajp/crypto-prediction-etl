import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta, date
import logging

logger = logging.getLogger(__name__)

dag_id = "D_Load_crypto_candles_day"
tags = ["D_Load", "crypto"]


def _task_failure_alert(context):
    from airflow_modules import send_notification

    send_notification(dag_id, tags, "ERROR")


def _send_warning_notification(optional_message=None):
    from airflow_modules import send_notification

    send_notification(dag_id, tags, "WARNING", optional_message)


def _get_candle_data():
    import time
    from airflow_modules import poloniex_operation

    assets = [
        "ADA_USDT",
        "BCH_USDT",
        "BNB_USDT",
        "BTC_USDT",
        "DOGE_USDT",
        "ETH_USDT",
        "LTC_USDT",
        "MKR_USDT",
        "SHIB_USDT",
        "TRX_USDT",
        "XRP_USDT",
    ]
    interval = "DAY_1"

    res = {}
    days = 7  # how many days ago you want to get
    period = 60 * 24 * days  # minute
    end = time.time()
    start = end - 60 * period
    for asset in assets:
        logger.info("{}: Load from {} to {}".format(asset, start, end))
        res[asset] = poloniex_operation.get_candle_data(asset, interval, start, end)
        time.sleep(10)

    return res


def _process_candle_data(ti):
    from airflow_modules import utils

    candle_data = ti.xcom_pull(task_ids="get_candle_day")
    res = utils.process_candle_data_from_poloniex(candle_data)

    return res


def _insert_data_to_cassandra(ti):
    from airflow_modules import cassandra_operation

    keyspace = "crypto"
    table_name = "candles_day"
    candle_data = ti.xcom_pull(task_ids="process_candle_data_for_ingestion")

    query = f"""
    INSERT INTO {table_name} (id,low,high,open,close,amount,quantity,buyTakerAmount,\
        buyTakerQuantity,tradeCount,ts,weightedAverage,interval,startTime,closeTime,dt,ts_insert_utc)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cassandra_operation.insert_data(keyspace, candle_data, query)


def _check_latest_dt():
    from airflow_modules import cassandra_operation

    # check if the expected data is inserted.
    keyspace = "crypto"
    table_name = "candles_day"
    target_asset = "BTC_USDT"
    prev_date_ts = datetime.today() - timedelta(days=1)
    prev_date = date(prev_date_ts.year, prev_date_ts.month, prev_date_ts.day).strftime(
        "%Y-%m-%d"
    )

    query = f"""
    select count(*) from {table_name} where dt = '{prev_date}' and id = '{target_asset}'
    """

    count = cassandra_operation.check_latest_dt(keyspace, query).one()[0]

    # If there is no data for prev-day, exit with error.
    if int(count) == 0:
        warning_message = "There is no data for prev_date ({}, asset:{})".format(
            prev_date, target_asset
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
    description="Load candles day data",
    schedule_interval="0 1 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    tags=tags,
    default_args=args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    get_candle_data = PythonOperator(
        task_id="get_candle_day", python_callable=_get_candle_data, do_xcom_push=True
    )

    process_candle_data = PythonOperator(
        task_id="process_candle_data_for_ingestion",
        python_callable=_process_candle_data,
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
        op_kwargs={"query_file": f"{query_dir}/D_Load_crypto_candles_day_001.sql"},
    )

    trigger = TriggerDagRunOperator(
        task_id="trigger_dagrun", trigger_dag_id="D_Load_crypto_candles_minute"
    )

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> get_candle_data
        >> process_candle_data
        >> insert_data_to_cassandra
        >> check_latest_dt
        >> load_from_cassandra_to_hive
        >> trigger
        >> dag_end
    )
