import sys

sys.path.append("/opt/airflow/git/crypto_prediction_dwh/script/")
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta, date
import time
from modules.utils import *
from airflow_modules import (
    poloniex_operation,
    cassandra_operation,
    trino_operation,
    utils,
)
import logging
import pytz

jst = pytz.timezone("Asia/Tokyo")
logger = logging.getLogger(__name__)

dag_id = "D_Load_crypto_candles_minute"


def _task_failure_alert(context):
    ts_now = datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S")
    message = f"{ts_now} [Failed] Airflow Dags: {dag_id}"
    send_line_message(message)


def _get_candle_data():
    assets = [
        "BTC_USDT",
        "ETH_USDT",
        "BNB_USDT",
        "XRP_USDT",
        "ADA_USDT",
        "DOGE_USDT",
        "SOL_USDT",
        "TRX_USDD",
        "UNI_USDT",
        "ATOM_USDT",
        "GMX_USDT",
        "SHIB_USDT",
        "MKR_USDT",
    ]

    interval = "MINUTE_1"

    res = {}
    initial_end = time.time()
    for asset in assets:
        for i in reversed(range(1, 5)):
            start = initial_end - (60 * 500 * i)
            end = initial_end - (60 * 500 * (i - 1))
            candle_data = poloniex_operation.get_candle_data(asset, interval, start, end)
            if asset in res:
                res[asset] += candle_data
            else:
                res[asset] = candle_data
            time.sleep(10)
    return res


def _process_candle_data(ti):
    candle_data = ti.xcom_pull(task_ids="get_candle_minite_for_1day")
    res = utils.process_candle_data_from_poloniex(candle_data)

    return res


def _insert_data_to_cassandra(ti):
    keyspace = "crypto"
    table_name = "candles_minute"
    candle_data = ti.xcom_pull(task_ids="process_candle_data_for_ingestion")

    query = f"""
    INSERT INTO {table_name} (id,low,high,open,close,amount,quantity,buyTakerAmount,\
        buyTakerQuantity,tradeCount,ts,weightedAverage,interval,startTime,closeTime,dt,ts_insert_utc)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cassandra_operation.insert_data(keyspace, candle_data, query)


def _check_latest_dt():
    # check if the expected data is inserted.
    keyspace = "crypto"
    table_name = "candles_minute"
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
        logger.error(
            "There is no data for prev-day ({}, asset={})".format(prev_date, target_asset)
        )
        raise AirflowFailException("Data missing error !!!")


def _load_from_cassandra_to_hive(query_file):
    with open(query_file, "r") as f:
        query = f.read()
    trino_operation.run(query)


with DAG(
    dag_id,
    description="Load candles minute data daily",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    tags=["D_Load", "crypto"],
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    get_candle_data = PythonOperator(
        task_id="get_candle_minite_for_1day",
        python_callable=_get_candle_data,
        do_xcom_push=True,
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

    query_dir = (
        "/opt/airflow/git/crypto_prediction_dwh/script/airflow/dags/query_script/trino"
    )
    load_from_cassandra_to_hive = PythonOperator(
        task_id="load_from_cassandra_to_hive",
        python_callable=_load_from_cassandra_to_hive,
        op_kwargs={"query_file": f"{query_dir}/D_Load_crypto_candles_minute_001.sql"},
    )

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> get_candle_data
        >> process_candle_data
        >> insert_data_to_cassandra
        >> check_latest_dt
        >> load_from_cassandra_to_hive
        >> dag_end
    )
