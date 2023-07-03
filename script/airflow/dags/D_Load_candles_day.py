import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta, date
import time
import env_settings
import logging

logger = logging.getLogger(__name__)

from modules import poloniex_operation, cassandra_operation, utils

def task_failure_alert(context):
    ts_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"{ts_now} [Failed] D_Load_candles_day"
    utils.send_line_message(message, env_settings.LINE_ACCESS_TOKEN)

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
    interval = "DAY_1"

    res = {}
    period = 60 * 24 * 2  # minute
    end = time.time()
    start = end - 60 * period
    for asset in assets:
        res[asset] = poloniex_operation.get_candle_data(asset, interval, start, end)
        time.sleep(10)

    return res


def _process_candle_data(ti):
    candle_data = ti.xcom_pull(task_ids="get_candle_day")
    res = utils.process_candle_data_from_poloniex(candle_data)

    return res


def _insert_data_to_cassandra(ti):
    keyspace = "crypto"
    table_name = "candles_day"

    candle_data = ti.xcom_pull(task_ids="process_candle_data_for_ingestion")
    cassandra_operation.insert_data(keyspace, table_name, candle_data)


def _check_latest_dt():
    # check if the expected data is inserted.
    keyspace = "crypto"
    table_name = "candles_day"
    target_asset = "BTC_USDT"
    prev_date_ts = datetime.today() - timedelta(days=1)
    prev_date = date(prev_date_ts.year, prev_date_ts.month, prev_date_ts.day).strftime("%Y-%m-%d")
    count = cassandra_operation.check_latest_dt(keyspace, table_name, prev_date, target_asset).one()[0]

    # If there is no data for prev-day, exit with error.
    if int(count) == 0:
        logger.error("There is no data for prev-day ({}, asset={})".format(prev_date, target_asset))
        raise AirflowFailException("Data missing error !!!")


with DAG(
    "D_Load_candles_day",
    description="Load candles day data",
    schedule_interval="0 0 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=task_failure_alert,
    tags=["D_Load", "candle"],
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    dag_end = DummyOperator(task_id="dag_end")

    get_candle_data = PythonOperator(task_id="get_candle_day", python_callable=_get_candle_data, do_xcom_push=True)

    process_candle_data = PythonOperator(task_id="process_candle_data_for_ingestion", python_callable=_process_candle_data, do_xcom_push=True)

    insert_data_to_cassandra = PythonOperator(task_id="insert_candle_data_to_cassandra", python_callable=_insert_data_to_cassandra)

    check_latest_dt = PythonOperator(task_id="check_latest_dt_existance", python_callable=_check_latest_dt)

    trigger = TriggerDagRunOperator(task_id="trigger_dagrun", trigger_dag_id="D_Load_candles_minute")

    dag_start >> get_candle_data >> process_candle_data >> insert_data_to_cassandra >> check_latest_dt >> trigger >> dag_end
