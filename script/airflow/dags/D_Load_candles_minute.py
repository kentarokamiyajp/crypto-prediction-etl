from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import time
from dwh_script.modules.utils import *
from modules import poloniex_operation, cassandra_operation, utils
import logging

logger = logging.getLogger(__name__)

def task_completed_notification():
    ts_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"{ts_now} [Completed] Airflow Dags: D_Load_candles_minute"
    send_line_message(message)

def task_failure_alert(context):
    ts_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"{ts_now} [Failed] Airflow Dags: D_Load_candles_minute"
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
    cassandra_operation.insert_data(keyspace, table_name, candle_data)


with DAG(
    "D_Load_candles_minute",
    description="Load candles minute data daily",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=task_failure_alert,
    tags=["D_Load", "candle"],
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    dag_end = PythonOperator(task_id="dag_end",  python_callable=task_completed_notification)

    get_candle_data = PythonOperator(task_id="get_candle_minite_for_1day", python_callable=_get_candle_data, do_xcom_push=True)

    process_candle_data = PythonOperator(task_id="process_candle_data_for_ingestion", python_callable=_process_candle_data, do_xcom_push=True)

    insert_data_to_cassandra = PythonOperator(task_id="insert_candle_data_to_cassandra", python_callable=_insert_data_to_cassandra)

    dag_start >> get_candle_data >> process_candle_data >> insert_data_to_cassandra >> dag_end
