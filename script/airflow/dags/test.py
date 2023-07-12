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
from airflow_modules import poloniex_operation, cassandra_operation, utils
import logging

logger = logging.getLogger(__name__)


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

    period = 60 * 24 * 2  # minute
    end = time.time()
    start = end - 60 * period
    poloniex_operation.test(assets[0], interval, start, end)



with DAG("test", description="DAG test", schedule_interval=None, start_date=datetime(2023, 1, 1), catchup=False, tags=["test"]) as dag:
    create_file_task = PythonOperator(task_id="create_file", python_callable=_get_candle_data)

    create_file_task