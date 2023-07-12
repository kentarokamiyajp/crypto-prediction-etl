import sys

sys.path.append("..")
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta, date
from dwh_script.modules.utils import *
from modules import yahoofinancials_operation, utils, cassandra_operation
import logging

logger = logging.getLogger(__name__)


def task_failure_alert(context):
    ts_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"{ts_now} [Failed] Airflow Dags: D_Load_stock_index_value_day"
    send_line_message(message)


def _get_stock_index_value():
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

    return yahoofinancials_operation.get_data_from_yahoofinancials(tickers)


def _process_stock_index_value(ti):
    stock_index_value = ti.xcom_pull(task_ids="get_stock_index_value")
    return utils.process_yahoofinancials_data(stock_index_value)

def _insert_data_to_cassandra(ti):
    keyspace = "stock"
    table_name = "stock_index_history"
    stock_index_value = ti.xcom_pull(task_ids="process_stock_index_value_for_ingestion")

    query = f"""
    INSERT INTO {table_name} (id,low,high,open,close,volume,adjclose,currency,date_unix,dt,ts_insert_utc)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cassandra_operation.insert_data(keyspace, stock_index_value, query)


def _check_latest_dt():
    # check if the expected data is inserted.
    keyspace = "stock"
    table_name = "stock_index_history"
    target_index = "^NDX"
    prev_date_ts = datetime.today() - timedelta(days=1)
    prev_date = date(prev_date_ts.year, prev_date_ts.month, prev_date_ts.day).strftime("%Y-%m-%d")

    query = f"""
    select count(*) from {table_name} where dt = '{prev_date}' and id = '{target_index}'
    """
    count = cassandra_operation.check_latest_dt(keyspace, query).one()[0]

    # If there is no data for prev-day, exit with error.
    if int(count) == 0:
        logger.error("There is no data for prev_date ({}, asset={})".format(prev_date, target_index))
        raise AirflowFailException("Data missing error !!!")


with DAG(
    "D_Load_stock_index_value_day",
    description="Load nasdaq-100 data",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=task_failure_alert,
    tags=["D_Load", "stock_index"],
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    dag_end = DummyOperator(task_id="dag_end")

    get_stock_index_value = PythonOperator(task_id="get_stock_index_value", python_callable=_get_stock_index_value, do_xcom_push=True)

    process_stock_index_value = PythonOperator(
        task_id="process_stock_index_value_for_ingestion", python_callable=_process_stock_index_value, do_xcom_push=True
    )

    insert_data_to_cassandra = PythonOperator(task_id="insert_candle_data_to_cassandra", python_callable=_insert_data_to_cassandra)

    check_latest_dt = PythonOperator(task_id="check_latest_dt_existance", python_callable=_check_latest_dt)

    dag_start >> get_stock_index_value >> process_stock_index_value >> insert_data_to_cassandra >> check_latest_dt >> dag_end
