import sys

sys.path.append("/opt/airflow/git/crypto_prediction_dwh/script/")
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta, date
from modules.utils import *
from airflow_modules import yahoofinancials_operation, utils, cassandra_operation
import logging

logger = logging.getLogger(__name__)


def task_failure_alert(context):
    ts_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"{ts_now} [Failed] Airflow Dags: D_Load_forex_rate_day"
    send_line_message(message)


def _get_forex_rate():
    currencies = ['EURUSD=X', 'GBPUSD=X', 'JPY=X']
    return yahoofinancials_operation.get_data_from_yahoofinancials(currencies)


def _process_forex_rate(ti):
    forex_rate = ti.xcom_pull(task_ids="get_forex_rate")
    return utils.process_yahoofinancials_data(forex_rate)


def _insert_data_to_cassandra(ti):
    keyspace = "forex"
    table_name = "forex_rate_history"
    forex_rate = ti.xcom_pull(task_ids="process_forex_rate_for_ingestion")

    query = f"""
    INSERT INTO {table_name} (id,low,high,open,close,volume,adjclose,currency,date_unix,dt,ts_insert_utc)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cassandra_operation.insert_data(keyspace, forex_rate, query)


def _check_latest_dt():
    # check if the expected data is inserted.
    keyspace = "forex"
    table_name = "forex_rate_history"
    target_index = "EURUSD=X"
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
    "D_Load_forex_rate_day",
    description="Load forex rate data",
    schedule_interval="0 0 * * 2-6",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=task_failure_alert,
    tags=["D_Load", "forex_rate"],
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    dag_end = DummyOperator(task_id="dag_end")

    get_forex_rate = PythonOperator(task_id="get_forex_rate", python_callable=_get_forex_rate, do_xcom_push=True)

    process_forex_rate = PythonOperator(
        task_id="process_forex_rate_for_ingestion", python_callable=_process_forex_rate, do_xcom_push=True
    )

    insert_data_to_cassandra = PythonOperator(task_id="insert_forex_rate_data_to_cassandra", python_callable=_insert_data_to_cassandra)

    check_latest_dt = PythonOperator(task_id="check_latest_dt_existance", python_callable=_check_latest_dt)
    
    trigger = TriggerDagRunOperator(task_id="trigger_dagrun", trigger_dag_id="D_Load_stock_index_value_day")

    dag_start >> get_forex_rate >> process_forex_rate >> insert_data_to_cassandra >> check_latest_dt >> trigger >> dag_end
