import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

dag_id = "OT_Load_crypto_candles_day"
tags = ["OT_Load", "crypto"]


def _task_failure_alert(context):
    from airflow_modules import send_notification

    send_notification(dag_id, tags, "ERROR")


def _get_crypto_candle_day_past_data():
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

    target_days = 1000  # how many days ago you want to get.
    seconds_of_one_day = 60 * 60 * 24  # seconds of one day
    period = seconds_of_one_day * target_days
    to_time = time.time()  # to this time to get the past data
    from_time = to_time - period  # from this time to get the past data

    res = {}
    window_size = seconds_of_one_day * 30  # Get data of <window_size> days for each time.
    curr_from_time = from_time
    curr_to_time = curr_from_time + window_size
    while True:
        for asset in assets:
            try:
                logger.info(
                    "{}: Load from {} to {}".format(asset, curr_from_time, curr_to_time)
                )
                data = poloniex_operation.get_candle_data(
                    asset, interval, curr_from_time, curr_to_time
                )
                if data != None:
                    if asset in res:
                        res[asset].extend(data)
                    else:
                        res[asset] = data
            except Exception as error:
                logger.error("Error: {}".format(error))
                logger.warn(
                    "Probably, {} does not exists at the time because it's new currency".format(
                        asset
                    )
                )
            time.sleep(5)
        curr_from_time = curr_to_time
        curr_to_time = curr_from_time + window_size

        if curr_from_time > to_time:
            break

    return res


def _process_candle_data(ti):
    from airflow_modules import utils

    candle_data = ti.xcom_pull(task_ids="get_crypto_candle_day_past_data")
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


def _load_from_cassandra_to_hive(query_file):
    from airflow_modules import trino_operation

    with open(query_file, "r") as f:
        query = f.read()
    trino_operation.run(query)


args = {"owner": "airflow", "retries": 5, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id,
    description="One time operation to load the past data for crypto candles day",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    tags=tags,
    default_args=args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    get_crypto_candle_day_past_data = PythonOperator(
        task_id="get_crypto_candle_day_past_data",
        python_callable=_get_crypto_candle_day_past_data,
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

    from airflow_modules import airflow_env_variables

    query_dir = "{}/trino".format(airflow_env_variables.QUERY_SCRIPT)
    load_from_cassandra_to_hive = PythonOperator(
        task_id="load_from_cassandra_to_hive",
        python_callable=_load_from_cassandra_to_hive,
        op_kwargs={"query_file": f"{query_dir}/D_Load_crypto_candles_day_001.sql"},
    )

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> get_crypto_candle_day_past_data
        >> process_candle_data
        >> insert_data_to_cassandra
        >> load_from_cassandra_to_hive
        >> dag_end
    )
