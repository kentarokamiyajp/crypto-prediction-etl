from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

dag_id = "OT_Load_crypto_candles_minute"
tags = ["OT_Load", "crypto"]


def _task_failure_alert(context):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "ERROR")


def _get_crypto_candle_minute_past_data():
    import time
    from airflow_modules import poloniex_operation, utils, cassandra_operation

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

    interval = "MINUTE_1"

    target_days = 300  # how many days ago you want to get.
    seconds_of_one_day = 60 * 60 * 24  # seconds of one day
    period = seconds_of_one_day * target_days
    to_time = time.time()  # from this time to get the past data
    from_time = to_time - period  # to this time to get the past data

    # Each GET request, only 500 records we can get.
    # This means data for 500 minutes per one request.
    # 1 day = 1440 minutes
    res = {}
    window_size = 60 * 500  # Get data of <window_size> minutes for each time.
    curr_from_time = from_time
    curr_to_time = curr_from_time + window_size
    batch_size = 1000
    curr_size = 0
    while True:
        for asset in assets:
            curr_size += 1
            try:
                logger.info(
                    "{}: Load from {} to {} ({}/{})".format(
                        asset, curr_from_time, curr_to_time, curr_size, batch_size
                    )
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
                raise AirflowFailException("API error !!! {}".format(error))

            if curr_size == batch_size:
                # preprocess
                candle_data = utils.process_candle_data_from_poloniex(res)

                # insert into cassandra table
                keyspace = "crypto"
                table_name = "candles_minute"
                query = f"""
                INSERT INTO {table_name} (id,low,high,open,close,amount,quantity,buyTakerAmount,\
                    buyTakerQuantity,tradeCount,ts,weightedAverage,interval,startTime,closeTime,dt,ts_insert_utc)\
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """
                cassandra_operation.insert_data(keyspace, candle_data, query)

                # reset
                curr_size = 0
                res = {}

            time.sleep(2)

        curr_from_time = curr_to_time
        curr_to_time = curr_from_time + window_size

        if curr_from_time > to_time:
            break
    
    # Operation for the remaining data.
    # preprocess
    candle_data = utils.process_candle_data_from_poloniex(res)
    # insert into cassandra table
    keyspace = "crypto"
    table_name = "candles_minute"
    query = f"""
    INSERT INTO {table_name} (id,low,high,open,close,amount,quantity,buyTakerAmount,\
        buyTakerQuantity,tradeCount,ts,weightedAverage,interval,startTime,closeTime,dt,ts_insert_utc)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cassandra_operation.insert_data(keyspace, candle_data, query)


def _insert_from_cassandra_to_hive():
    from airflow_modules import trino_operation

    # Delete all from hive
    query = """
    DELETE FROM hive.crypto_raw.candles_minute
    """
    trino_operation.run(query)

    # insert into hive from cassandra
    query = """
    INSERT INTO
        hive.crypto_raw.candles_minute (
            id,
            low,
            high,
            open,
            close,
            amount,
            quantity,
            buyTakerAmount,
            buyTakerQuantity,
            tradeCount,
            ts,
            weightedAverage,
            interval_type,
            startTime,
            closeTime,
            dt,
            ts_insert_utc,
            year,
            month,
            day,
            hour
        )
    SELECT
        id,
        low,
        high,
        open,
        close,
        amount,
        quantity,
        buyTakerAmount,
        buyTakerQuantity,
        tradeCount,
        ts,
        weightedAverage,
        interval,
        startTime,
        closeTime,
        dt,
        ts_insert_utc,
        year (from_unixtime (closeTime)),
        month (from_unixtime (closeTime)),
        day (from_unixtime (closeTime)),
        hour (from_unixtime (closeTime))
    FROM
        cassandra.crypto.candles_minute
    """
    trino_operation.run(query)


args = {"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id,
    description="One time operation to load the past data for crypto candles minute",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    concurrency=1,  # can run N tasks at the same time
    max_active_runs=1,  # can run N DAGs at the same time
    tags=tags,
    default_args=args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    get_crypto_candle_minute_past_data = PythonOperator(
        task_id="get_crypto_candle_minute_past_data",
        python_callable=_get_crypto_candle_minute_past_data,
    )

    # insert_from_cassandra_to_hive = PythonOperator(
    #     task_id="insert_from_cassandra_to_hive",
    #     python_callable=_insert_from_cassandra_to_hive,
    # )

    dag_end = DummyOperator(task_id="dag_end")

    # (
    #     dag_start
    #     >> get_crypto_candle_minute_past_data
    #     >> insert_from_cassandra_to_hive
    #     >> dag_end
    # )


    (
        dag_start
        >> get_crypto_candle_minute_past_data
        >> dag_end
    )