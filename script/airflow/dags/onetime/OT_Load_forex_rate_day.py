from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


dag_id = "OT_Load_forex_rate_day"
tags = ["onetime", "load", "forex"]


def _task_failure_alert(context):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "ERROR")


def _get_forex_rate_past_data():
    from airflow_modules import yahoofinancials_operation, utils
    import time

    currencies = ["EURUSD=X", "GBPUSD=X", "JPY=X"]

    interval = "daily"

    target_days = 5000  # how many days ago you want to get.
    seconds_of_one_day = 60 * 60 * 24  # seconds of one day
    period = seconds_of_one_day * target_days
    to_ts = time.time()  # to this time to get the past data
    from_ts = to_ts - period  # from this time to get the past data

    res = None
    window_size = seconds_of_one_day * 100  # Get data of <window_size> days for each time.
    curr_from_ts = from_ts
    curr_to_ts = curr_from_ts + window_size
    while True:
        curr_from_date = utils.get_dt_from_unix_time(curr_from_ts)
        curr_to_date = utils.get_dt_from_unix_time(curr_to_ts)

        logger.info("Load from {} to {}".format(curr_from_date, curr_to_date))

        data = yahoofinancials_operation.get_data_from_yahoofinancials(
            currencies, interval, curr_from_date, curr_to_date
        )

        if res == None:
            res = data
        else:
            for symbol_name, data in data.items():
                res[symbol_name]["prices"].extend(data["prices"])

        curr_from_ts = curr_to_ts
        curr_to_ts = curr_from_ts + window_size

        if curr_from_ts > to_ts:
            break

        time.sleep(5)

    return res


def _process_forex_rate(ti):
    from airflow_modules import utils

    forex_rate = ti.xcom_pull(task_ids="get_forex_rate_past_data")
    return utils.process_yahoofinancials_data(forex_rate)


def _insert_data_to_cassandra(ti):
    from airflow_modules import cassandra_operation

    keyspace = "forex"
    table_name = "forex_rate_day"
    forex_rate = ti.xcom_pull(task_ids="process_forex_rate_for_ingestion")

    query = f"""
    INSERT INTO {table_name} (id,low,high,open,close,volume,adjclose,currency,unixtime_create,dt_create_utc,tz_gmtoffset,ts_insert_utc)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cassandra_operation.insert_data(keyspace, forex_rate, query)


def _load_from_cassandra_to_hive():
    from airflow_modules import trino_operation

    query = """
    DELETE FROM hive.forex_raw.forex_rate_day
    """
    logger.info("RUN QUERY")
    logger.info(query)
    trino_operation.run(query)

    query = """
    INSERT INTO
        hive.forex_raw.forex_rate_day (
            id,
            low,
            high,
            open,
            close,
            volume,
            adjclose,
            currency,
            unixtime_create,
            dt_create_utc,
            tz_gmtoffset,
            ts_insert_utc,
            year,
            month,
            day
        )
    SELECT
        id,
        low,
        high,
        open,
        close,
        volume,
        adjclose,
        currency,
        unixtime_create,
        dt_create_utc,
        tz_gmtoffset,
        ts_insert_utc,
        year (dt_create_utc),
        month (dt_create_utc),
        day (dt_create_utc)
    FROM
        cassandra.forex.forex_rate_day
    """
    logger.info("RUN QUERY")
    logger.info(query)
    trino_operation.run(query)


args = {"owner": "airflow", "retries": 3, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="One time operation to load the past data for forex rate day",
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

    # get_crude_oil_price_past_data = PythonOperator(
    #     task_id="get_crude_oil_price_past_data",
    #     python_callable=_get_crude_oil_price_past_data,
    #     do_xcom_push=True,
    # )

    # process_crude_oil_price = PythonOperator(
    #     task_id="process_crude_oil_price_for_ingestion",
    #     python_callable=_process_crude_oil_price,
    #     do_xcom_push=True,
    # )

    # insert_data_to_cassandra = PythonOperator(
    #     task_id="insert_crude_oil_price_data_to_cassandra",
    #     python_callable=_insert_data_to_cassandra,
    # )

    load_from_cassandra_to_hive = PythonOperator(
        task_id="load_from_cassandra_to_hive",
        python_callable=_load_from_cassandra_to_hive,
    )

    dag_end = DummyOperator(task_id="dag_end")

    # (
    #     dag_start
    #     >> get_crude_oil_price_past_data
    #     >> process_crude_oil_price
    #     >> insert_data_to_cassandra
    #     >> load_from_cassandra_to_hive
    #     >> dag_end
    # )

    (dag_start >> load_from_cassandra_to_hive >> dag_end)
