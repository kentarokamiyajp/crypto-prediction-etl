import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


dag_id = "OT_Load_crude_oil_price_day"
tags = ["OT_Load", "oil"]


def _task_failure_alert(context):
    from airflow_modules import send_notification

    send_notification(dag_id, tags, "ERROR")


def _get_crude_oil_price_past_data():
    from airflow_modules import yahoofinancials_operation, utils
    import time

    symbols = ["CL=F"]

    interval = "daily"

    target_days = 5000  # how many days ago you want to get.
    seconds_of_one_day = 60 * 60 * 24  # seconds of one day
    period = seconds_of_one_day * target_days
    to_ts = time.time()  # to this time to get the past data
    from_ts = to_ts - period  # from this time to get the past data

    res = None
    window_size = (
        seconds_of_one_day * 100
    )  # Get data of <window_size> days for each time.
    curr_from_ts = from_ts
    curr_to_ts = curr_from_ts + window_size
    while True:
        curr_from_date = utils.get_dt_from_unix_time(curr_from_ts)
        curr_to_date = utils.get_dt_from_unix_time(curr_to_ts)

        logger.info("Load from {} to {}".format(curr_from_date, curr_to_date))

        data = yahoofinancials_operation.get_data_from_yahoofinancials(
            symbols, interval, curr_from_date, curr_to_date
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


def _process_crude_oil_price(ti):
    from airflow_modules import utils

    crude_oil_price = ti.xcom_pull(task_ids="get_crude_oil_price_past_data")
    return utils.process_yahoofinancials_data(crude_oil_price)


def _insert_data_to_cassandra(ti):
    from airflow_modules import cassandra_operation

    keyspace = "oil"
    table_name = "crude_oil_price_day"
    crude_oil_price = ti.xcom_pull(task_ids="process_crude_oil_price_for_ingestion")

    query = f"""
    INSERT INTO {table_name} (id,low,high,open,close,volume,adjclose,currency,dt_unix,dt,tz_gmtoffset,ts_insert_utc)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cassandra_operation.insert_data(keyspace, crude_oil_price, query)


def _load_from_cassandra_to_hive(query_file):
    from airflow_modules import trino_operation

    with open(query_file, "r") as f:
        query = f.read()
    trino_operation.run(query)


args = {"owner": "airflow", "retries": 5, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="One time operation to load the past data for Crude Oil price day",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    tags=tags,
    default_args=args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    get_crude_oil_price_past_data = PythonOperator(
        task_id="get_crude_oil_price_past_data",
        python_callable=_get_crude_oil_price_past_data,
        do_xcom_push=True,
    )

    process_crude_oil_price = PythonOperator(
        task_id="process_crude_oil_price_for_ingestion",
        python_callable=_process_crude_oil_price,
        do_xcom_push=True,
    )

    insert_data_to_cassandra = PythonOperator(
        task_id="insert_crude_oil_price_data_to_cassandra",
        python_callable=_insert_data_to_cassandra,
    )

    from airflow_modules import airflow_env_variables

    query_dir = "{}/trino".format(airflow_env_variables.QUERY_SCRIPT)
    load_from_cassandra_to_hive = PythonOperator(
        task_id="load_from_cassandra_to_hive",
        python_callable=_load_from_cassandra_to_hive,
        op_kwargs={"query_file": f"{query_dir}/D_Load_crude_oil_price_day_001.sql"},
    )

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> get_crude_oil_price_past_data
        >> process_crude_oil_price
        >> insert_data_to_cassandra
        >> load_from_cassandra_to_hive
        >> dag_end
    )
