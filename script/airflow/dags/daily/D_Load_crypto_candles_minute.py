from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta, date
import traceback
import logging

logger = logging.getLogger(__name__)

dag_id = "D_Load_crypto_candles_minute"
tags = ["daily", "load", "crypto"]


def _task_failure_alert(context):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "ERROR")


def _send_warning_notification(optional_message=None):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "WARNING", optional_message)


def _get_candle_data():
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

    interval = "MINUTE_1"

    target_days = 2  # data of how many days ago you want to get.
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
    while True:
        for asset in assets:
            max_retry_cnt = 5
            curr_retry_cnt = 0
            while curr_retry_cnt <= max_retry_cnt:
                try:
                    logger.info(
                        "{}: Load from {} to {}".format(
                            asset, curr_from_time, curr_to_time
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
                    time.sleep(2)
                    break
                except Exception as error:
                    logger.error("Error: {}".format(error))
                    logger.error(traceback.format_exc())
                    curr_retry_cnt += 1
                    time.sleep(600)

                if curr_retry_cnt > max_retry_cnt:
                    raise AirflowFailException("Poloniex API is dead now.")

        curr_from_time = curr_to_time
        curr_to_time = curr_from_time + window_size

        if curr_from_time > to_time:
            break

    return res


def _process_candle_data(ti):
    from airflow_modules import utils

    candle_data = ti.xcom_pull(task_ids="get_candle_minite_for_1day")
    res = utils.process_candle_data_from_poloniex(candle_data)

    return res


def _insert_data_to_cassandra(ti):
    from airflow_modules import cassandra_operation

    keyspace = "crypto"
    table_name = "candles_minute"
    candle_data = ti.xcom_pull(task_ids="process_candle_data_for_ingestion")

    query = f"""
    INSERT INTO {table_name} (id,low,high,open,close,amount,quantity,buyTakerAmount,\
        buyTakerQuantity,tradeCount,ts,weightedAverage,interval,startTime,closeTime,dt,ts_insert_utc)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    logger.info("RUN QUERY")
    logger.info(query)

    cassandra_operation.insert_data(keyspace, candle_data, query)


def _check_latest_dt():
    from airflow_modules import cassandra_operation

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

    logger.info("RUN QUERY")
    logger.info(query)

    count = cassandra_operation.check_latest_dt(keyspace, query).one()[0]

    # If there is no data for prev-day, exit with error.
    if int(count) == 0:
        logger.error(
            "There is no data for prev-day ({}, asset:{})".format(prev_date, target_asset)
        )
        warning_message = "There is no data for prev_date ({}, asset:{})".format(
            prev_date, target_asset
        )
        logger.warn(warning_message)
        _send_warning_notification(warning_message)


def _delete_past_data_from_hive(query_file, delete_days):
    from airflow_modules import trino_operation

    with open(query_file, "r") as f:
        query_script = f.read()

    """
    To delete data from non-transactional table in hive,
    only partition columns must be specified in WHERE clause.
    However, to delete data in a certain period such as deleting from N days ago,
    need to repeat a delete query N times.
    """
    for N in range(0, delete_days + 1):
        query = query_script.replace("${N}", str(-N))
        logger.info("RUN QUERY")
        logger.info(query)
        trino_operation.run(query)


def _hive_deletion_check(query_file, delete_days):
    from airflow_modules import trino_operation

    with open(query_file, "r") as f:
        query = f.read()

    query = query.replace("${N}", str(-delete_days))
    logger.info("RUN QUERY")
    logger.info(query)
    res = trino_operation.run(query)

    select_count = res[0][0]
    if select_count != 0:
        error_msg = "Past data is not deleted !!!"
        logger.error(error_msg)
        logger.error("select_count:{}".format(select_count))
        raise AirflowFailException(error_msg)


def _load_from_cassandra_to_hive(query_file, delete_days):
    from airflow_modules import trino_operation

    with open(query_file, "r") as f:
        query = f.read()

    query = query.replace("${N}", str(-delete_days))
    logger.info("RUN QUERY")
    logger.info(query)
    trino_operation.run(query)


args = {"owner": "airflow", "retries": 3, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="Load candles minute data daily",
    schedule_interval="0 1 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    concurrency=1,  # can run N tasks at the same time
    max_active_runs=1,  # can run N DAGs at the same time
    tags=tags,
    default_args=args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    get_candle_data = PythonOperator(
        task_id="get_candle_minite_for_1day",
        python_callable=_get_candle_data,
        pool="poloniex_pool",
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

    from airflow_modules import airflow_env_variables

    query_dir = "{}/trino".format(airflow_env_variables.QUERY_SCRIPT_HOME)

    delete_days = 3

    delete_past_data_from_hive = PythonOperator(
        task_id="delete_past_data_from_hive",
        python_callable=_delete_past_data_from_hive,
        op_kwargs={
            "query_file": f"{query_dir}/D_Load_crypto_candles_minute_001.sql",
            "delete_days": delete_days,
        },
    )

    hive_deletion_check = PythonOperator(
        task_id="hive_deletion_check",
        python_callable=_hive_deletion_check,
        op_kwargs={
            "query_file": f"{query_dir}/D_Load_crypto_candles_minute_002.sql",
            "delete_days": delete_days,
        },
    )

    load_from_cassandra_to_hive = PythonOperator(
        task_id="load_from_cassandra_to_hive",
        python_callable=_load_from_cassandra_to_hive,
        op_kwargs={
            "query_file": f"{query_dir}/D_Load_crypto_candles_minute_003.sql",
            "delete_days": delete_days,
        },
    )

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> get_candle_data
        >> process_candle_data
        >> insert_data_to_cassandra
        >> check_latest_dt
        >> delete_past_data_from_hive
        >> hive_deletion_check
        >> load_from_cassandra_to_hive
        >> dag_end
    )
