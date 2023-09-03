from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta, date
import logging

logger = logging.getLogger(__name__)

dag_id = "D_Load_stock_index_value_day"
tags = ["daily", "load", "stock_index"]


def _task_failure_alert(context):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "ERROR")


def _send_warning_notification(optional_message=None):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "WARNING", optional_message)


def _get_stock_index_value(load_from_days):
    from airflow_modules import yahoofinancials_operation, utils
    import time

    tickers = [
        "^NDX",  # NASDAQ 100
        "^DJI",  # Dow Jones Industrial Average
        "^DJT",  # Dow Jones Transportation Averag
        "^DJU",  # Dow Jones Utility Average
        "^BANK",  # NASDAQ Bank
        "^IXCO",  # NASDAQ Computer
        "^NBI",  # NASDAQ Biotechnology
        "^NDXT",  # NASDAQ 100 Technology Sector
        "^INDS",  # NASDAQ Industrial
        "^INSR",  # NASDAQ Insurance
        "^OFIN",  # NASDAQ Other Finance
        "^IXTC",  # NASDAQ Telecommunications
        "^TRAN",  # NASDAQ Transportation
        "^NYY",  # NYSE TMT INDEX
        "^NYI",  # NYSE INTL 100 INDEX
        "^NY",  # NYSE U.S. 100 Index
        "^NYL",  # NYSE WORLD LEADERS INDEX
        "^XMI",  # NYSE ARCA MAJOR MARKET INDEX
        "^OEX",  # S&P 100 INDEX
        "^GSPC",  # S&P 500
        "^HSI",  # HANG SENG INDEX
        "^FCHI",  # CAC 40
        "^BVSP",  # IBOVESPA
        "^N225",  # Nikkei 225
        "^RUA",  # Russell 3000
        "^XAX",  # NYSE AMEX COMPOSITE INDEX
    ]

    interval = "daily"

    # how many days ago you want to get.
    target_days = load_from_days

    # seconds of one day
    seconds_of_one_day = 60 * 60 * 24
    period = seconds_of_one_day * target_days

    # to this time to get the past data
    to_ts = time.time()

    # from this time to get the past data
    from_ts = to_ts - period

    from_date = utils.get_dt_from_unix_time(from_ts)
    to_date = utils.get_dt_from_unix_time(to_ts)

    logger.info("Load from {} to {}".format(from_date, to_date))

    return yahoofinancials_operation.get_data_from_yahoofinancials(
        tickers, interval, from_date, to_date
    )


def _process_stock_index_value(ti):
    from airflow_modules import utils

    stock_index_value = ti.xcom_pull(task_ids="get_stock_index_value")
    return utils.process_yahoofinancials_data(stock_index_value)


def _insert_data_to_cassandra(ti):
    from airflow_modules import cassandra_operation

    keyspace = "stock"
    table_name = "stock_index_day"
    stock_index_value = ti.xcom_pull(task_ids="process_stock_index_value_for_ingestion")

    query = f"""
    INSERT INTO {table_name} (id,low,high,open,close,volume,adjclose,currency,dt_unix,dt,tz_gmtoffset,ts_insert_utc)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    logger.info("RUN QUERY")
    logger.info(query)

    cassandra_operation.insert_data(keyspace, stock_index_value, query)


def _check_latest_dt():
    import pytz
    from airflow_modules import cassandra_operation, utils

    # check if the expected data is inserted.
    keyspace = "stock"
    table_name = "stock_index_day"
    target_index = "^NDX"
    tz = pytz.timezone("EST")  # Time zone for NYSE
    prev_date_ts = datetime.now(tz) - timedelta(days=1)
    prev_date = date(prev_date_ts.year, prev_date_ts.month, prev_date_ts.day).strftime(
        "%Y-%m-%d"
    )

    query = f"""
    select count(*) from {table_name} where dt = '{prev_date}' and id = '{target_index}'
    """
    count = cassandra_operation.check_latest_dt(keyspace, query).one()[0]

    logger.info("RUN QUERY")
    logger.info(query)

    # If there is no data for prev-day even on the market holiday, exit with error.
    market = "NYSE"
    if int(count) == 0 and utils.is_makert_open(prev_date, market):
        warning_message = "There is no data for prev_date ({}, asset:{})".format(
            prev_date, target_index
        )
        logger.warn(warning_message)
        _send_warning_notification(warning_message)


def _delete_past_data_from_hive(query_file, days_delete_from):
    from airflow_modules import trino_operation

    with open(query_file, "r") as f:
        query_script = f.read()

    """
    To delete data from non-transactional table in hive,
    only partition columns must be specified in WHERE clause.
    However, to delete data in a certain period such as deleting from N days ago,
    need to repeat a delete query N times.
    """
    for N in range(0, days_delete_from + 1):
        query = query_script.replace("${N}", str(-N))
        logger.info("RUN QUERY")
        logger.info(query)
        trino_operation.run(query)


def _hive_deletion_check(query_file, days_delete_from):
    from airflow_modules import trino_operation

    with open(query_file, "r") as f:
        query = f.read()

    query = query.replace("${N}", str(-days_delete_from))
    logger.info("RUN QUERY")
    logger.info(query)
    res = trino_operation.run(query)

    select_count = res[0][0]
    if select_count != 0:
        error_msg = "Past data is not deleted !!!"
        logger.error(error_msg)
        logger.error("select_count:{}".format(select_count))
        raise AirflowFailException(error_msg)


def _load_from_cassandra_to_hive(query_file, days_delete_from):
    from airflow_modules import trino_operation

    with open(query_file, "r") as f:
        query = f.read()

    query = query.replace("${N}", str(-days_delete_from))
    logger.info("RUN QUERY")
    logger.info(query)
    trino_operation.run(query)


args = {"owner": "airflow", "retries": 3, "retry_delay": timedelta(minutes=10)}
load_from_days = 7

with DAG(
    dag_id,
    description="Load nasdaq-100 data",
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

    get_stock_index_value = PythonOperator(
        task_id="get_stock_index_value",
        python_callable=_get_stock_index_value,
        op_kwargs={"load_from_days": load_from_days},
        do_xcom_push=True,
    )

    process_stock_index_value = PythonOperator(
        task_id="process_stock_index_value_for_ingestion",
        python_callable=_process_stock_index_value,
        pool="yfinance_pool",
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

    delete_past_data_from_hive = PythonOperator(
        task_id="delete_past_data_from_hive",
        python_callable=_delete_past_data_from_hive,
        op_kwargs={
            "query_file": f"{query_dir}/D_Load_stock_index_value_day_001.sql",
            "days_delete_from": load_from_days,
        },
    )

    hive_deletion_check = PythonOperator(
        task_id="hive_deletion_check",
        python_callable=_hive_deletion_check,
        op_kwargs={
            "query_file": f"{query_dir}/D_Load_stock_index_value_day_002.sql",
            "days_delete_from": load_from_days,
        },
    )

    load_from_cassandra_to_hive = PythonOperator(
        task_id="load_from_cassandra_to_hive",
        python_callable=_load_from_cassandra_to_hive,
        op_kwargs={
            "query_file": f"{query_dir}/D_Load_stock_index_value_day_003.sql",
            "days_delete_from": load_from_days,
        },
    )

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> get_stock_index_value
        >> process_stock_index_value
        >> insert_data_to_cassandra
        >> check_latest_dt
        >> delete_past_data_from_hive
        >> hive_deletion_check
        >> load_from_cassandra_to_hive
        >> dag_end
    )
