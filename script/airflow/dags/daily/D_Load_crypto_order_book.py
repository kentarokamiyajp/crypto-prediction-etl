from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

dag_id = "D_Load_crypto_order_book"
tags = ["daily", "load", "crypto"]


def _task_failure_alert(context):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "ERROR")


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

with DAG(
    dag_id,
    description="Load order book data collected by Kafka producer",
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

    from airflow_modules import airflow_env_variables

    query_dir = "{}/trino".format(airflow_env_variables.QUERY_SCRIPT_HOME)

    days_delete_from = 3

    delete_past_data_from_hive = PythonOperator(
        task_id="delete_past_data_from_hive",
        python_callable=_delete_past_data_from_hive,
        op_kwargs={
            "query_file": f"{query_dir}/D_Load_crypto_order_book_001.sql",
            "days_delete_from": days_delete_from,
        },
    )

    hive_deletion_check = PythonOperator(
        task_id="hive_deletion_check",
        python_callable=_hive_deletion_check,
        op_kwargs={
            "query_file": f"{query_dir}/D_Load_crypto_order_book_002.sql",
            "days_delete_from": days_delete_from,
        },
    )

    load_from_cassandra_to_hive = PythonOperator(
        task_id="load_from_cassandra_to_hive",
        python_callable=_load_from_cassandra_to_hive,
        op_kwargs={
            "query_file": f"{query_dir}/D_Load_crypto_order_book_003.sql",
            "days_delete_from": days_delete_from,
        },
    )

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> delete_past_data_from_hive
        >> hive_deletion_check
        >> load_from_cassandra_to_hive
        >> dag_end
    )
