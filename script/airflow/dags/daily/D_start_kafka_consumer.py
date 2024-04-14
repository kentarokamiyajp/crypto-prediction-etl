import sys
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


dag_id = "D_start_kafka_consumer"
tags = ["daily", "load", "kafka", "consumer"]


def _task_failure_alert(context):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "ERROR")


args = {"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="Start Kafka consumer processes",
    schedule_interval="30 15 * * 5",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    tags=tags,
    default_args=args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    from airflow_modules import airflow_env_variables

    sys.path.append(airflow_env_variables.DWH_SCRIPT)
    from common import env_variables

    ssh_hook = SSHHook(
        remote_host=env_variables.PYTHON_SERVER_HOST,
        username=env_variables.PYTHON_SERVER_USERNAME,
        key_file=env_variables.AIRFLOW_PRIVATE_KEY,
        port=env_variables.PYTHON_SERVER_SSH_PORT,
    )
    
    consumer_home = "/home/pyuser/git/crypto_prediction_dwh/script/kafka_consumers"

    start_candles_minute_consumer = SSHOperator(
        task_id="start_candles_minute_consumer",
        ssh_hook=ssh_hook,
        command=f" cd {consumer_home} && sh main_candles_minute_consumer.sh ",
    )

    start_candles_realtime_consumer = SSHOperator(
        task_id="start_candles_realtime_consumer",
        ssh_hook=ssh_hook,
        command=f" cd {consumer_home} && sh main_candles_realtime_consumer.sh ",
    )

    start_market_trade_consumer = SSHOperator(
        task_id="start_market_trade_consumer",
        ssh_hook=ssh_hook,
        command=f" cd {consumer_home} && sh main_market_trade_consumer.sh ",
    )

    start_order_book_consumer = SSHOperator(
        task_id="start_order_book_consumer",
        ssh_hook=ssh_hook,
        command=f" cd {consumer_home} && sh main_order_book_consumer.sh ",
    )

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> [
            start_candles_minute_consumer,
            start_candles_realtime_consumer,
            start_market_trade_consumer,
            start_order_book_consumer,
        ]
        >> dag_end
    )
