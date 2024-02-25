import sys
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


dag_id = "D_kill_kafka_consumer"
tags = ["kafka"]


def _task_failure_alert(context):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "ERROR")


args = {"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="Kill Kafka consumer processes",
    schedule_interval="0 8 * * 0",
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

    kill_candles_minute_consumer = SSHOperator(
        task_id="kill_candles_minute_consumer",
        ssh_hook=ssh_hook,
        command=""" ps axf | grep candles_minute_consumer | grep -v grep | awk '{print "kill -9 " $1}' | sh """,
    )

    kill_candles_realtime_consumer = SSHOperator(
        task_id="kill_candles_realtime_consumer",
        ssh_hook=ssh_hook,
        command=""" ps axf | grep candles_realtime_consumer | grep -v grep | awk '{print "kill -9 " $1}' | sh """,
    )

    kill_market_trade_consumer = SSHOperator(
        task_id="kill_market_trade_consumer",
        ssh_hook=ssh_hook,
        command=""" ps axf | grep market_trade_consumer | grep -v grep | awk '{print "kill -9 " $1}' | sh """,
    )

    kill_order_book_consumer = SSHOperator(
        task_id="kill_order_book_consumer",
        ssh_hook=ssh_hook,
        command=""" ps axf | grep order_book_consumer | grep -v grep | awk '{print "kill -9 " $1}' | sh """,
    )

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> [
            kill_candles_minute_consumer,
            kill_candles_realtime_consumer,
            kill_market_trade_consumer,
            kill_order_book_consumer,
        ]
        >> dag_end
    )
