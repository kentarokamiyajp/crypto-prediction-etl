import sys
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


dag_id = "OT_start_kafka_consumer_crypto_candles_minute"
tags = ["OT_start"]


def _task_failure_alert(context):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "ERROR")


args = {"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="Start kafka consumer for crypto candles minute",
    schedule_interval=None,
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
        port=env_variables.PYTHON_SERVER_SSH_PORT,
        key_file=env_variables.AIRFLOW_PRIVATE_KEY,
    )

    start_kafka_consumer_crypto_candles_minute = SSHOperator(
        task_id="start_kafka_consumer_crypto_candles_minute",
        ssh_hook=ssh_hook,
        command=" /home/pyuser/.pyenv/shims/python {}/main_candles_minute.py ".format(
            env_variables.KAFKA_CONSUMER_HOME
        ),
    )

    dag_end = DummyOperator(task_id="dag_end")

    (dag_start >> start_kafka_consumer_crypto_candles_minute >> dag_end)
