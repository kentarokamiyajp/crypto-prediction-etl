import sys
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


dag_id = "D_start_airflow_container"
tags = ["PREP"]


def _task_failure_alert(context):
    from airflow_modules import send_notification

    send_notification(dag_id, tags, "ERROR")


args = {"owner": "airflow", "retries": 5, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="Start airflow docker containers",
    schedule_interval="0 0 * * *",
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
        remote_host=env_variables.UBUNTU_HOST,
        username=env_variables.UBUNTU_USER,
        key_file=env_variables.AIRFLOW_PRIVATE_KEY,
        port=22,
    )

    ssh_operation = SSHOperator(
        task_id="ssh_operation",
        ssh_hook=ssh_hook,
        command="cd {} && docker-compose start && if [ $? -eq 0 ]; then exit 0; else 1; fi".format(
            env_variables.UBUNTU_AIRFLOW_DOCKER_HOME
        ),
    )

    dag_end = DummyOperator(task_id="dag_end")

    (dag_start >> ssh_operation >> dag_end)
