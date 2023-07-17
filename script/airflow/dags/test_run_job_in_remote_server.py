from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime

import logging

logger = logging.getLogger(__name__)

with DAG(
    "test_run_job_in_remote_server",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["test"],
) as dag:
    remote_task = SSHOperator(
        task_id="remote_task",
        ssh_conn_id="ssh_python_docker",
        command="python /home/kamiken/sample.py",
    )

    remote_task
