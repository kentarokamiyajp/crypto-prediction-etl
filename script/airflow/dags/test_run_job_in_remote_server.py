from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

import logging

logger = logging.getLogger(__name__)

with DAG(
    "test_run_job_in_remote_server",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
) as dag:
    insert_diff_in_hive = SSHOperator(
        task_id="insert_diff_in_hive",
        ssh_conn_id="ssh_python_docker",
        command="ls -al",
    )

    tdch_to_landing = SSHOperator(
        task_id="tdch_to_landing",
        ssh_conn_id="ssh_python_docker",
        command="ls -al",
    )

    update_meta_in_hive = SSHOperator(
        task_id="update_meta_in_hive",
        ssh_conn_id="ssh_python_docker",
        command="ls -al",
    )

    daily_end_check = SSHOperator(
        task_id="daily_end_check",
        ssh_conn_id="ssh_python_docker",
        command="exit 1",
    )

    copy_from_landing_to_raw = SSHOperator(
        task_id="copy_from_landing_to_raw",
        ssh_conn_id="ssh_python_docker",
        command="hostname",
        trigger_rule="one_success",
    )

    dag_end = DummyOperator(task_id="dag_end", trigger_rule="all_done")

    (
        insert_diff_in_hive
        >> tdch_to_landing
        >> update_meta_in_hive
        >> daily_end_check
        >> copy_from_landing_to_raw
        >> dag_end
    )
