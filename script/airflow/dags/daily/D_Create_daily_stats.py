import sys
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


dag_id = "D_Create_daily_stats"
tags = ["daily", "create", "dbt"]


def _task_failure_alert(context):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "ERROR")


args = {"owner": "airflow", "retries": 3, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="Create mart tables for daily stats",
    schedule_interval="15 1 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    tags=tags,
    default_args=args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    ##############################################
    # Task Group to wait for previous tasks finish
    ##############################################
    _allowed_states = ["success"]
    _failed_states = ["failed", "skipped"]
    _check_existence = True
    _poke_interval = 10
    _execution_delta = timedelta(minutes=5)
    _mode = "reschedule"
    _timeout = 7200

    wait_prev_tasks = ExternalTaskSensor(
        task_id="wait_prev_tasks",
        external_dag_id="D_Check_trunk_load_end",
        external_task_id="dag_end",
        allowed_states=_allowed_states,
        failed_states=_failed_states,
        check_existence=_check_existence,
        poke_interval=_poke_interval,
        execution_delta=_execution_delta,
        mode=_mode,
        timeout=_timeout,
    )

    from airflow_modules import airflow_env_variables

    sys.path.append(airflow_env_variables.DWH_SCRIPT)
    from common import env_variables

    ssh_hook = SSHHook(
        remote_host=env_variables.DBT_HOST,
        username=env_variables.DBT_USER,
        key_file=env_variables.AIRFLOW_PRIVATE_KEY,
        port=22,
    )

    ssh_operation = SSHOperator(
        task_id="ssh_operation",
        ssh_hook=ssh_hook,
        command=" cd {}; sh dbt_run.sh; if [ $? -eq 0 ]; then exit 0; else exit 1; fi ".format(
            env_variables.DBT_PROJECT_HOME
        ),
    )

    dag_end = DummyOperator(task_id="dag_end")

    (dag_start >> wait_prev_tasks >> ssh_operation >> dag_end)
