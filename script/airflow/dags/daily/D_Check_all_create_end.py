import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

dag_id = "D_Check_all_create_end"
tags = ["daily", "check"]


def _task_failure_alert(context):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "ERROR")


args = {"owner": "airflow", "retries": 3, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="Check all creating dags completed",
    schedule_interval="10 1 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    concurrency=5,  # can run N tasks at the same time
    max_active_runs=1,  # can run N DAGs at the same time
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
    _timeout = 1800

    with TaskGroup(
        "wait_target_tasks", tooltip="Wait for the all create tasks finish"
    ) as wait_target_tasks:
        wait_for_D_Create_indicator_day = ExternalTaskSensor(
            task_id="wait_for_D_Create_indicator_day",
            external_dag_id="D_Create_indicator_day",
            external_task_id="dag_end",
            allowed_states=_allowed_states,
            failed_states=_failed_states,
            check_existence=_check_existence,
            poke_interval=_poke_interval,
            execution_delta=_execution_delta,
            mode=_mode,
            timeout=_timeout,
        )

        [wait_for_D_Create_indicator_day]

    dag_end = DummyOperator(task_id="dag_end")

    (dag_start >> wait_target_tasks >> dag_end)
