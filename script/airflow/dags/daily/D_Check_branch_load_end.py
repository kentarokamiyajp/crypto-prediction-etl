import sys
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

dag_id = "D_Check_branch_load_end"
tags = ["daily", "check"]


def _task_failure_alert(context):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "ERROR")


args = {"owner": "airflow", "retries": 3, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="Check branch data loading completed",
    schedule_interval="0 21 * * 5",
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
    _execution_delta = timedelta(minutes=180)
    _mode = "reschedule"
    _timeout = 3600

    with TaskGroup(
        "wait_target_tasks", tooltip="Wait for the all load tasks finish"
    ) as wait_target_tasks:
        wait_for_D_Load_crypto_candles_minute = ExternalTaskSensor(
            task_id="wait_for_D_Load_crypto_candles_minute",
            external_dag_id="D_Load_crypto_candles_minute",
            external_task_id="dag_end",
            allowed_states=_allowed_states,
            failed_states=_failed_states,
            check_existence=_check_existence,
            poke_interval=_poke_interval,
            execution_delta=timedelta(minutes=300),
            mode=_mode,
            timeout=_timeout,
        )

        wait_for_D_Load_crypto_market_trade = ExternalTaskSensor(
            task_id="wait_for_D_Load_crypto_market_trade",
            external_dag_id="D_Load_crypto_market_trade",
            external_task_id="dag_end",
            allowed_states=_allowed_states,
            failed_states=_failed_states,
            check_existence=_check_existence,
            poke_interval=_poke_interval,
            execution_delta=_execution_delta,
            mode=_mode,
            timeout=_timeout,
        )

        wait_for_D_Load_crypto_order_book = ExternalTaskSensor(
            task_id="wait_for_D_Load_crypto_order_book",
            external_dag_id="D_Load_crypto_order_book",
            external_task_id="dag_end",
            allowed_states=_allowed_states,
            failed_states=_failed_states,
            check_existence=_check_existence,
            poke_interval=_poke_interval,
            execution_delta=_execution_delta,
            mode=_mode,
            timeout=_timeout,
        )

        wait_for_D_Load_crypto_candles_realtime = ExternalTaskSensor(
            task_id="wait_for_D_Load_crypto_candles_realtime",
            external_dag_id="D_Load_crypto_candles_realtime",
            external_task_id="dag_end",
            allowed_states=_allowed_states,
            failed_states=_failed_states,
            check_existence=_check_existence,
            poke_interval=_poke_interval,
            execution_delta=_execution_delta,
            mode=_mode,
            timeout=_timeout,
        )

        [
            wait_for_D_Load_crypto_candles_minute,
            wait_for_D_Load_crypto_market_trade,
            wait_for_D_Load_crypto_order_book,
            wait_for_D_Load_crypto_candles_realtime,
        ]

    dag_end = DummyOperator(task_id="dag_end")

    (dag_start >> wait_target_tasks >> dag_end)
