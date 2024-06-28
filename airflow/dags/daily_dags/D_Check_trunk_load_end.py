import sys
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

sys.path.append("/opt/airflow")
from conf.dag_common import default_args, default_task_sensor_args
from common_functions.notification import send_line_notification

DAG_ID = "D_Check_trunk_load_end"
TAGS = ["daily", "check"]

with DAG(
    DAG_ID,
    tags=TAGS,
    description="Check trunk data loading completed",
    schedule_interval="10 16 * * 5",
    on_failure_callback=lambda context: send_line_notification(
        context=context, dag_id=DAG_ID, tags=TAGS, type="ERROR"
    ),
    catchup=False,
    concurrency=1,  # can run N tasks at the same time
    max_active_runs=1,  # can run N DAGs at the same time
    default_args=default_args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    ##############################################
    # Task Group to wait for previous tasks finish
    ##############################################
    with TaskGroup(
        "wait_target_tasks", tooltip="Wait for the all load tasks finish"
    ) as wait_target_tasks:
        wait_for_D_Load_crude_oil_price_day = ExternalTaskSensor(
            task_id="wait_for_D_Load_crude_oil_price_day",
            external_dag_id="D_Load_crude_oil_price_day",
            external_task_id="dag_end",
            execution_delta=timedelta(minutes=10),
            **default_task_sensor_args,
        )

        wait_for_D_Load_crypto_candles_day = ExternalTaskSensor(
            task_id="wait_for_D_Load_crypto_candles_day",
            external_dag_id="D_Load_crypto_candles_day",
            external_task_id="dag_end",
            execution_delta=timedelta(minutes=10),
            **default_task_sensor_args,
        )

        wait_for_D_Load_forex_rate_day = ExternalTaskSensor(
            task_id="wait_for_D_Load_forex_rate_day",
            external_dag_id="D_Load_forex_rate_day",
            external_task_id="dag_end",
            execution_delta=timedelta(minutes=10),
            **default_task_sensor_args,
        )

        wait_for_D_Load_gold_price_day = ExternalTaskSensor(
            task_id="wait_for_D_Load_gold_price_day",
            external_dag_id="D_Load_gold_price_day",
            external_task_id="dag_end",
            execution_delta=timedelta(minutes=10),
            **default_task_sensor_args,
        )

        wait_for_D_Load_natural_gas_price_day = ExternalTaskSensor(
            task_id="wait_for_D_Load_natural_gas_price_day",
            external_dag_id="D_Load_natural_gas_price_day",
            external_task_id="dag_end",
            execution_delta=timedelta(minutes=10),
            **default_task_sensor_args,
        )

        wait_for_D_Load_stock_index_value_day = ExternalTaskSensor(
            task_id="wait_for_D_Load_stock_index_value_day",
            external_dag_id="D_Load_stock_index_value_day",
            external_task_id="dag_end",
            execution_delta=timedelta(minutes=10),
            **default_task_sensor_args,
        )

        [
            wait_for_D_Load_crude_oil_price_day,
            wait_for_D_Load_crypto_candles_day,
            wait_for_D_Load_forex_rate_day,
            wait_for_D_Load_gold_price_day,
            wait_for_D_Load_natural_gas_price_day,
            wait_for_D_Load_stock_index_value_day,
        ]

    dag_end = DummyOperator(task_id="dag_end")

    (dag_start >> wait_target_tasks >> dag_end)
