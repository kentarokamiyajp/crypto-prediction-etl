import sys
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.append("/opt/airflow")
from conf.dag_common import default_args, default_task_sensor_args
from common_functions.notification import send_line_notification

DAG_ID = "D_Check_all_create_end"
TAGS = ["daily", "check"]

with DAG(
    DAG_ID,
    tags=TAGS,
    description="Check all creating dags completed",
    schedule_interval="0 17 * * 5",
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
    wait_for_D_Create_indicator_day = ExternalTaskSensor(
        task_id="wait_for_D_Create_indicator_day",
        external_dag_id="D_Create_indicator_day",
        external_task_id="dag_end",
        execution_delta=timedelta(minutes=45),
        **default_task_sensor_args,
    )

    dag_end = DummyOperator(task_id="dag_end")

    (dag_start >> wait_for_D_Create_indicator_day >> dag_end)
