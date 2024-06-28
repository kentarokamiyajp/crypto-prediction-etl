import sys
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.append("/opt/airflow")
from conf.dag_common import default_args, default_task_sensor_args
from common_functions.notification import send_line_notification

DAG_ID = "D_Check_all_load_end"
TAGS = ["daily", "check"]

with DAG(
    DAG_ID,
    tags=TAGS,
    description="Check all loading dags completed",
    schedule_interval="0 21 * * 5",
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
    wait_trunk_load = ExternalTaskSensor(
        task_id="wait_trunk_load",
        external_dag_id="D_Check_trunk_load_end",
        external_task_id="dag_end",
        execution_delta=timedelta(minutes=60 * 4 + 50),
        **default_task_sensor_args,
    )

    wait_branch_load = ExternalTaskSensor(
        task_id="wait_branch_load",
        external_dag_id="D_Check_branch_load_end",
        external_task_id="dag_end",
        execution_delta=timedelta(minutes=0),
        **default_task_sensor_args,
    )

    dag_end = DummyOperator(task_id="dag_end")

    (dag_start >> [wait_trunk_load, wait_branch_load] >> dag_end)
