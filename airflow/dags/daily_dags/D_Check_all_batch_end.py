import sys
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

sys.path.append("/opt/airflow")
from conf.dag_common import default_args, default_task_sensor_args
from common_functions.notification import send_line_notification

DAG_ID = "D_Check_all_batch_end"
TAGS = ["daily", "check"]

with DAG(
    DAG_ID,
    tags=TAGS,
    description="Check all creating dags completed",
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
    with TaskGroup(
        "wait_target_tasks", tooltip="Wait for the all create tasks finish"
    ) as wait_target_tasks:
        wait_for_D_Check_all_load_end = ExternalTaskSensor(
            task_id="wait_for_D_Check_all_load_end",
            external_dag_id="D_Check_all_load_end",
            external_task_id="dag_end",
            execution_delta=timedelta(minutes=0),
            **default_task_sensor_args,
        )

        wait_for_D_Check_all_create_end = ExternalTaskSensor(
            task_id="wait_for_D_Check_all_create_end",
            external_dag_id="D_Check_all_create_end",
            external_task_id="dag_end",
            execution_delta=timedelta(minutes=240),
            **default_task_sensor_args,
        )

        [wait_for_D_Check_all_load_end, wait_for_D_Check_all_create_end]

    task_complete_notification = PythonOperator(
        task_id="task_complete_notification",
        python_callable=send_line_notification,
        op_kwargs={
            "dag_id": DAG_ID,
            "tags": TAGS,
            "type": "INFO",
            "optional_message": "All Daily Create Dags Completed !!!",
        },
    )

    dag_end = DummyOperator(task_id="dag_end")

    (dag_start >> wait_target_tasks >> task_complete_notification >> dag_end)
