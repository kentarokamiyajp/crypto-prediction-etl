import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, date
import time
import logging

logger = logging.getLogger(__name__)


def sleep():
    print("start_sleep")
    time.sleep(120)
    print("end_sleep")


# max_active_runs: max number of dags can run at the same time
# concurrency: max number of tasks can run at the same time
with DAG(
    "test",
    description="DAG test",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["test"],
) as dag:
    with TaskGroup(group_id="group1") as tg1:
        sleep_1 = PythonOperator(task_id="sleep_1", python_callable=sleep)
        sleep_2 = PythonOperator(task_id="sleep_2", python_callable=sleep)
        sleep_3 = PythonOperator(task_id="sleep_3", python_callable=sleep)

        sleep_1 >> sleep_2 >> sleep_3

        sleep_4 = PythonOperator(task_id="sleep_4", python_callable=sleep)
        sleep_5 = PythonOperator(task_id="sleep_5", python_callable=sleep)
        sleep_6 = PythonOperator(task_id="sleep_6", python_callable=sleep)

        sleep_4 >> sleep_5 >> sleep_6

        sleep_7 = PythonOperator(task_id="sleep_7", python_callable=sleep)
        sleep_8 = PythonOperator(task_id="sleep_8", python_callable=sleep)
        sleep_9 = PythonOperator(task_id="sleep_9", python_callable=sleep)

        sleep_7 >> sleep_8 >> sleep_9

    dag_end = DummyOperator(task_id="dag_end")

    tg1 >> dag_end
