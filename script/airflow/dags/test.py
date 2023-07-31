import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
from airflow.operators.bash_operator import BashOperator
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
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["test"],
) as dag:
    
    t1 = BashOperator(
        task_id='whoami',
        bash_command='whoami',
        dag=dag,
    )
    
    t1