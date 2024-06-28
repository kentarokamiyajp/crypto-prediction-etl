import sys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

sys.path.append("/opt/airflow")
from common_functions import import_airflow_variables
from common_functions.notification import send_line_notification
from conf.dag_common import default_args

DAG_ID = "D_Import_airflow_variables"
TAGS = ["daily", "import"]

with DAG(
    DAG_ID,
    description="Import Airflow Variables from .env file",
    schedule_interval="30 15 * * 5",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=lambda context: send_line_notification(
        context=context, dag_id=DAG_ID, tags=TAGS, type="ERROR"
    ),
    concurrency=5,  # can run N tasks at the same time
    max_active_runs=1,  # can run N DAGs at the same time
    tags=TAGS,
    default_args=default_args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    import_airflow_variables = PythonOperator(
        task_id="import_airflow_variables", python_callable=import_airflow_variables.main
    )

    dag_end = DummyOperator(task_id="dag_end")

    (dag_start >> import_airflow_variables >> dag_end)
