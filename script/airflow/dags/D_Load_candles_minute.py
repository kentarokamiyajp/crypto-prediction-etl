from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import logging
logger = logging.getLogger(__name__)

from modules import test

def create_file():
    res = test.create_file()
    logger.info(res)

with DAG(
    'D_Load_candles_minute',
    description='Load candles minute data daily',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:
    
    create_file_task = PythonOperator(
        task_id='create_file',
        python_callable=create_file
    )
    
    create_file_task