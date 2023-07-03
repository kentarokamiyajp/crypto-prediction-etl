from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import logging

logger = logging.getLogger(__name__)

from modules import test


def create_file():
    res = test.create_file()
    logger.info(res)


def delete_file():
    res = test.delete_file()
    logger.info(res)


def check_hostname():
    res = test.check_hostname()
    logger.info(res)


def check_whoami():
    res = test.check_whoami()
    logger.info(res)


def check_pwd():
    res = test.check_pwd()
    logger.info(res)


with DAG("test", description="DAG test", schedule_interval=None, start_date=datetime(2023, 1, 1), catchup=False, tags=["test"]) as dag:
    create_file_task = PythonOperator(task_id="create_file", python_callable=create_file)

    delete_file_task = PythonOperator(task_id="delete_file", python_callable=delete_file)

    check_hostname_task = PythonOperator(task_id="check_hostname", python_callable=check_hostname)

    check_whoami_task = PythonOperator(task_id="check_whoami", python_callable=check_whoami)

    check_pwd_task = PythonOperator(task_id="check_pwd", python_callable=check_pwd)

    check_hostname_task >> check_whoami_task >> check_pwd_task >> create_file_task >> delete_file_task
