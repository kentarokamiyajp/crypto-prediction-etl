import sys
from airflow import DAG, settings
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.models import DagRun
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


dag_id = "D_stop_airflow_container"
tags = ["PREP"]


def _task_failure_alert(context):
    from airflow_modules import send_notification

    send_notification(dag_id, tags, "ERROR")


def _batch_end_check(target_dag_id_list, _execution_date):
    for target_dag_id in target_dag_id_list:
        dag_run = DagRun.find(dag_id=target_dag_id, session=settings.Session())

        if dag_run == []:
            raise AirflowException("[ERROR] DAG ({}) not found !!!".format(target_dag_id))

        dag_latest_executed_date = dag_run[-1].queued_at
        dag_latest_status = dag_run[-1].state

        logger.info(
            "target_dag_id:{}, dag_latest_executed_date:{}, dag_latest_status:{})".format(
                target_dag_id, dag_latest_executed_date, dag_latest_status
            )
        )

        if (
            _execution_date in str(dag_latest_executed_date)
            and dag_latest_status == "success"
        ):
            pass
        else:
            raise AirflowException("[ERROR] DAG is not completed or failed !!!")


args = {"owner": "airflow", "retries": 5, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="Stop airflow docker containers",
    schedule_interval="0 2 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    tags=tags,
    default_args=args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    import pytz

    target_dag_id_list = ["D_Load_crypto_candles_minute", "D_Load_crude_oil_price_day"]
    _execution_date = datetime.now(tz=pytz.timezone("UTC")).strftime("%Y-%m-%d")
    batch_end_check = PythonOperator(
        task_id="batch_end_check",
        python_callable=_batch_end_check,
        op_args=[target_dag_id_list, _execution_date],
    )

    from airflow_modules import airflow_env_variables

    sys.path.append(airflow_env_variables.DWH_SCRIPT)
    from common import env_variables

    ssh_hook = SSHHook(
        remote_host=env_variables.UBUNTU_HOST,
        username=env_variables.UBUNTU_USER,
        key_file=env_variables.AIRFLOW_PRIVATE_KEY,
        port=22,
    )

    ssh_operation = SSHOperator(
        task_id="ssh_operation",
        ssh_hook=ssh_hook,
        command="cd {} && docker-compose stop && exit".format(
            env_variables.UBUNTU_AIRFLOW_DOCKER_HOME
        ),
    )

    dag_end = DummyOperator(task_id="dag_end")

    (dag_start >> batch_end_check >> ssh_operation >> dag_end)
