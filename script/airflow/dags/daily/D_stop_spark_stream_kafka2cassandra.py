import sys
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


dag_id = "D_stop_spark_stream_kafka2cassandra"
tags = ["daily", "load", "pyspark", "streaming"]


def _task_failure_alert(context):
    from airflow_modules.utils import send_notification

    send_notification(dag_id, tags, "ERROR")


args = {"owner": "airflow", "retries": 0, "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id,
    description="Stop Spark Streaming to load from Kafka to Cassandra",
    schedule_interval="0 3 * * 0",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    on_failure_callback=_task_failure_alert,
    tags=tags,
    default_args=args,
) as dag:
    dag_start = DummyOperator(task_id="dag_start")

    from airflow_modules import airflow_env_variables

    sys.path.append(airflow_env_variables.DWH_SCRIPT)
    from common import env_variables

    ssh_hook = SSHHook(
        remote_host=env_variables.BATCH_HOST,
        username=env_variables.BATCH_USER,
        key_file=env_variables.AIRFLOW_PRIVATE_KEY,
        port=env_variables.BATCH_HOST_SSH_PORT,
    )

    base_command = f"export KAFKA_HOME=/home/{env_variables.BATCH_USER}/kafka && \
                    export HADOOP_HOME=/home/{env_variables.BATCH_USER}/hadoop-3.3.6 && \
                    export JAVA_HOME=/usr/local/openjdk-11 && \
                    export PATH=$PATH:$KAFKA_HOME/bin:$HADOOP_HOME/bin:$JAVA_HOME/bin && \
                    . /home/{env_variables.BATCH_USER}/pyvenv/bin/activate"

    spark_stream_home = "/home/batch/git/crypto_prediction_dwh/script/spark_operations/streaming"

    stop_candles_minute_consumer = SSHOperator(
        task_id="spark_stream_crypto_candles",
        ssh_hook=ssh_hook,
        command=""" ps axf | grep candles | grep -v grep | awk '{print "kill -9 " $1}' | sudo sh """,
    )

    stop_market_trade_minute_consumer = SSHOperator(
        task_id="spark_stream_crypto_market_trade",
        ssh_hook=ssh_hook,
        command=""" ps axf | grep market_trade | grep -v grep | awk '{print "kill -9 " $1}' | sudo sh """,
    )

    stop_order_book_minute_consumer = SSHOperator(
        task_id="spark_stream_crypto_order_book",
        ssh_hook=ssh_hook,
        command=""" ps axf | grep order_book | grep -v grep | awk '{print "kill -9 " $1}' | sudo sh """,
    )

    dag_end = DummyOperator(task_id="dag_end")

    (
        dag_start
        >> [
            stop_candles_minute_consumer,
            stop_market_trade_minute_consumer,
            stop_order_book_minute_consumer,
        ]
        >> dag_end
    )
