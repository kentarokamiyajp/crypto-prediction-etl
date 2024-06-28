import sys
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator

sys.path.append("/opt/airflow")
from conf.dag_common import default_args
from common_functions.notification import send_line_notification
from dwh_modules.common import env_variables


DAG_ID = "D_Stop_spark_stream_kafka2cassandra"
TAGS = ["daily", "load", "pyspark", "streaming"]

with DAG(
    DAG_ID,
    tags=TAGS,
    description="Stop Spark Streaming to load from Kafka to Cassandra",
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

    spark_stream_home = "/home/batch/git/crypto_prediction_dwh/modules/spark_operations/streaming"

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
