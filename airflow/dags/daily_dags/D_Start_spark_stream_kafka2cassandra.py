import sys
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator

sys.path.append("/opt/airflow")
from conf.dag_common import default_args
from common_functions.notification import send_line_notification
from dwh_modules.common import env_variables

DAG_ID = "D_Start_spark_stream_kafka2cassandra"
TAGS = ["daily", "load", "pyspark", "streaming"]

# Change args
default_args["retries"] = 0

with DAG(
    DAG_ID,
    tags=TAGS,
    description="Start Spark Streaming to load from Kafka to Cassandra",
    schedule_interval="30 15 * * 5",
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

    spark_stream_home = "/home/batch/git/crypto_prediction_dwh/modules/spark_operations/streaming"

    base_command = f"export KAFKA_HOME=/home/{env_variables.BATCH_USER}/kafka && \
                    export HADOOP_HOME=/home/{env_variables.BATCH_USER}/hadoop-3.3.6 && \
                    export JAVA_HOME=/usr/local/openjdk-11 && \
                    export PATH=$PATH:$KAFKA_HOME/bin:$HADOOP_HOME/bin:$JAVA_HOME/bin && \
                    . /home/{env_variables.BATCH_USER}/pyvenv/bin/activate && \
                    cd {spark_stream_home}"

    start_candles_minute_consumer = SSHOperator(
        task_id="spark_stream_crypto_candles",
        ssh_hook=ssh_hook,
        command=f" {base_command} && sh main.sh candles ",
    )

    start_market_trade_minute_consumer = SSHOperator(
        task_id="spark_stream_crypto_market_trade",
        ssh_hook=ssh_hook,
        command=f" {base_command} && sh main.sh market_trade ",
    )

    start_order_book_minute_consumer = SSHOperator(
        task_id="spark_stream_crypto_order_book",
        ssh_hook=ssh_hook,
        command=f" {base_command} && sh main.sh order_book ",
    )

    dag_end = DummyOperator(
        task_id="dag_end",
        trigger_rule="all_done",
    )

    (
        dag_start
        >> [
            start_candles_minute_consumer,
            start_market_trade_minute_consumer,
            start_order_book_minute_consumer,
        ]
        >> dag_end
    )
