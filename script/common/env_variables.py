import os
from os.path import join, dirname
from dotenv import load_dotenv

load_dotenv(verbose=True)

dotenv_path = join(dirname(__file__), ".env")
load_dotenv(dotenv_path, override=True)

LINE_NOTIFY_URL = os.environ.get("LINE_NOTIFY_URL")
LINE_ACCESS_TOKEN = os.environ.get("LINE_ACCESS_TOKEN")

MAC_HOST = os.environ.get("MAC_HOST")
MAC_USER = os.environ.get("MAC_USER")

UBUNTU_HOST = os.environ.get("UBUNTU_HOST")
UBUNTU_USER = os.environ.get("UBUNTU_USER")
UBUNTU_AIRFLOW_DOCKER_HOME = os.environ.get("UBUNTU_AIRFLOW_DOCKER_HOME")
UBUNTU_AIRFLOW_DAGS_HOME = os.environ.get("UBUNTU_AIRFLOW_DAGS_HOME")

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST")
CASSANDRA_PORT = os.environ.get("CASSANDRA_PORT")
CASSANDRA_USERNAME = os.environ.get("CASSANDRA_USERNAME")
CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD")

KAFKA_CONSUMER_HOME = os.environ.get("KAFKA_CONSUMER_HOME")
KAFKA_PRODUCER_HOME = os.environ.get("KAFKA_PRODUCER_HOME")
KAFKA_LOG_HOME = os.environ.get("KAFKA_LOG_HOME")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")

PYTHON_SERVER_HOST = os.environ.get("PYTHON_SERVER_HOST")
PYTHON_SERVER_SSH_PORT = os.environ.get("PYTHON_SERVER_SSH_PORT")
PYTHON_SERVER_USERNAME = os.environ.get("PYTHON_SERVER_USERNAME")
PYTHON_SERVER_PASSWORD = os.environ.get("PYTHON_SERVER_PASSWORD")

POLONIEX_HOME = os.environ.get("POLONIEX_HOME")

TRINO_HOST = os.environ.get("TRINO_HOST")
TRINO_PORT = os.environ.get("TRINO_PORT")
TRINO_USER = os.environ.get("TRINO_USER")

HDFS_HOST = os.environ.get("HDFS_HOST")
HDFS_PORT = os.environ.get("HDFS_PORT")

SPARK_VERSION = os.environ.get("SPARK_VERSION")
SPARK_MASTER_HOST = os.environ.get("SPARK_MASTER_HOST")
SPARK_MASTER_PORT = os.environ.get("SPARK_MASTER_PORT")
SPARK_VOLUME_HOME = os.environ.get("SPARK_VOLUME_HOME")
SPARK_STREAMING_HDFS_CHECKPOINT_DIR = os.environ.get("SPARK_STREAMING_HDFS_CHECKPOINT_DIR")
HIVE_METASTORE_HOST = os.environ.get("HIVE_METASTORE_HOST")
HIVE_METASTORE_PORT = os.environ.get("HIVE_METASTORE_PORT")
HISTORY_SERVER_HOST = os.environ.get("HISTORY_SERVER_HOST")
HISTORY_SERVER_POST = os.environ.get("HISTORY_SERVER_POST")
HISTORY_LOG_HOME = os.environ.get("HISTORY_LOG_HOME")

AIRFLOW_EXEC_USER = os.environ.get("AIRFLOW_EXEC_USER")
AIRFLOW_PRIVATE_KEY = os.environ.get("AIRFLOW_PRIVATE_KEY")

DBT_HOST = os.environ.get("DBT_HOST")
DBT_USER = os.environ.get("DBT_USER")
DBT_PROJECT_HOME = os.environ.get("DBT_PROJECT_HOME")

BATCH_HOST = os.environ.get("BATCH_HOST")
BATCH_HOST_SSH_PORT = os.environ.get("BATCH_HOST_SSH_PORT")
BATCH_USER = os.environ.get("BATCH_USER")
