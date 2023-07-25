import os
from os.path import join, dirname
from dotenv import load_dotenv

load_dotenv(verbose=True)

dotenv_path = join(dirname(__file__), ".env")
load_dotenv(dotenv_path)

LINE_NOTIFY_URL = os.environ.get("LINE_NOTIFY_URL")
LINE_ACCESS_TOKEN = os.environ.get("LINE_ACCESS_TOKEN")

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST")
CASSANDRA_PORT = os.environ.get("CASSANDRA_PORT")
CASSANDRA_USERNAME = os.environ.get("CASSANDRA_USERNAME")
CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD")

KAFKA_CONSUMER_HOME = os.environ.get("KAFKA_CONSUMER_HOME")
KAFKA_PRODUCER_HOME = os.environ.get("KAFKA_PRODUCER_HOME")
KAFKA_LOG_HOME = os.environ.get("KAFKA_LOG_HOME")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")

POLONIEX_HOME = os.environ.get("POLONIEX_HOME")

TRINO_HOST = os.environ.get("TRINO_HOST")
TRINO_PORT = os.environ.get("TRINO_PORT")
TRINO_USER = os.environ.get("TRINO_USER")
