import os, sys
import copy
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pprint import pprint

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common import env_variables
from common.utils import get_ts_now


SPARK_VERSION = env_variables.SPARK_VERSION
SPARK_MASTER_HOST = env_variables.SPARK_MASTER_HOST
SPARK_MASTER_PORT = env_variables.SPARK_MASTER_PORT
HIVE_METASTORE_HOST = env_variables.HIVE_METASTORE_HOST
HIVE_METASTORE_PORT = env_variables.HIVE_METASTORE_PORT
CASSANDRA_HOST = env_variables.CASSANDRA_HOST
CASSANDRA_PORT = env_variables.CASSANDRA_PORT
CASSANDRA_USERNAME = env_variables.CASSANDRA_USERNAME
CASSANDRA_PASSWORD = env_variables.CASSANDRA_PASSWORD

DEFAULT_CONFIGS = {
    "spark.master": f"spark://{SPARK_MASTER_HOST}:{SPARK_MASTER_PORT}",
    "spark.hadoop.hive.metastore.uris": f"thrift://{HIVE_METASTORE_HOST}:{HIVE_METASTORE_PORT}",
    "spark.sql.warehouse.dir": "/user/hive/warehouse",
    "spark.jars.packages": f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION},com.datastax.spark:spark-cassandra-connector_2.12:3.0.0",
    "spark.streaming.stopGracefullyOnShutdown": "true",
    "spark.debug.maxToStringFields": "100",
    "spark.cores.max": "1",
    "spark.executor.cores": "1",
    "spark.executor.memory": "1g",
    "spark.cassandra.connection.host": CASSANDRA_HOST,
    "spark.cassandra.connection.port": CASSANDRA_PORT,
    "spark.cassandra.auth.username": CASSANDRA_USERNAME,
    "spark.cassandra.auth.password": CASSANDRA_PASSWORD,
    "spark.ui.port": "4040",
}

"""
spark.cores.max: Max number of cores for a spark session, not max cores allocating to each worker.
spark.executor.cores: Number of cores allocating to each worker. The number can be less than number of cores each worker has.
spark.executor.memory: Memory size for each worker.

E.g., 2 cores * 5 workers

    case-1:
        .config("spark.cores.max", 3)
        .config("spark.executor.cores", "1")
        .config("spark.executor.memory", "1g")

        >> 3 executors are created and each executor has 1 core and 1g memory.
        
    case-2:
        .config("spark.cores.max", 3)
        .config("spark.executor.cores", "3")
        .config("spark.executor.memory", "1g")

        >> Failed. 
        >> Since each worker has 2 cores, we can only set even number and less 2.
        >> A executor is created in one spark worker.
    
"""


def _update_config(custom_config):
    tmp_new_config = copy.deepcopy(DEFAULT_CONFIGS)

    # Overwrite default configs
    for k, v in custom_config.items():
        tmp_new_config[k] = v

    new_config = tuple([(k, v) for k, v in tmp_new_config.items()])

    return new_config


def _create_session(app_name, custom_configs):
    # Create Spark Config
    configs = _update_config(custom_configs)
    spark_conf = SparkConf().setAppName(app_name).setAll(configs)

    # Create Spark session with the config
    spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()

    print("Successfully created Spark Session !!!")
    pprint(configs)

    spark.sparkContext.setLogLevel("ERROR")

    return spark


class BaseSparkSession:
    def __init__(self, custom_configs, base_spark_app_name):
        # Create Spark session with the config
        spark_app_name = "{} PySpark Hive Session for {}".format(
            get_ts_now("Asia/Tokyo"), base_spark_app_name
        )

        self.spark = _create_session(spark_app_name, custom_configs)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.spark.stop()
        print("Stopped Spark Session !!!")

    def run_sql(self, query):
        print(f"Run PySpark Query\n{query}")
        return self.spark.sql(query)


class SparkStreamer:
    def __init__(self, app_name, custom_configs):
        # Create Spark session with the config
        self.spark = _create_session(app_name, custom_configs)
        self.read_stream = None
        self.write_stream = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.spark.stop()
        print("Stopped Spark Session !!!")

        if self.write_stream:
            latest_offsets = self.write_stream.lastProgress["sources"][0]["startOffset"]
            print("Latest Offsets: {}".format(latest_offsets))
