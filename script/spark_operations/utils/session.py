import os, sys
import traceback

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common import env_variables
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


SPARK_VERSION = env_variables.SPARK_VERSION
SPARK_MASTER_HOST = env_variables.SPARK_MASTER_HOST
SPARK_MASTER_PORT = env_variables.SPARK_MASTER_PORT
CASSANDRA_HOST = "192.168.10.4"
CASSANDRA_PORT = env_variables.CASSANDRA_PORT
CASSANDRA_USERNAME = env_variables.CASSANDRA_USERNAME
CASSANDRA_PASSWORD = env_variables.CASSANDRA_PASSWORD

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

class SparkStreamer:
    def __init__(self, app_name, configs):
        default_configs = {
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
        }

        # Overwrite default configs
        for k, v in configs.items():
            default_configs[k] = v

        configs = tuple([(k, v) for k, v in default_configs.items()])

        # Create Spark Config
        conf = SparkConf().setAppName(app_name).setAll(configs)

        # Create Spark session with the config
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")

        self.read_stream = None
        self.write_stream = None

    def __del__(self):
        try:
            self.spark.stop()
            print("Stopped Spark Session !!!")

            if self.write_stream:
                latest_offsets = self.write_stream.lastProgress["sources"][0]["startOffset"]
                print("Latest Offsets: {}".format(latest_offsets))

        except Exception as e:
            traceback.print_exc()
            print("Got an error, but destroyed Spark Session anyway !!!")
