import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from common import env_variables

"""
Spark Streaming: Kafka Topic -> Cassandra
"""

spark = (
    SparkSession.builder.appName("SparkCassandraApp")
    .config("spark.master", "spark://192.168.10.14:7077")
    .config("spark.hadoop.hive.metastore.uris", "thrift://192.168.10.14:9083")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .config("spark.cassandra.connection.host", env_variables.CASSANDRA_HOST)
    .config("spark.cassandra.connection.port", env_variables.CASSANDRA_PORT)
    .config("spark.cassandra.auth.username", env_variables.CASSANDRA_USERNAME)
    .config("spark.cassandra.auth.password", env_variables.CASSANDRA_PASSWORD)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = (
    spark.read.format("org.apache.spark.sql.cassandra")
    .options(table="candles_day", keyspace="crypto")
    .load()
)

df.show()
