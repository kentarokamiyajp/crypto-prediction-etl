from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from common import env_variables

spark = (
    SparkSession.builder.appName("SparkCassandraApp")
    .config("spark.cassandra.connection.host", env_variables.CASSANDRA_HOST)
    .config("spark.cassandra.connection.port", env_variables.CASSANDRA_PORT)
    .config("spark.cassandra.auth.username", env_variables.CASSANDRA_USERNAME)
    .config("spark.cassandra.auth.password", env_variables.CASSANDRA_PASSWORD)
    .master("local")
    .getOrCreate()
)

df = (
    spark.read.format("org.apache.spark.sql.cassandra")
    .options(table="coins_list", keyspace="crypto")
    .load()
)

df.show()
