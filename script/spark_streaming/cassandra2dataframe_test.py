from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession

spark = (
    SparkSession.builder.appName("SparkCassandraApp")
    .config("spark.cassandra.connection.host", "192.168.240.3")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.cassandra.auth.username", "kamiken")
    .config("spark.cassandra.auth.password", "kamiken")
    .master("local")
    .getOrCreate()
)

df = (
    spark.read.format("org.apache.spark.sql.cassandra")
    .options(table="coins_list", keyspace="crypto")
    .load()
)

df.show()
