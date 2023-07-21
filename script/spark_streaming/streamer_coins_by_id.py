from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import DataStreamWriter
from pprint import pprint
from datetime import datetime
from modules import env_variables

"""
Spark Streaming: Kafka Topic -> Cassandra
"""
MAX_MEMORY = "1g"

spark = (
    SparkSession.builder.appName("SparkCassandraApp")
    .config("spark.cassandra.connection.host", env_variables.CASSANDRA_HOST)
    .config("spark.cassandra.connection.port", env_variables.CASSANDRA_PORT)
    .config("spark.cassandra.auth.username", env_variables.CASSANDRA_USERNAME)
    .config("spark.cassandra.auth.password", env_variables.CASSANDRA_PASSWORD)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0",
    )
    .config("spark.executor.memory", MAX_MEMORY)
    .config("spark.driver.memory", MAX_MEMORY)
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

KAFKA_BOOTSTRAP_SERVER = env_variables.KAFKA_BOOTSTRAP_SERVERS

# Load messages from Kafka Topic
KAFKA_TOPIC_coins_by_id = "crypto.coins_by_id"

readDF_coins_by_id = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", KAFKA_TOPIC_coins_by_id)
    .option("startingOffsets", "latest")
    .load()
)

# define schema for Kafka message
schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("name", StringType(), False),
        StructField("asset_platform_id", StringType(), True),
        StructField("platforms", StringType(), True),
        StructField("detail_platforms", StringType(), True),
        StructField("block_time_in_minutes", FloatType(), True),
        StructField("hashing_algorithm", StringType(), True),
        StructField("categories", StringType(), True),
        StructField("public_notice", StringType(), True),
        StructField("additional_notices", StringType(), True),
        StructField("localization", StringType(), True),
        StructField("description", StringType(), True),
        StructField("links", StringType(), True),
        StructField("image", StringType(), True),
        StructField("country_origin", StringType(), True),
        StructField("genesis_date", StringType(), True),
        StructField("sentiment_votes_up_percentage", FloatType(), True),
        StructField("sentiment_votes_down_percentage", FloatType(), True),
        StructField("market_cap_rank", FloatType(), True),
        StructField("coingecko_rank", FloatType(), True),
        StructField("coingecko_score", FloatType(), True),
        StructField("developer_score", FloatType(), True),
        StructField("community_score", FloatType(), True),
        StructField("liquidity_score", FloatType(), True),
        StructField("public_interest_score", FloatType(), True),
        StructField("market_data", StringType(), True),
        StructField("community_data", StringType(), True),
        StructField("developer_data", StringType(), True),
        StructField("public_interest_stats", StringType(), True),
        StructField("status_updates", StringType(), True),
        StructField("last_updated", StringType(), True),
        StructField("tickers", StringType(), True),
    ]
)
readDF_coins_by_id.printSchema()
value_df_coins_by_id = readDF_coins_by_id.select(
    from_json(col("value").cast("string"), schema).alias("value")
)

flattened_df_coins_by_id = (
    value_df_coins_by_id.withColumn("id", value_df_coins_by_id["value"]["id"])
    .withColumn("symbol", value_df_coins_by_id["value"]["symbol"])
    .withColumn("name", value_df_coins_by_id["value"]["name"])
    .withColumn("asset_platform_id", value_df_coins_by_id["value"]["asset_platform_id"])
    .withColumn("platforms", value_df_coins_by_id["value"]["platforms"])
    .withColumn("detail_platforms", value_df_coins_by_id["value"]["detail_platforms"])
    .withColumn(
        "block_time_in_minutes", value_df_coins_by_id["value"]["block_time_in_minutes"]
    )
    .withColumn("hashing_algorithm", value_df_coins_by_id["value"]["hashing_algorithm"])
    .withColumn("categories", value_df_coins_by_id["value"]["categories"])
    .withColumn("public_notice", value_df_coins_by_id["value"]["public_notice"])
    .withColumn("additional_notices", value_df_coins_by_id["value"]["additional_notices"])
    .withColumn("localization", value_df_coins_by_id["value"]["localization"])
    .withColumn("description", value_df_coins_by_id["value"]["description"])
    .withColumn("links", value_df_coins_by_id["value"]["links"])
    .withColumn("image", value_df_coins_by_id["value"]["image"])
    .withColumn("country_origin", value_df_coins_by_id["value"]["country_origin"])
    .withColumn("genesis_date", value_df_coins_by_id["value"]["genesis_date"])
    .withColumn(
        "sentiment_votes_up_percentage",
        value_df_coins_by_id["value"]["sentiment_votes_up_percentage"],
    )
    .withColumn(
        "sentiment_votes_down_percentage",
        value_df_coins_by_id["value"]["sentiment_votes_down_percentage"],
    )
    .withColumn("market_cap_rank", value_df_coins_by_id["value"]["market_cap_rank"])
    .withColumn("coingecko_rank", value_df_coins_by_id["value"]["coingecko_rank"])
    .withColumn("coingecko_score", value_df_coins_by_id["value"]["coingecko_score"])
    .withColumn("developer_score", value_df_coins_by_id["value"]["developer_score"])
    .withColumn("community_score", value_df_coins_by_id["value"]["community_score"])
    .withColumn("liquidity_score", value_df_coins_by_id["value"]["liquidity_score"])
    .withColumn(
        "public_interest_score", value_df_coins_by_id["value"]["public_interest_score"]
    )
    .withColumn("market_data", value_df_coins_by_id["value"]["market_data"])
    .withColumn("community_data", value_df_coins_by_id["value"]["community_data"])
    .withColumn("developer_data", value_df_coins_by_id["value"]["developer_data"])
    .withColumn(
        "public_interest_stats", value_df_coins_by_id["value"]["public_interest_stats"]
    )
    .withColumn("status_updates", value_df_coins_by_id["value"]["status_updates"])
    .withColumn("last_updated", value_df_coins_by_id["value"]["last_updated"])
    .withColumn("tickers", value_df_coins_by_id["value"]["tickers"])
    .drop("value")
)

print("{} INFO Got data from {} ".format(datetime.now(), KAFKA_TOPIC_coins_by_id))


def writeToCassandra_coins_by_id(writeDF, _):
    writeDF.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table="coins_by_id", keyspace="crypto"
    ).save()


print(
    "{} INFO Upserted the {} data into Cassandra".format(
        datetime.now(), KAFKA_TOPIC_coins_by_id
    )
)
write_stream_coins_by_id = (
    flattened_df_coins_by_id.writeStream.foreachBatch(writeToCassandra_coins_by_id)
    .outputMode("append")
    .start()
    .awaitTermination()
)

# print result
# flattened_df_coins_by_id.writeStream.outputMode("append").format('console').start().awaitTermination()
