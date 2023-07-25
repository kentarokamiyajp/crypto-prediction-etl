from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import DataStreamWriter
from pprint import pprint
from datetime import datetime
import logging
from common import env_variables


args = sys.argv
curr_date = args[1]
curr_timestamp = args[2]
logdir = f"{curr_date}"

logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=f"{logdir}/spark_streaming_coins_market_{curr_timestamp}.log",
    filemode="w",
)

logger = logging.getLogger()
logger.setLevel(10)

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
    .master("local[1]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

KAFKA_BOOTSTRAP_SERVER = env_variables.KAFKA_BOOTSTRAP_SERVERS

# Load messages from Kafka Topic
KAFKA_TOPIC_coins_markets = "crypto.coins_markets"

readDF_coins_market = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", KAFKA_TOPIC_coins_markets)
    .option("startingOffsets", "latest")
    .load()
)

# define schema for Kafka message
schema = StructType(
    [
        StructField(
            "data",
            ArrayType(
                StructType(
                    [
                        StructField("id", StringType(), False),
                        StructField("symbol", StringType(), False),
                        StructField("name", StringType(), False),
                        StructField("image", StringType(), True),
                        StructField("current_price", FloatType(), True),
                        StructField("market_cap", FloatType(), True),
                        StructField("market_cap_rank", FloatType(), True),
                        StructField("fully_diluted_valuation", FloatType(), True),
                        StructField("total_volume", FloatType(), True),
                        StructField("high_24h", FloatType(), True),
                        StructField("low_24h", FloatType(), True),
                        StructField("price_change_24h", FloatType(), True),
                        StructField("price_change_percentage_24h", FloatType(), True),
                        StructField("market_cap_change_24h", FloatType(), True),
                        StructField(
                            "market_cap_change_percentage_24h", FloatType(), True
                        ),
                        StructField("circulating_supply", FloatType(), True),
                        StructField("total_supply", FloatType(), True),
                        StructField("max_supply", FloatType(), True),
                        StructField("ath", FloatType(), True),
                        StructField("ath_change_percentage", FloatType(), True),
                        StructField("ath_date", StringType(), True),
                        StructField("atl", FloatType(), True),
                        StructField("atl_change_percentage", FloatType(), True),
                        StructField("atl_date", StringType(), True),
                        StructField("roi", StringType(), True),
                        StructField("last_updated", StringType(), True),
                    ]
                )
            ),
        )
    ]
)
readDF_coins_market.printSchema()
value_df_coins_market = readDF_coins_market.select(
    from_json(col("value").cast("string"), schema).alias("value")
)
exploded_df_coins_market = value_df_coins_market.selectExpr("explode(value.data) as data")

flattened_df_coins_market = (
    exploded_df_coins_market.withColumn("id", expr("data.id"))
    .withColumn("name", expr("data.name"))
    .withColumn("ath", expr("data.ath"))
    .withColumn("ath_change_percentage", expr("data.ath_change_percentage"))
    .withColumn("ath_date", expr("data.ath_date"))
    .withColumn("atl", expr("data.atl"))
    .withColumn("atl_change_percentage", expr("data.atl_change_percentage"))
    .withColumn("atl_date", expr("data.atl_date"))
    .withColumn("circulating_supply", expr("data.circulating_supply"))
    .withColumn("current_price", expr("data.current_price"))
    .withColumn("fully_diluted_valuation", expr("data.fully_diluted_valuation"))
    .withColumn("high_24h", expr("data.high_24h"))
    .withColumn("image", expr("data.image"))
    .withColumn("last_updated", expr("data.last_updated"))
    .withColumn("low_24h", expr("data.low_24h"))
    .withColumn("market_cap", expr("data.market_cap"))
    .withColumn("market_cap_change_24h", expr("data.market_cap_change_24h"))
    .withColumn(
        "market_cap_change_percentage_24h", expr("data.market_cap_change_percentage_24h")
    )
    .withColumn("market_cap_rank", expr("data.market_cap_rank"))
    .withColumn("max_supply", expr("data.max_supply"))
    .withColumn("price_change_24h", expr("data.price_change_24h"))
    .withColumn("price_change_percentage_24h", expr("data.price_change_percentage_24h"))
    .withColumn("roi", expr("data.roi"))
    .withColumn("symbol", expr("data.symbol"))
    .withColumn("total_supply", expr("data.total_supply"))
    .withColumn("total_volume", expr("data.total_volume"))
    .drop("data")
)

logger.info("{} INFO Got data from {} ".format(datetime.now(), KAFKA_TOPIC_coins_markets))


def writeToCassandra_coins_market(writeDF, _):
    writeDF.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        table="coins_markets", keyspace="crypto"
    ).save()


logger.info(
    "{} INFO Upserted the {} data into Cassandra".format(
        datetime.now(), KAFKA_TOPIC_coins_markets
    )
)
write_stream_coins_market = (
    flattened_df_coins_market.writeStream.foreachBatch(writeToCassandra_coins_market)
    .outputMode("update")
    .start()
    .awaitTermination()
)
