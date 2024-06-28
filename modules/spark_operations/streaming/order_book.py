import os, sys
from datetime import datetime, timezone
from pyspark.sql.types import *
from pyspark.sql.functions import *

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common import env_variables
from spark_operations.utils.session import SparkStreamer
from spark_operations.utils.read_stream import start_kafka_read_stream
from spark_operations.utils.write_stream import start_cassandra_write_stream


# Define spark config to create a spark session
SPARK_CONFIG = {"spark.cores.max": "3", "spark.executor.cores": "1", "spark.executor.memory": "1g"}

# Define Kafka config for streaming from a Kafka topic
# Offset&Commit information can be found at CHECKPOINT_DIR
# and they're managed by spark, not by Kafka consumer group.
KAFKA_CONFIGS = {
    "subscribe": "crypto.order_book",
    "maxOffsetsPerTrigger": 10000,
    "startingOffsets": "earliest",
}

# Define Cassandra config to ingest data from the Kafka stream
CASSANDRA_CONFIG = {
    "output_mode": "append",
    "dest_keyspace": "crypto",
    "dest_table": "order_book_realtime",
}


def main():
    ##########################
    # Create Spark Session
    ##########################
    # Define Spark config for SparkSession.builder.config
    app_name = "SparkStreamer_{}_{}".format(
        KAFKA_CONFIGS["subscribe"], datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    )

    # Create Spark session for streaming
    spark_streamer = SparkStreamer(app_name, SPARK_CONFIG)

    ##########################
    # Stream from Kafka Topic
    ##########################
    # Start loading data from Kafka topic
    start_kafka_read_stream(spark_streamer, KAFKA_CONFIGS)

    ##########################
    # Ingest to Cassandra
    ##########################
    # Define schema for message from Kafka Topic
    schema = StructType(
        [
            StructField(
                "data",
                ArrayType(
                    StructType(
                        [
                            StructField("id", StringType(), False),
                            StructField("seqid", StringType(), False),
                            StructField("asks", ArrayType(ArrayType(StringType())), False),
                            StructField("bids", ArrayType(ArrayType(StringType())), False),
                            StructField("createTime", StringType(), True),
                            StructField("ts_send", StringType(), True),
                        ]
                    )
                ),
                True,
            )
        ]
    )

    ask_df = (
        spark_streamer.read_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("value"))
        .select(
            element_at(col("value.data.id"), 1).alias("id"),
            element_at(col("value.data.seqid"), 1).alias("seqid"),
            element_at(col("value.data.asks"), 1).alias("asks"),
            element_at(col("value.data.createTime"), 1).alias("createtime"),
            element_at(col("value.data.ts_send"), 1).alias("ts_send"),
        )
    )

    ask_df = (
        ask_df.select("*", posexplode_outer("asks"))
        .withColumn("order_rank", col("pos") + 1)
        .withColumn("quote_price", element_at(col("col"), 1))
        .withColumn("base_amount", element_at(col("col"), 2))
        .withColumn("order_type", lit("ask"))
        .drop("value", "asks", "pos", "col", "exploded_ask")
    )

    bid_df = (
        spark_streamer.read_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("value"))
        .select(
            element_at(col("value.data.id"), 1).alias("id"),
            element_at(col("value.data.seqid"), 1).alias("seqid"),
            element_at(col("value.data.bids"), 1).alias("bids"),
            element_at(col("value.data.createTime"), 1).alias("createtime"),
            element_at(col("value.data.ts_send"), 1).alias("ts_send"),
        )
    )

    bid_df = (
        bid_df.select("*", posexplode_outer("bids"))
        .withColumn("order_rank", col("pos") + 1)
        .withColumn("quote_price", element_at(col("col"), 1))
        .withColumn("base_amount", element_at(col("col"), 2))
        .withColumn("order_type", lit("bid"))
        .drop("value", "bids", "pos", "col", "exploded_bid")
    )

    final_df = (
        ask_df.union(bid_df)
        .withColumn("ts_create_utc", from_unixtime("createTime", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("dt_create_utc", from_unixtime("createTime", "yyyy-MM-dd"))
        .withColumn("ts_insert_utc", to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    )

    # show final_df
    # final_df.writeStream.format("console").start().awaitTermination()

    # Start writing to Cassandra
    checkpoint_location = "hdfs://{}:{}/{}/{}.{}".format(
        env_variables.HDFS_HOST,
        env_variables.HDFS_PORT,
        env_variables.SPARK_STREAMING_HDFS_CHECKPOINT_DIR,
        CASSANDRA_CONFIG["dest_keyspace"],
        CASSANDRA_CONFIG["dest_table"],
    )
    print("checkpoint_location:", checkpoint_location)
    start_cassandra_write_stream(spark_streamer, final_df, checkpoint_location, CASSANDRA_CONFIG)

    # Keep streaming til getting termination.
    spark_streamer.write_stream.awaitTermination()


if __name__ == "__main__":
    main()
