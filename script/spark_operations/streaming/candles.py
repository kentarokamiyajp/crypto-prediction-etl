import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common import env_variables
from datetime import datetime, timezone
from pyspark.sql.types import *
from pyspark.sql.functions import *
from spark_operations.utils.session import SparkStreamer
from spark_operations.utils.read_stream import start_kafka_read_stream
from spark_operations.utils.write_stream import start_cassandra_write_stream


# Define spark config to create a spark session
SPARK_CONFIG = {"spark.cores.max": "1", "spark.executor.cores": "1", "spark.executor.memory": "1g"}

# Define Kafka config for streaming from a Kafka topic
# Offset&Commit information can be found at CHECKPOINT_DIR
# and they're managed by spark, not by Kafka consumer group.
KAFKA_CONFIGS = {
    "subscribe": "crypto.candles_minute",
    "maxOffsetsPerTrigger": 10000,
    "startingOffsets": "earliest",
}

# Define Cassandra config to ingest data from the Kafka stream
CASSANDRA_CONFIG = {
    "output_mode": "append",
    "dest_keyspace": "crypto",
    "dest_table": "candles_realtime",
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
                            StructField("low", StringType(), True),
                            StructField("high", StringType(), True),
                            StructField("open", StringType(), True),
                            StructField("close", StringType(), True),
                            StructField("amount", StringType(), True),
                            StructField("quantity", StringType(), True),
                            StructField("tradeCount", StringType(), True),
                            StructField("ts_send", StringType(), True),
                            StructField("startTime", StringType(), False),
                            StructField("closeTime", StringType(), False),
                        ]
                    )
                ),
                True,
            )
        ]
    )

    # Add some timestamp columns to DF from Kafka stream
    transformed_df = (
        spark_streamer.read_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("value"))
        .select(
            element_at(col("value.data.id"), 1).alias("id"),
            element_at(col("value.data.low"), 1).alias("low"),
            element_at(col("value.data.high"), 1).alias("high"),
            element_at(col("value.data.open"), 1).alias("open"),
            element_at(col("value.data.close"), 1).alias("close"),
            element_at(col("value.data.amount"), 1).alias("amount"),
            element_at(col("value.data.quantity"), 1).alias("quantity"),
            element_at(col("value.data.tradeCount"), 1).alias("tradecount"),
            element_at(col("value.data.ts_send"), 1).alias("ts_send"),
            element_at(col("value.data.startTime"), 1).alias("starttime"),
            element_at(col("value.data.closeTime"), 1).alias("closetime"),
            from_unixtime(element_at(col("value.data.ts_send"), 1), "yyyy-MM-dd HH:mm:ss").alias(
                "ts_create_utc"
            ),
            from_unixtime(element_at(col("value.data.ts_send"), 1), "yyyy-MM-dd").alias(
                "dt_create_utc"
            ),
            to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("ts_insert_utc"),
        )
    )

    # Show data
    # transformed_df.writeStream.format("console").start().awaitTermination()

    # Start writing to Cassandra
    checkpoint_location = "hdfs://{}:{}{}/{}.{}".format(
        env_variables.HDFS_HOST,
        env_variables.HDFS_PORT,
        env_variables.SPARK_STREAMING_HDFS_CHECKPOINT_DIR,
        CASSANDRA_CONFIG["dest_keyspace"],
        CASSANDRA_CONFIG["dest_table"],
    )
    print("checkpoint_location:", checkpoint_location)
    start_cassandra_write_stream(
        spark_streamer, transformed_df, checkpoint_location, CASSANDRA_CONFIG
    )

    # Keep streaming til getting termination.
    spark_streamer.write_stream.awaitTermination()


if __name__ == "__main__":
    main()
