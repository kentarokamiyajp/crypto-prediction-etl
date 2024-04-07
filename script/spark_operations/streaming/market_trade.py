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
    "subscribe": "crypto.market_trade",
    "maxOffsetsPerTrigger": 1000,
    "startingOffsets": "earliest",
}

# Define Cassandra config to ingest data from the Kafka stream
CASSANDRA_CONFIG = {
    "output_mode": "append",
    "dest_keyspace": "crypto",
    "dest_table": "test_market_trade_realtime",
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
                            StructField("trade_id", StringType(), False),
                            StructField("takerSide", StringType(), True),
                            StructField("amount", StringType(), True),
                            StructField("quantity", StringType(), True),
                            StructField("price", StringType(), True),
                            StructField("createTime", StringType(), True),
                            StructField("ts_send", StringType(), True),
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
            element_at(col("value.data.trade_id"), 1).alias("trade_id"),
            element_at(col("value.data.takerSide"), 1).alias("takerside"),
            element_at(col("value.data.amount"), 1).alias("amount"),
            element_at(col("value.data.quantity"), 1).alias("quantity"),
            element_at(col("value.data.price"), 1).alias("price"),
            element_at(col("value.data.createTime"), 1).alias("createtime"),
            element_at(col("value.data.ts_send"), 1).alias("ts_send"),
            from_unixtime(element_at(col("value.data.createTime"), 1), "yyyy-MM-dd HH:mm:ss").alias(
                "ts_create_utc"
            ),
            from_unixtime(element_at(col("value.data.createTime"), 1), "yyyy-MM-dd").alias(
                "dt_create_utc"
            ),
            to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("ts_insert_utc"),
        )
    )

    # Start writing to Cassandra
    checkpoint_location = "{}/{}.{}".format(
        env_variables.SPARK_STREAMING_CHECKPOINT_DIR,
        CASSANDRA_CONFIG["dest_keyspace"],
        CASSANDRA_CONFIG["dest_table"],
    )
    print("checkpoint_location:",checkpoint_location)
    start_cassandra_write_stream(
        spark_streamer, transformed_df, checkpoint_location, CASSANDRA_CONFIG
    )

    # Keep streaming til getting termination.
    spark_streamer.write_stream.awaitTermination()


if __name__ == "__main__":
    main()
