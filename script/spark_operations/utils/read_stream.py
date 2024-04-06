import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common import env_variables

DEFAULT_CONFIGS = {
    "kafka.bootstrap.servers": env_variables.KAFKA_BOOTSTRAP_SERVERS,
    "failOnDataLoss": "false",
}


def start_kafka_read_stream(spark_streamer, kafka_configs):
    config = DEFAULT_CONFIGS
    for k, v in kafka_configs.items():
        config[k] = v

    spark_streamer.read_stream = (
        spark_streamer.spark.readStream.format("kafka").options(**config).load()
    )
