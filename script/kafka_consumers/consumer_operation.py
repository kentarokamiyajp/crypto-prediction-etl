"""
confluent_kafka:
    https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-consumer
"""

import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from confluent_kafka import Consumer, KafkaException, TopicPartition
from common import env_variables
import logging


class KafkaConsumer:
    def __init__(
        self,
        curr_date: str,
        curr_timestamp: str,
        consumer_id: str,
        group_id: str,
        offset_type: str,
        _logdir: str = None,
    ):
        self.kafka_conf = {
            "bootstrap.servers": env_variables.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": offset_type,
            "session.timeout.ms": 600000,
            "max.poll.interval.ms": 6000000,
        }
        self.consumer = Consumer(self.kafka_conf)

        # set logging
        if _logdir == None:
            logdir = "{}/{}".format(env_variables.KAFKA_LOG_HOME, curr_date)
        else:
            logdir = _logdir
            
        logging.basicConfig(
            format="%(asctime)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            filename=f"{logdir}/{consumer_id}_{curr_timestamp}.log",
            filemode="a",
        )
        self.logger = logging.getLogger()
        self.logger.setLevel(20)

    def subscribe(self, topics: list):
        self.consumer.subscribe(topics)

    def poll(self, timeout: float):
        return self.consumer.poll(timeout)

    def close(self):
        self.consumer.close()

    def get_partitions(self, topic):
        # Get the topic's partitions
        metadata = self.consumer.list_topics(topic, timeout=10)
        if metadata.topics[topic].error is not None:
            raise KafkaException(metadata.topics[topic].error)

        # Construct TopicPartition list of partitions to query
        partitions = [TopicPartition(topic, p) for p in metadata.topics[topic].partitions]

        return partitions
