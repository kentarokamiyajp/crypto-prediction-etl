import os, sys
import random
import json
from confluent_kafka import Producer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import env_variables


class KafkaBaseProducer:
    def __init__(self):
        kafka_conf = {"bootstrap.servers": env_variables.KAFKA_BOOTSTRAP_SERVERS}
        self.Producer = Producer(kafka_conf)

    def produce_message(self, topic_name: str, message: str, num_partitions: int, key: str = None):
        partition_id = random.randint(0, num_partitions - 1)
        self.Producer.produce(topic_name, value=message.encode("utf-8"), partition=partition_id, key=key)

    def poll_message(self, timeout=0):
        self.Producer.poll(timeout)


if __name__ == "__main__":
    producer = KafkaBaseProducer()

    topic_name = "crypto.candles_minute_test_2"
    message = {"a": 100}
    num_partitions = 3

    producer.produce_message(topic_name, json.dumps(message), num_partitions)
    producer.poll_message(timeout=10)
