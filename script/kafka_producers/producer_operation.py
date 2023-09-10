import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from confluent_kafka import Producer
import random
from common import env_variables
import logging


class KafkaProducer:
    def __init__(self, curr_date: str, curr_timestamp: str, producer_id: str):
        self.kafka_conf = {"bootstrap.servers": env_variables.KAFKA_BOOTSTRAP_SERVERS}
        self.Producer = Producer(self.kafka_conf)

        # set logging
        logdir = "{}/{}".format(env_variables.KAFKA_LOG_HOME, curr_date)
        logging.basicConfig(
            format="%(asctime)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            filename=f"{logdir}/{producer_id}_{curr_timestamp}.log",
            filemode="a",
        )
        self.logger = logging.getLogger()
        self.logger.setLevel(20)

    def receipt_(self, err, msg):
        if err is not None:
            self.logger.error("Error: {}".format(err))
        else:
            message = "Produced message on topic {} with value of {}\n".format(msg.topic(), msg.value().decode("utf-8"))

    def produce_message(self, topic_name: str, message: str, num_partitions: int):
        partition_id = random.randint(0, num_partitions - 1)
        self.Producer.produce(
            topic_name,
            value=message.encode("utf-8"),
            partition=partition_id,
            callback=self.receipt_,
        )

    def poll_message(self, timeout=0):
        self.Producer.poll(timeout)


if __name__ == "__main__":
    curr_date = "99999999"
    curr_timestamp = "9999-99-99 99:99:99"
    producer_id = "test"

    # Create kafka producer instance with logging.
    kafka_producer = KafkaProducer(curr_date, curr_timestamp, producer_id)
