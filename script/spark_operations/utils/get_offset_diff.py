import os, sys
import json
import time
import pytz
from datetime import datetime, timezone
from kafka import KafkaConsumer, TopicPartition

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common import env_variables

KAFKA_BOOTSTRAP_SERVERS = env_variables.KAFKA_BOOTSTRAP_SERVERS.split(",")

# set the timezone to US/Pacific
os.environ["TZ"] = "Asia/Tokyo"
time.tzset()
TZ_JST = pytz.timezone("Asia/Tokyo")


def _time_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _get_spark_offsets(spark_offset_file, kafka_topic):
    with open(spark_offset_file, "r") as f:
        tmp_spark_offsets = f.readlines()

    spark_offsets = None
    for line in tmp_spark_offsets:
        if kafka_topic in line:
            spark_offsets = json.loads(line)[kafka_topic]

    return dict(sorted(spark_offsets.items(), key=lambda x: x[0]))


def _compare_both_offsets(spark_offsets, kafka_offsets):
    offset_diff = {}
    for partition_id, spark_offset in spark_offsets.items():
        kafka_offset = kafka_offsets[int(partition_id)]
        offset_diff[f"partition-{partition_id}"] = int(kafka_offset) - int(spark_offset)

    return {'offset_diff': dict(sorted(offset_diff.items(), key=lambda x: x[0]))}


def main(spark_offset_file, kafka_topic):

    spark_offsets = _get_spark_offsets(spark_offset_file, kafka_topic)

    if spark_offsets:
        # configure consumer
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        partition_ids = consumer.partitions_for_topic(kafka_topic)

        kafka_offsets = {}

        for partition_id in partition_ids:

            partition = TopicPartition(kafka_topic, int(partition_id))
            consumer.assign([partition])

            # seek earliest offset
            consumer.seek_to_end()
            offset = consumer.position(partition)
            kafka_offsets[partition_id] = offset

        kafka_offsets = dict(sorted(kafka_offsets.items(), key=lambda x: x[0]))

    offset_diff = _compare_both_offsets(spark_offsets, kafka_offsets)

    print(
        "{} {} (kafka_offsets: {}, spark_offsets: {})".format(
            _time_now(), offset_diff, kafka_offsets, spark_offsets
        )
    )


if __name__ == "__main__":
    args = sys.argv

    if len(args) != 3:
        print("Invalid arguments !!!")
        print("e.g., python get_offset_diff.py <spark_offset_file> <kafka_topic>")
        sys.exit(1)

    spark_offset_file = args[1]
    kafka_topic = args[2]
    main(spark_offset_file, kafka_topic)
