import os, sys
import json
import time
import pytz
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common import env_variables, utils

KAFKA_BOOTSTRAP_SERVERS = env_variables.KAFKA_BOOTSTRAP_SERVERS.split(",")
SPARK_VOLUME_HOME = env_variables.SPARK_VOLUME_HOME

# set the timezone to US/Pacific
os.environ["TZ"] = "Asia/Tokyo"
time.tzset()
TZ_JST = pytz.timezone("Asia/Tokyo")
CURRENT_DATE = datetime.now(TZ_JST).strftime("%Y%m%d")

ALLOWED_OFFSET_DIFF = 1000


def _time_now_jst():
    return datetime.now(TZ_JST).strftime("%Y-%m-%d %H:%M:%S")


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

    return {"offset_diff": dict(sorted(offset_diff.items(), key=lambda x: x[0]))}


def _check_diff(kafka_topic, current_offset_diff, tmp_file):
    try:
        with open(tmp_file, "r") as f:
            previous_offset_diff = json.load(f)["offset_diff"]
    except:
        print("There is no previous offset log !!!")
        print(f"Offset tmp file: {tmp_file}")
        return None

    for partition_id, pre_offset_diff in previous_offset_diff.items():
        curr_offset_diff = current_offset_diff[partition_id]
        if curr_offset_diff > pre_offset_diff and ALLOWED_OFFSET_DIFF < curr_offset_diff:
            print(f"curr_offset_diff: {curr_offset_diff}")
            print(f"pre_offset_diff: {pre_offset_diff}")
            print(f"ALLOWED_OFFSET_DIFF: {ALLOWED_OFFSET_DIFF}")
            message = (
                f"FAILED Spark Streaming !!!\n\nToo many offset differences in {kafka_topic} !!!"
            )
            utils.send_line_message(message)
            sys.exit(1)

    return None


def main(spark_offset_file, kafka_topic, printing=True):

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

    if printing:
        print(
            "{} {} (kafka_offsets: {}, spark_offsets: {})".format(
                _time_now_jst(), offset_diff, kafka_offsets, spark_offsets
            )
        )

    tmp_file = f"{SPARK_VOLUME_HOME}/tmp/{CURRENT_DATE}/offset_diff_tmp_{kafka_topic}.txt"

    # Compare "current offset diff" and "previous offset diff"
    _check_diff(kafka_topic, offset_diff["offset_diff"], tmp_file)

    # Save "current offset diff" for the next comparison
    with open(tmp_file, "w") as f:
        json.dump(offset_diff, f)


if __name__ == "__main__":
    args = sys.argv

    if len(args) != 3:
        print("Invalid arguments !!!")
        print("e.g., python check_offset_diff.py <spark_offset_file> <kafka_topic>")
        sys.exit(1)

    spark_offset_file = args[1]
    kafka_topic = args[2]
    main(spark_offset_file, kafka_topic)
