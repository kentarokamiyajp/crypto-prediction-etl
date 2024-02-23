import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import env_variables, utils
from datetime import datetime
from dotenv import load_dotenv
import confluent_kafka
import pytz
import time
import traceback
import logging

# set the timezone to US/Pacific
os.environ["TZ"] = "Asia/Tokyo"
time.tzset()
TZ_JST = pytz.timezone("Asia/Tokyo")

CONF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "conf")


def _set_logger(curr_date: str, curr_timestamp: str, group_id: str) -> logging.Logger:
    logdir = "{}/{}".format(env_variables.KAFKA_LOG_HOME, curr_date)
    logging.basicConfig(
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        filename=f"{logdir}/offset_lags_check_{group_id}_{curr_timestamp}.log",
        filemode="a",
    )
    logger = logging.getLogger()
    logger.setLevel(20)
    return logger


def main():
    # Get arguments
    args = sys.argv
    curr_date = args[1]
    curr_timestamp = args[2]
    group_id = args[3]

    # set logger
    logger = _set_logger(curr_date, curr_timestamp, group_id)

    # Load variables from conf file
    load_dotenv(verbose=True)
    conf_file = os.path.join(CONF_DIR, f"{group_id}_offset_lags_check.cf")
    load_dotenv(conf_file)

    # Variables for offset lags check
    topic_ids = os.environ.get("TOPIC_IDS").split(",")
    if len(topic_ids) < 1:
        logger.error("Failed to get topics !!!")
        logger.error(f"topic_ids = {topic_ids}")
        sys.exit(1)

    max_offset_lags = int(os.environ.get("MAX_OFFSET_LAGS"))
    sleep_time = int(os.environ.get("SLEEP_TIME"))

    consumer = confluent_kafka.Consumer({"bootstrap.servers": env_variables.KAFKA_BOOTSTRAP_SERVERS, "group.id": group_id})
    max_notification_count = 3
    curr_notification_count = 0
    while True:
        try:
            total_lags = 0
            for topic in topic_ids:
                # Get the topic's partitions
                metadata = consumer.list_topics(topic, timeout=10)
                if metadata.topics[topic].error is not None:
                    ts_now = datetime.now(TZ_JST).strftime("%Y-%m-%d %H:%M:%S")
                    message = f"{ts_now} [WARNING] Check Kafka Consumer Offsets: topic '{topic}' does not exist !!!"
                    utils.send_line_message(message)
                    raise confluent_kafka.KafkaException(metadata.topics[topic].error)

                # Construct TopicPartition list of partitions to query
                partitions = [confluent_kafka.TopicPartition(topic, p) for p in metadata.topics[topic].partitions]

                # Query committed offsets for this group and the given partitions
                committed = consumer.committed(partitions, timeout=10)

                for partition in committed:
                    # Get the partitions low and high watermark offsets.
                    (lo, hi) = consumer.get_watermark_offsets(partition, timeout=10, cached=False)
                    if hi < 0:
                        ts_now = datetime.now(TZ_JST).strftime("%Y-%m-%d %H:%M:%S")
                        message = f"{ts_now} [WARNING] Check Kafka Consumer Offsets: Consumer seems not working well ({group_id}) !!!"
                        utils.send_line_message(message)
                        curr_notification_count += 1
                    elif partition.offset < 0:
                        total_lags += int(hi - lo)
                    else:
                        total_lags += int(hi - partition.offset)
            
            
            logger.info(f"Current offset lags for {group_id}: {total_lags}")

            # If lag exceeds the max acceptable number, send a notification.
            if total_lags > max_offset_lags:
                ts_now = datetime.now(TZ_JST).strftime("%Y-%m-%d %H:%M:%S")
                message = f"{ts_now} [WARNING] Huge Offset Lags: {group_id} !!!"
                utils.send_line_message(message)
                curr_notification_count += 1

            if curr_notification_count > max_notification_count:
                ts_now = datetime.now(TZ_JST).strftime("%Y-%m-%d %H:%M:%S")
                message = f"{ts_now} [FAILED] Stop checking offset lags for today !!!"
                utils.send_line_message(message)
                consumer.close()
                
                # change here for weekly batch
                curr_notification_count = 0
                time.sleep(3600*24) # sleep 1 day
                # sys.exit(1)

        except Exception as error:
            logger.error("Failed to check consumer offset lags !!!")
            logger.error("Error:".format(error))
            logger.error(traceback.format_exc())
            ts_now = datetime.now(TZ_JST).strftime("%Y-%m-%d %H:%M:%S")
            message = f"{ts_now} [FAILED] Failed to check consumer offset lags: {group_id} !!!"
            utils.send_line_message(message)
            consumer.close()
            sys.exit(1)

        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
