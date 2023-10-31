import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import time
import traceback
import pytz
from consumer_operation import KafkaConsumer
from dotenv import load_dotenv
from datetime import datetime, timezone, date
from cassandra_operations import cassandra_operator
from common import utils

CONF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "conf")

# set the timezone to US/Pacific
os.environ["TZ"] = "Asia/Tokyo"
time.tzset()
TZ_JST = pytz.timezone("Asia/Tokyo")


def main():
    # Get arguments
    args = sys.argv
    curr_date = args[1]
    curr_timestamp = args[2]
    consumer_id = args[3]

    # Load variables from conf file
    load_dotenv(verbose=True)
    conf_file = os.path.join(CONF_DIR, f"{consumer_id}.cf")
    load_dotenv(conf_file)

    # Kafka config
    topic_id = os.environ.get("TOPIC_ID")
    group_id = os.environ.get("GROUP_ID")
    offset_type = os.environ.get("OFFSET_TYPE")

    # Cassandra config
    keyspace = os.environ.get("KEYSPACE")
    table_name = os.environ.get("TABLE_NAME")
    cass_ope = cassandra_operator.Operator(keyspace)
    insert_query = f"""
    INSERT INTO {table_name} (id,low,high,open,close,amount,quantity,\
        tradeCount,startTime,closeTime,ts_send,dt_create_utc,ts_create_utc,ts_insert_utc)\
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    # Create consumer
    consumer = KafkaConsumer(curr_date, curr_timestamp, consumer_id, group_id, offset_type)
    consumer.subscribe([topic_id])

    max_retry_cnt = int(os.environ.get("RETRY_COUNT"))
    curr_retry_cnt = 0
    sleep_time = 600

    consumer.logger.info("Start to consume")
    while True:
        try:
            msg = consumer.poll(10.0)
            if msg is None:
                continue
            if msg.error():
                consumer.logger.error("Consumer error: {}".format(msg.error()))
                sys.exit(1)

            consumed_data = json.loads(msg.value().decode("utf-8"))

            batch_data = []
            for d in consumed_data["data"]:
                ts_create_utc = datetime.utcfromtimestamp(int(d['closeTime']))
                dt_create_utc = date(ts_create_utc.year, ts_create_utc.month, ts_create_utc.day).strftime("%Y-%m-%d")
                
                batch_data.append(
                    [
                        d["id"],
                        float(d["low"]),
                        float(d["high"]),
                        float(d["open"]),
                        float(d["close"]),
                        float(d["amount"]),
                        float(d["quantity"]),
                        int(d["tradeCount"]),
                        int(d["startTime"]),
                        int(d["closeTime"]),
                        int(d["ts_send"]),
                        dt_create_utc,
                        str(ts_create_utc),
                        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    ]
                )
            cass_ope.insert_batch_data(insert_query, batch_data)
            curr_retry_cnt = 0

        except Exception as error:
            curr_retry_cnt += 1
            if curr_retry_cnt > max_retry_cnt:
                consumer.logger.error("Kafka Consumer failed !!!")
                consumer.logger.error("Error:".format(error))
                consumer.logger.error(traceback.format_exc())
                ts_now = datetime.now(TZ_JST).strftime("%Y-%m-%d %H:%M:%S")
                message = f"{ts_now} [Failed] Kafka consumer: {consumer_id}.py"
                utils.send_line_message(message)
                consumer.close()
                break
            else:
                consumer.logger.error("Kafka Consumer failed !!! Retry ({}/{})".format(curr_retry_cnt, max_retry_cnt))
                consumer.logger.error("Error:".format(error))
                consumer.logger.error(traceback.format_exc())
            time.sleep(sleep_time)


if __name__ == "__main__":
    main()
