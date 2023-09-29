import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import time
from datetime import datetime, timezone, date
from cassandra_operations import cassandra_operator
from common import utils
import traceback
from consumer_operation import KafkaConsumer
import pytz
from dotenv import load_dotenv

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
    INSERT INTO {table_name} (id,seqid,order_type,quote_price,base_amount,order_rank,createTime,ts_send,dt_create_utc,ts_create_utc,ts_insert_utc) \
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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

            for d in consumed_data["data"]:
                id = d["id"]
                seqid = int(d["seqid"])
                createTime = d["createTime"]
                ts_send = int(d["ts_send"])
                asks = d["asks"]
                bids = d["bids"]
                ts_create_utc = datetime.utcfromtimestamp(int(d['createTime']))
                dt_create_utc = date(ts_create_utc.year, ts_create_utc.month, ts_create_utc.day).strftime("%Y-%m-%d")

                for order_type, orders in [["ask", asks], ["bid", bids]]:
                    batch_data = []
                    for i, order in enumerate(orders):
                        quote_price = float(order[0])
                        base_amount = float(order[1])

                        # Rank of the order in order book.
                        # From '1' to '20' (since websocket API can only get top 20 orders as of 2023-08-13)
                        order_rank = i + 1

                        batch_data.append(
                            [
                                id,
                                seqid,
                                order_type,
                                quote_price,
                                base_amount,
                                order_rank,
                                createTime,
                                ts_send,
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
                consumer.logger.error("Kafka consumer failed !!!")
                consumer.logger.error("Error:".format(error))
                consumer.logger.error(traceback.format_exc())
                ts_now = datetime.now(TZ_JST).strftime("%Y-%m-%d %H:%M:%S")
                message = f"{ts_now} [Failed] Kafka consumer: {consumer_id}.py"
                utils.send_line_message(message)
                consumer.close()
                break
            else:
                consumer.logger.error("Kafka consumer failed !!! Retry ({}/{})".format(curr_retry_cnt, max_retry_cnt))
                consumer.logger.error("Error:".format(error))
                consumer.logger.error(traceback.format_exc())
            time.sleep(sleep_time)


if __name__ == "__main__":
    main()
