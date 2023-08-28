import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from confluent_kafka import Consumer
import json
from datetime import datetime, timezone
import logging
from common import env_variables, utils
from cassandra_operations import cassandra_operator
import pytz
import traceback
import time

jst = pytz.timezone("Asia/Tokyo")


args = sys.argv
curr_date = args[1]
curr_timestamp = args[2]
consumer_id = args[3]
symbol = args[4]

###################
# Set logging env #
###################
logdir = "{}/{}".format(env_variables.KAFKA_LOG_HOME, curr_date)
logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=f"{logdir}/{consumer_id}_{symbol}_{curr_timestamp}.log",
    filemode="w",
)

logger = logging.getLogger()
logger.setLevel(20)


####################
# Set Kafka config #
####################
def _error_cb(error):
    print(error)


def _task_failure_alert():
    ts_now = datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S")
    message = f"{ts_now} [Failed] Kafka consumer: {consumer_id}.py ({symbol})"
    utils.send_line_message(message)


kafka_conf = {
    "bootstrap.servers": env_variables.KAFKA_BOOTSTRAP_SERVERS,
    "group.id": f"order-book-consumer-{symbol}",
    "auto.offset.reset": "earliest",
    "error_cb": _error_cb,
    "session.timeout.ms":600000,
    "max.poll.interval.ms":6000000,
}

target_topic = f"crypto.order_book_{symbol}"

# set a producer
c = Consumer(kafka_conf)
logger.info("Kafka Consumer has been initiated...")
c.subscribe([target_topic])
logger.info("Kafka Consumer is established !!!")

##########################
# Set Cassandra Operator #
##########################
keyspace = "crypto"
table_name = "order_book_realtime"
cass_ope = cassandra_operator.Operator(keyspace)

logger.info("cassandra connection is established !!!")

insert_query = f"""
INSERT INTO {table_name} (id,seqid,order_type,quote_price,base_amount,order_rank,createTime,ts_send,dt_insert_utc,ts_insert_utc) \
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


def main():
    max_retry_cnt = 5
    curr_retry_cnt = 0
    sleep_time = 600
    
    while True:
        try:
            msg = c.poll(10.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            data = json.loads(msg.value().decode("utf-8"))

            for d in data["data"]:
                id = d["id"]
                seqid = int(d["seqid"])
                createTime = d["createTime"]
                ts_send = int(d["ts_send"])
                asks = d["asks"]
                bids = d["bids"]
                
                for order_type, orders in [['ask',asks],['bid',bids]]:
                    batch_data = []
                    for i, order in enumerate(orders):
                        quote_price = float(order[0])
                        base_amount = float(order[1])
                        
                        # Rank of the order in order book. 
                        # From '1' to '20' (since websocket API can only get top 20 orders as of 2023-08-13)
                        order_rank = i+1
                        
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
                                datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                            ]
                        )
                    cass_ope.insert_batch_data(insert_query, batch_data)
                    curr_retry_cnt = 0

        except Exception as error:
            curr_retry_cnt+=1
            if curr_retry_cnt > max_retry_cnt:
                logger.error("Kafka producer failed !!!")
                logger.error("Error:".format(error))
                logger.error(traceback.format_exc())
                _task_failure_alert()
                c.close()
                sys.exit(1)
            else:
                logger.error("Kafka producer failed !!! Retry ({}/{})".format(curr_retry_cnt,max_retry_cnt))
                logger.error("Error:".format(error))
                logger.error(traceback.format_exc())
            time.sleep(sleep_time)

if __name__ == "__main__":
    main()
