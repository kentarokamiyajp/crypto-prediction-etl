import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from confluent_kafka import admin, Producer
import json
import time
from datetime import datetime, date
import logging
import random
from poloniex_apis import websocket_api
from common import env_variables, utils
import pytz
import traceback

jst = pytz.timezone("Asia/Tokyo")

polo_operator = websocket_api.PoloniexOperator()

###################
# Set logging env #
###################

args = sys.argv
curr_date = args[1]
curr_timestamp = args[2]
producer_id = args[3]
print(curr_date, curr_timestamp)

logdir = "{}/{}".format(env_variables.KAFKA_LOG_HOME, curr_date)
logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=f"{logdir}/{producer_id}_{curr_timestamp}.log",
    filemode="w",
)

logger = logging.getLogger()
logger.setLevel(20)


###################
# Set Kafka config #
###################
kafka_conf = {"bootstrap.servers": env_variables.KAFKA_BOOTSTRAP_SERVERS}

# Create an instance of the AdminClient
admin_client = admin.AdminClient(kafka_conf)

# Define the topic configuration
target_topic = "crypto.order_book"
num_partitions = 3
replication_factor = 2

# Create a NewTopic object
new_topic = admin.NewTopic(
    topic=target_topic,
    num_partitions=num_partitions,
    replication_factor=replication_factor,
)

# Create the topic using the AdminClient
admin_client.create_topics([new_topic])

# set a producer
p = Producer(kafka_conf)
logger.info("Kafka Producer has been initiated...")


def _receipt(err, msg):
    if err is not None:
        logger.error("Error: {}".format(err))
    else:
        message = "Produced message on topic {} with value of {}\n".format(
            msg.topic(), msg.value().decode("utf-8")
        )


def _get_dt_from_unix_time(dt_unix_time):
    dt_with_time = datetime.fromtimestamp(int(dt_unix_time) / 1000.0)
    dt = date(dt_with_time.year, dt_with_time.month, dt_with_time.day).strftime(
        "%Y-%m-%d"
    )
    return dt


def _unix_time_millisecond_to_second(unix_time):
    return int((unix_time) / 1000.0)


def _task_failure_alert(message):
    utils.send_line_message(message)


def send_to_kafka_topic(raw_data):
    order_data = {
        "data": [
            {
                "id": data["symbol"],# symbol name
                "createTime": _unix_time_millisecond_to_second(data["createTime"]),# time the record was created
                "asks": data["asks"], # sell orders, in ascending order of price
                "bids": data["bids"],# buy orders, in descending order of price
                "seqid": data["id"],# id of the record (SeqId)
                "ts_send": _unix_time_millisecond_to_second(data["ts"]),# send timestamp
            }
            for data in raw_data["data"]
        ]
    }

    m = json.dumps(order_data)
    p.produce(
        target_topic,
        value=m.encode("utf-8"),
        partition=random.randint(0, num_partitions - 1),
        callback=_receipt,
    )
    p.poll(0)

########
# Main #
########
def main():
    
    data_to_send = {
        "event": "subscribe", # event type: ping, pong, subscribe, unsubscribe, unsubscribe_all, list_subscriptions
        "channel": ["book"],
        "symbols": ["ADA_USDT",
                    "BCH_USDT",
                    "BNB_USDT",
                    "BTC_USDT",
                    "DOGE_USDT",
                    "ETH_USDT",
                    "LTC_USDT",
                    "MKR_USDT",
                    "SHIB_USDT",
                    "TRX_USDT",
                    "XRP_USDT"],
        "depth": 20
        }
    
    request_interval = 0.1 # seconds
    
    try:
        polo_operator.main_send_request(data_to_send, 
                                        request_interval, 
                                        transfer_to_kafka=True, 
                                        send_to_kafka_topic=send_to_kafka_topic)
    except Exception as error:
        ts_now = datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S")
        message = (
            f"{ts_now} [Failed] Kafka producer: {producer_id}.py (unknown error)"
        )
        _task_failure_alert(message)
        logger.error("An exception occurred: {}".format(error))
        logger.error(traceback.format_exc())

        
if __name__ == "__main__":
    main()
