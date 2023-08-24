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

###################
# Set logging env #
###################

args = sys.argv
curr_date = args[1]
curr_timestamp = args[2]
consumer_id = args[3]
logdir = "{}/{}".format(env_variables.KAFKA_LOG_HOME, curr_date)
logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=f"{logdir}/{consumer_id}_{curr_timestamp}.log",
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
    message = f"{ts_now} [Failed] Kafka consumer: {consumer_id}.py"
    utils.send_line_message(message)


kafka_conf = {
    "bootstrap.servers": env_variables.KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "candles-minute-consumer",
    "auto.offset.reset": "earliest",
    "error_cb": _error_cb,
    "session.timeout.ms":600000,
    "max.poll.interval.ms":6000000,
}

target_topic = "crypto.candles_minute"

# set a producer
c = Consumer(kafka_conf)
logger.info("Kafka Consumer has been initiated...")
c.subscribe([target_topic])


##########################
# Set Cassandra Operator #
##########################
keyspace = "crypto"
table_name = "candles_minute_realtime"
cass_ope = cassandra_operator.Operator(keyspace)

insert_query = f"""
INSERT INTO {table_name} (id,low,high,open,close,amount,quantity,buyTakerAmount,\
    buyTakerQuantity,tradeCount,ts,weightedAverage,interval,startTime,closeTime,dt,ts_insert_utc)\
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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

            batch_data = []
            for d in data["data"]:
                batch_data.append(
                    [
                        d["id"],
                        float(d["low"]),
                        float(d["high"]),
                        float(d["open"]),
                        float(d["close"]),
                        float(d["amount"]),
                        float(d["quantity"]),
                        float(d["buyTakerAmount"]),
                        float(d["buyTakerQuantity"]),
                        int(d["tradeCount"]),
                        int(d["ts"]),
                        float(d["weightedAverage"]),
                        d["interval"],
                        int(d["startTime"]),
                        int(d["closeTime"]),
                        d["dt"],
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
