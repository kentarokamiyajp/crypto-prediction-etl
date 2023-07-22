import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from confluent_kafka import admin, Producer
import json
import time
from datetime import datetime, date
import logging
import random
from poloniex_apis import get_request
from modules import env_variables, utils
import pytz

jst = pytz.timezone("Asia/Tokyo")

polo_operator = get_request.PoloniexOperator()

###################
# Set logging env #
###################

args = sys.argv
curr_date = args[1]
curr_timestamp = args[2]
print(curr_date, curr_timestamp)

logdir = "{}/{}".format(env_variables.KAFKA_LOG_HOME, curr_date)
logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=f"{logdir}/candles_minute_producer_{curr_timestamp}.log",
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
target_topic = "crypto.candles_minute"
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


########
# Main #
########
def main():
    # Set parameters for crypto data
    assets = [
        "BTC_USDT",
        "ETH_USDT",
        "BNB_USDT",
        "XRP_USDT",
        "ADA_USDT",
        "DOGE_USDT",
        "SOL_USDT",
        "TRX_USDD",
        "UNI_USDT",
        "ATOM_USDT",
        "GMX_USDT",
        "SHIB_USDT",
        "MKR_USDT",
    ]

    retry_count = 0
    max_retry_count = 5
    try:
        while True:
            # get_candles
            for asset in assets:
                try:
                    # set parameters
                    interval = "MINUTE_1"
                    period = 5  # minute
                    end = time.time()
                    start = end - 60 * period
                    
                    # get data
                    raw_candle_data = polo_operator.get_candles(
                        asset, interval, start, end
                    )
                    retry_count = 0
                except Exception as error:
                    retry_count += 1
                    logger.warning(f"API ERROR: Could not get candle data ({error})")
                    logger.warning(f"Retry Requst: {retry_count}")
                    if retry_count == max_retry_count:
                        ts_now = datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S")
                        message = f"{ts_now} [Failed] Kafka producer: candles_minute_producer.py (exceeeded max retry count)"
                        _task_failure_alert(message)
                        sys.exit(1)
                    time.sleep(600)

                candle_data = {
                    "data": [
                        {
                            "id": asset,
                            "low": data[0],
                            "high": data[1],
                            "open": data[2],
                            "close": data[3],
                            "amount": data[4],
                            "quantity": data[5],
                            "buyTakerAmount": data[6],
                            "buyTakerQuantity": data[7],
                            "tradeCount": data[8],
                            "ts": _unix_time_millisecond_to_second(data[9]),
                            "weightedAverage": data[10],
                            "interval": data[11],
                            "startTime": _unix_time_millisecond_to_second(data[12]),
                            "closeTime": _unix_time_millisecond_to_second(data[13]),
                            "dt": _get_dt_from_unix_time(data[12]),
                        }
                        for data in raw_candle_data
                    ]
                }

                m = json.dumps(candle_data)
                p.produce(
                    target_topic,
                    value=m.encode("utf-8"),
                    partition=random.randint(0, num_partitions - 1),
                    callback=_receipt,
                )
                time.sleep(5)
    except Exception as error:
        ts_now = datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S")
        message = (
            f"{ts_now} [Failed] Kafka producer: candles_minute_producer.py (unknow error)"
        )
        _task_failure_alert(message)
        logger.error(f"An exception occurred: {error}")


if __name__ == "__main__":
    main()
