import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import time
from datetime import datetime, date
from poloniex_apis import rest_api
from common import utils
import traceback
from producer_operation import KafkaProducer
import pytz
from dotenv import load_dotenv

CONF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "conf")

# set the timezone to US/Pacific
os.environ["TZ"] = "Asia/Tokyo"
time.tzset()
TZ_JST = pytz.timezone("Asia/Tokyo")


def _unix_time_millisecond_to_second(unix_time):
    return int((unix_time) / 1000.0)


def main():
    # Get arguments
    args = sys.argv
    curr_date = args[1]
    curr_timestamp = args[2]
    producer_id = args[3]

    # Load variables from conf file
    load_dotenv(verbose=True)
    conf_file = os.path.join(CONF_DIR, f"{producer_id}.cf")
    load_dotenv(conf_file)
    num_partitions = os.environ.get("NUM_PARTITIONS")
    topic_id = os.environ.get("TOPIC_ID")
    symbols = os.environ.get("SYMBOLS").split(",")
    interval = os.environ.get("INTERVAL")
    period = os.environ.get("PERIOD")

    # Create kafka producer instance with logging.
    kafka_producer = KafkaProducer(curr_date, curr_timestamp, producer_id)

    # Create Poloniex operator to get crypto price values via REST API.
    polo_api_operator = rest_api.PoloniexOperator()

    retry_count = 0
    max_retry_count = int(os.environ.get("RETRY_COUNT"))
    try:
        while True:
            for symbol in symbols:
                try:
                    end = time.time()
                    start = end - 60 * int(period)
                    raw_candle_data = polo_api_operator.get_candles(symbol, interval, start, end)
                    retry_count = 0
                except Exception as error:
                    retry_count += 1
                    kafka_producer.logger.warning(f"API ERROR: Could not get candle data ({error})")
                    kafka_producer.logger.warning(f"Retry Request: {retry_count}")
                    kafka_producer.logger.warning(traceback.format_exc())
                    if retry_count == max_retry_count:
                        ts_now = datetime.now(TZ_JST).strftime("%Y-%m-%d %H:%M:%S")
                        message = f"{ts_now} [Failed] Kafka producer: {producer_id}.py (exceeded max retry count)"
                        utils.send_line_message(message)
                        break
                    time.sleep(600)

                candle_data = {
                    "data": [
                        {
                            "id": symbol,
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
                        }
                        for data in raw_candle_data
                    ]
                }

                kafka_producer.produce_message(topic_id, json.dumps(candle_data), int(num_partitions))
                kafka_producer.poll_message(timeout=10)
                time.sleep(10)

    except Exception as error:
        ts_now = datetime.now(TZ_JST).strftime("%Y-%m-%d %H:%M:%S")
        message = f"{ts_now} [Failed] Kafka producer: {producer_id}.py (unknown error)"
        utils.send_line_message(message)
        kafka_producer.logger.error(f"An exception occurred: {error}")
        kafka_producer.logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()
