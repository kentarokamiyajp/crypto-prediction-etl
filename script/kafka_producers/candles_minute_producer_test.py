import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import time
from datetime import datetime, date
import threading
from common import utils
import traceback
import pytz
from dotenv import load_dotenv
from poloniex_apis import websocket_api

CONF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "conf")

# set the timezone to US/Pacific
os.environ["TZ"] = "Asia/Tokyo"
time.tzset()
TZ_JST = pytz.timezone("Asia/Tokyo")


def _unix_time_millisecond_to_second(unix_time):
    return int((unix_time) / 1000.0)


def process_websocket_response(response):
    candle_data = {
        "data": [
            {
                "id": data["symbol"],  # symbol name
                "low":data["low"], # lowest price over the interval
                "high":data["high"], # highest price over the interval
                "open":data["open"], # price at the start time
                "close":data["close"], # price at the end time
                "amount":data["amount"], # quote units traded over the interval
                "quantity":data["quantity"], # base units traded over the interval
                "tradeCount":data["tradeCount"], # count of trades
                "ts_send": _unix_time_millisecond_to_second(data["ts"]),  # send timestamp
                "startTime": _unix_time_millisecond_to_second(data["startTime"]), # start time of interval
                "closeTime": _unix_time_millisecond_to_second(data["closeTime"]), # close time of interval
            }
            for data in response["data"]
        ]
    }
    return candle_data


def _start_procedure(producer_id, connection_type, request_data, send_kafka, kafka_config):
    polo_ws_operator = websocket_api.PoloniexSocketOperator(connection_type, request_data, send_kafka, kafka_config)

    retry_count = 0
    max_retry_count = int(os.environ.get("RETRY_COUNT"))
    try:
        while True:
            try:
                polo_ws_operator.run_forever()
            except Exception as error:
                polo_ws_operator.kafka_producer.logger.warning(f"API ERROR: Could not get candle minute data ({error})")
                polo_ws_operator.kafka_producer.logger.warning(f"Retry Request: {retry_count}")
                polo_ws_operator.kafka_producer.logger.warning(traceback.format_exc())
                if retry_count > max_retry_count:
                    break
                retry_count += 1
                time.sleep(60)

    except Exception as error:
        ts_now = datetime.now(TZ_JST).strftime("%Y-%m-%d %H:%M:%S")
        message = f"{ts_now} [Failed] Kafka producer: {producer_id}.py (exceeded max retry count)"
        utils.send_line_message(message)
        polo_ws_operator.kafka_producer.logger.error(f"An exception occurred: {error}")
        polo_ws_operator.kafka_producer.logger.error(traceback.format_exc())


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
    
    for symbol in symbols:
        subscribe_payload = {
            "event": "subscribe",
            "channel": ["candles_minute_1"],
            "symbols": [symbol],
        }

        ping_payload = {"event": "ping"}

        # Create Poloniex WebSocket operator
        connection_type = "public"
        request_data = {
            "subscribe_payload": json.dumps(subscribe_payload),
            "ping_payload": json.dumps(ping_payload),
        }
        send_kafka = True
        kafka_config = {
            "curr_date": curr_date,
            "curr_timestamp": curr_timestamp,
            "producer_id": producer_id,
            "topic_id": topic_id,
            "num_partitions": num_partitions,
            "func_process_response": process_websocket_response,
        }

        thread = threading.Thread(
            target=_start_procedure,
            args=(
                producer_id,
                connection_type,
                request_data,
                send_kafka,
                kafka_config,
            ),
        )
        thread.start()


if __name__ == "__main__":
    main()
