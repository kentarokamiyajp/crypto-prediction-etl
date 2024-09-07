"""
1. Consume topic from the existing topic 'crypto.candles_minute'
2. Aggregation by grouping by "minute"
3. Produce the aggregated data to a new topic
"""

import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import time
import traceback
import pytz
import pandas as pd
from consumer_operation import KafkaConsumer
from dotenv import load_dotenv
from datetime import datetime, timezone, date, timedelta
from common import utils
from common.indicators import get_indicators, get_quotes
from pprint import pprint
from kafka_producers.KafkaBaseProducer import KafkaBaseProducer

CONF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "conf")

# set the timezone to US/Pacific
os.environ["TZ"] = "Asia/Tokyo"
time.tzset()
TZ_JST = pytz.timezone("Asia/Tokyo")


def consume_realtime_data(curr_date, curr_timestamp, consumer_id):

    # Kafka config
    topic_id = os.environ.get("TOPIC_ID")
    group_id = os.environ.get("GROUP_ID")
    offset_type = os.environ.get("OFFSET_TYPE")

    # Create consumer
    consumer = KafkaConsumer(curr_date, curr_timestamp, consumer_id, group_id, offset_type)

    # Create producer
    producer = KafkaBaseProducer()

    # Get the topic's partitions
    partitions = consumer.get_partitions(topic_id)
    num_partitions = len(partitions)

    # init variables
    latest_minute_data = {}
    prev_candle_close_ts_ymdhm = {}
    indicator_base_df = pd.DataFrame(
        columns=["open", "high", "low", "close", "amount", "ts_create_utc", "dt"]
    )

    consumer.subscribe([topic_id])

    consumer.logger.info("Start to consume")
    while True:
        msg = consumer.poll(10.0)

        if msg is None:
            continue

        if msg.error():
            consumer.logger.error("Consumer error: {}".format(msg.error()))
            sys.exit(1)

        consumed_data = json.loads(msg.value().decode("utf-8"))
        crypto_id = consumed_data["data"][0]["id"]
        this_open = consumed_data["data"][0]["open"]
        this_high = consumed_data["data"][0]["high"]
        this_low = consumed_data["data"][0]["low"]
        this_close = consumed_data["data"][0]["close"]

        # Focus on only BTC
        if crypto_id != "BTC_USDT":
            continue

        this_msg_partition = msg.partition()
        this_candle_close_ts_ymdhm = datetime.utcfromtimestamp(
            int(consumed_data["data"][0]["closeTime"])
        ).strftime("%Y-%m-%d %H:%M")
        this_candle_close_ts_ymdhms = datetime.utcfromtimestamp(
            int(consumed_data["data"][0]["closeTime"])
        ).strftime("%Y-%m-%d %H:%M:%S")
        this_candle_ts_send_ymdhms = datetime.utcfromtimestamp(
            int(consumed_data["data"][0]["ts_send"])
        ).strftime("%Y-%m-%d %H:%M:%S")

        print(
            f"{crypto_id}: timestamp for the latest data -> {this_candle_ts_send_ymdhms} ({this_open}, {this_high}, {this_low}, {this_close})"
        )

        ########################
        # MAIN PART
        ########################
        # Publish candle data to kafka topic if it meets the following conditions
        if (
            latest_minute_data != {}
            and prev_candle_close_ts_ymdhm.get(crypto_id, "2099-01-01 00:00")
            < this_candle_close_ts_ymdhm
        ):

            # fetched all of data in a minute (MM:00~MM:59)
            # so push the latest data (MM:59) to a topic

            base_message = latest_minute_data[crypto_id]["data"][0]
            ts_create_utc = datetime.utcfromtimestamp(int(base_message["closeTime"]))

            indicator_base_df = indicator_base_df.drop(["dt"], axis=1)
            indicator_base_df = indicator_base_df.append(
                {
                    "open": float(base_message["open"]),
                    "high": float(base_message["high"]),
                    "low": float(base_message["low"]),
                    "close": float(base_message["close"]),
                    "amount": float(base_message["amount"]),
                    "ts_create_utc": str(ts_create_utc),
                },
                ignore_index=True,
            )

            indicator_base_df["dt"] = pd.date_range(
                start="1/1/2018", periods=len(indicator_base_df)
            )

            indicator_base_df = indicator_base_df.iloc[-180:]

            print(indicator_base_df)

            quotes = get_quotes(indicator_base_df)
            target_indicators = ["macd", "stoch", "sma5", "sma10", "sma30", "sma60"]
            indicators = get_indicators(quotes, target_indicators)

            # OHLC data
            message = {
                "open": base_message["open"],
                "high": base_message["high"],
                "low": base_message["low"],
                "close": base_message["close"],
                "sma5": indicators["sma5"][-1].sma,
                "sma10": indicators["sma10"][-1].sma,
                "sma30": indicators["sma30"][-1].sma,
                "sma60": indicators["sma60"][-1].sma,
                "macd": indicators["macd"][-1].macd,
                "macd_signal": indicators["macd"][-1].signal,
                "stoch_d": indicators["stoch"][-1].d,
                "stoch_k": indicators["stoch"][-1].k,
                "stoch_j": indicators["stoch"][-1].j,
                "ts_create_utc": str(ts_create_utc),
            }

            print()
            print(base_message["id"])
            print(message)
            print()

            # Publish the message
            produce_minute_data(producer, message, key=base_message["id"])

        latest_minute_data[crypto_id] = consumed_data

        prev_candle_close_ts_ymdhm[crypto_id] = this_candle_close_ts_ymdhm


def produce_minute_data(producer, message, key):

    topic_name = "crypto.candles_minute_test_2"
    num_partitions = 3

    producer.produce_message(topic_name, json.dumps(message), num_partitions, key=key)
    producer.poll_message(timeout=10)


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

    consume_realtime_data(curr_date, curr_timestamp, consumer_id)


if __name__ == "__main__":
    main()
