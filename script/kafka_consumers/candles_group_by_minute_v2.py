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


# Calculate Gradient
def calculate_gradient(df, column_name, span):
    gradient = []

    for i in range(len(df)):
        try:
            gradient.append((df[column_name][i] - df[column_name][i - span]) / (i - (i - span)))
        except:
            gradient.append(None)

    return gradient


def calculate_gain(past_price, current_price):
    return round(((current_price / past_price) - 1.0) * 100.0, 2)


def check_buy_condition(
    grad_threshold,
    ema30_5_span_grad,
    macd_golden_cross,
    stoch_golden_cross,
    stoch_all_dead_cross,
    macd_grad,
    macd_signal_grad,
    macd_signal_grad_threshold,
):
    if (
        ema30_5_span_grad > grad_threshold
        and 1 == macd_golden_cross[-1]
        # and 1 in macd_golden_cross
        and 1 in stoch_golden_cross
        # and 1 not in stoch_all_dead_cross
        # and abs(macd_signal_grad) > macd_signal_grad_threshold
        # and abs(macd_grad - macd_signal_grad) > macd_signal_grad_threshold
    ):
        return True
    return False


def check_sell_condition(
    take_profit_threshold,
    cut_loss_threshold,
    acceptable_gain_loss,
    prev_gain,
    current_gain,
    long_position_period,
    long_position_period_threshold,
    macd_dead_cross_happened,
    macd,
    macd_signal,
    macd_golden_cross_threshold,
    stoch_k,
    stoch_d,
    selling_stoch_threshold,
    macd_dead_cross,
    macd_all_dead_cross,
    stoch_dead_cross,
):
    if (
        (current_gain > take_profit_threshold)
        # or (prev_gain - acceptable_gain_loss > current_gain)
        or (current_gain < cut_loss_threshold)
        or (long_position_period > long_position_period_threshold)
        # or (1 in macd_dead_cross and 1 in stoch_dead_cross)
        or (1 in macd_dead_cross)
        or (
            1 in stoch_dead_cross
            and (stoch_k < selling_stoch_threshold or stoch_d < selling_stoch_threshold)
        )
        or (abs(current_gain) < 0.01 and long_position_period > 10)
        or (macd_dead_cross_happened == True and 0.0 < macd < macd_golden_cross_threshold / 2.0)
    ):
        return True

    return False


def consume_realtime_data(curr_date, curr_timestamp, consumer_id, init_trade_variables):

    # Kafka config
    topic_id = os.environ.get("TOPIC_ID")
    group_id = os.environ.get("GROUP_ID")
    offset_type = os.environ.get("OFFSET_TYPE")
    
    trade_variables = {}

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
    prev_candle_close_ts_ymdhms = None
    indicator_base_df = pd.DataFrame(
        columns=["open", "high", "low", "close", "amount", "ts_create_utc", "dt"]
    )
    indicator_df = pd.DataFrame(
        columns=[
            "open",
            "high",
            "low",
            "close",
            "sma5",
            "sma10",
            "sma30",
            "sma60",
            "macd",
            "macd_signal",
            "stoch_d",
            "stoch_k",
            "stoch_j",
            "ts_create_utc",
            "dt",
        ]
    )
    prev_message = None

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
        dt_today = datetime.utcfromtimestamp(int(consumed_data["data"][0]["ts_send"])).strftime(
            "%Y-%m-%d"
        )
        
        if trade_variables == {}:
            trade_variables[this_candle_close_ts_ymdhms] = init_trade_variables
        else:
            trade_variables[this_candle_close_ts_ymdhms] = trade_variables[prev_candle_close_ts_ymdhms]

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

            # try:

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
            ).sort_values(by=["ts_create_utc"])

            indicator_base_df["dt"] = pd.date_range(
                start="1/1/2018", periods=len(indicator_base_df)
            )

            indicator_base_df = indicator_base_df.iloc[-179:]
            indicator_df = indicator_df.iloc[-179:]

            quotes = get_quotes(indicator_base_df)
            target_indicators = ["macd", "stoch", "sma5", "sma10", "sma30", "sma60"]
            indicators = get_indicators(quotes, target_indicators)

            # OHLC data
            try:
                message = {
                    "open": float(base_message["open"]),
                    "high": float(base_message["high"]),
                    "low": float(base_message["low"]),
                    "close": float(base_message["close"]),
                    "sma5": float(indicators["sma5"][-1].sma),
                    "sma10": float(indicators["sma10"][-1].sma),
                    "sma30": float(indicators["sma30"][-1].sma),
                    "sma60": float(indicators["sma60"][-1].sma),
                    "macd": float(indicators["macd"][-1].macd),
                    "macd_signal": float(indicators["macd"][-1].signal),
                    "stoch_d": float(indicators["stoch"][-1].d),
                    "stoch_k": float(indicators["stoch"][-1].k),
                    "stoch_j": float(indicators["stoch"][-1].j),
                    "ts_create_utc": str(ts_create_utc),
                    "dt": dt_today,
                }
            except:
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
                    "dt": dt_today,
                }

            indicator_df = indicator_df.append(message, ignore_index=True)
            indicator_df["ema30_5_span_grad"] = calculate_gradient(
                indicator_df, column_name="sma30", span=5
            )
            indicator_df["macd_5_span_grad"] = calculate_gradient(
                indicator_df, column_name="macd", span=5
            )
            indicator_df["macd_signal_5_span_grad"] = calculate_gradient(
                indicator_df, column_name="macd_signal", span=5
            )

            print(indicator_df)

            message["ema30_5_span_grad"] = indicator_df["ema30_5_span_grad"].iloc[-1]
            message["macd_5_span_grad"] = indicator_df["ema30_5_span_grad"].iloc[-1]
            message["macd_signal_5_span_grad"] = indicator_df["ema30_5_span_grad"].iloc[-1]

            print()
            print(message)
            print()

            # trade
            if prev_message != None and None not in message.values() and len(indicator_df) == 180:
                trade_result_df, trade_variables = trade_main(indicator_df, trade_variables)
                trade_result = dict(trade_result_df.iloc[-1])
                trade_result = {key: str(val) for key, val in trade_result.items()}
                print("trade_result:", trade_result)

                # Publish the message
                produce_minute_data(producer, trade_result, key=base_message["id"])

            prev_message = message

            # except:
            #     pass

        latest_minute_data[crypto_id] = consumed_data

        prev_candle_close_ts_ymdhm[crypto_id] = this_candle_close_ts_ymdhm
        prev_candle_close_ts_ymdhms = this_candle_close_ts_ymdhms


def produce_minute_data(producer, message, key):

    topic_name = "crypto.candles_minute_test_2_v2"
    num_partitions = 3

    producer.produce_message(topic_name, json.dumps(message), num_partitions, key=key)
    producer.poll_message(timeout=10)


def trade_main(indicators_df, trade_variables):
    start_idx_from = 100

    ########################
    # List objects to save trading results
    ########################
    transaction_type = [None for i in range(start_idx_from)]
    transaction_price = [None for i in range(start_idx_from)]
    transaction_current_gain = [None for i in range(start_idx_from)]
    invest_amount = [None for i in range(start_idx_from)]
    profits = [0 for i in range(start_idx_from)]
    accumulate_gain_in_a_day = [0 for i in range(start_idx_from)]
    stop_trade_flg = [None for i in range(start_idx_from)]
    trade_count = [0 for i in range(start_idx_from)]
    macd_golden_cross = [None for i in range(start_idx_from)]
    macd_dead_cross = [None for i in range(start_idx_from)]
    macd_all_dead_cross = [None for i in range(start_idx_from)]
    stoch_golden_cross = [None for i in range(start_idx_from)]
    stoch_dead_cross = [None for i in range(start_idx_from)]
    stoch_all_dead_cross = [None for i in range(start_idx_from)]
    sold = [None for i in range(start_idx_from)]
    bought = [None for i in range(start_idx_from)]

    # ########################
    # # Init variable
    # ########################
    # current_invest_amount = 1000000  # initial price to invest
    # is_long_position = False  # True if holding crypto
    # long_position_period = 0  # how many period (N minutes) we keep holding crypto
    # purchase_price = None  # purchased crypto price
    # stop_trading_today = False
    # prev_gain = 0.0
    # current_gain = 0.0
    # total_profit_in_a_day = 0.0
    # previous_date = "2000-01-01"  # initialize with a very old day
    # macd_dead_cross_happened = False

    # ########################
    # # Common Variables
    # ########################
    # acceptable_max_total_gain_loss_in_one_day = (
    #     -1.0
    # )  # if I lose X% of my money, stop trading in the day.
    # acceptable_max_total_profit_loss_ratio_in_one_day = (
    #     -0.005  # if I lose X yen, stop trading in the day.
    # )
    # cross_span = 1  # used to calculate golden cross and dead cross
    # macd_golden_cross_threshold = 5.0  # Threshold for MACD Golden Cross (macd < ${macd_golden_cross_threshold} -> golden cross, else dead cross)
    # stoch_golden_cross_threshold = 25.0  # Threshold for Stoch Golden Cross (stoch_k/D < ${stoch_golden_cross_threshold} -> golden cross)
    # num_bins_to_check_macd_golden_cross = (
    #     1  # used to check if there is a macd golden cross in the recent N period (minute)
    # )
    # num_bins_to_check_stoch_golden_cross = (
    #     9  # used to check if there is a stoch golden cross in the recent N period (minute)
    # )
    # num_bins_to_check_dead_cross = (
    #     3  # used to check if there is a dead cross in the recent N period (minute)
    # )

    # macd_signal_grad_threshold = 0.1

    # ########################
    # # Variables for checking buying condition function
    # ########################
    # grad_threshold = -3.0  # if <current_grad> > ${grad_threshold}, then buy
    # acceptable_gain_loss = 0.1  # If you hold long position, you might lose but losing ${acceptable_gain_loss}% is acceptable
    # num_bins_to_check_stoch_dead_cross_for_buy = 5

    # ########################
    # # Variables for checking selling condition function
    # ########################
    # take_profit_threshold = 0.3  # %
    # cut_loss_threshold = -0.1  # %
    # long_position_period_threshold = 50  # holding long position at most 30 minutes
    # selling_stoch_threshold = (
    #     65.0  # sell position when stoch value is under ${selling_stoch_threshold}
    # )

    for i in range(start_idx_from, len(indicators_df)):
        today = str(indicators_df["dt"][i])
        ts_create_utc = str(indicators_df["ts_create_utc"][i-1])
        current_price = indicators_df["close"][i]
        current_invest_amount = trade_variables[ts_create_utc]["current_invest_amount"]

        acceptable_max_total_profit_loss_in_one_day = (
            current_invest_amount
            * trade_variables[ts_create_utc]["acceptable_max_total_profit_loss_ratio_in_one_day"]
        )

        if trade_variables[ts_create_utc]["previous_date"] != today:
            trade_variables[ts_create_utc]["stop_trading_today"] = False
            initial_invest_amount_today = current_invest_amount
            trade_variables[ts_create_utc]["total_profit_in_a_day"] = 0.0
            total_gain_in_a_day = calculate_gain(initial_invest_amount_today, current_invest_amount)
        else:
            total_gain_in_a_day = 0.0

        accumulate_gain_in_a_day.append(total_gain_in_a_day)
        if trade_variables[ts_create_utc]["stop_trading_today"] == False and (
            total_gain_in_a_day < trade_variables[ts_create_utc]["acceptable_max_total_gain_loss_in_one_day"]
            or trade_variables[ts_create_utc]["total_profit_in_a_day"] < acceptable_max_total_profit_loss_in_one_day
        ):
            stop_trade_flg.append(1)
            trade_variables[ts_create_utc]["stop_trading_today"] = True
            print("stop trading:", today)
        else:
            stop_trade_flg.append(0)

        if trade_variables[ts_create_utc]["stop_trading_today"]:
            # Stop trading today since the total loss is huge !!!
            transaction_type.append("stop_trading")
            transaction_price.append(None)
            transaction_current_gain.append(0.0)
            invest_amount.append(current_invest_amount)
            profits.append(None)
            trade_count.append(0)
            macd_golden_cross.append(None)
            macd_dead_cross.append(None)
            macd_all_dead_cross.append(None)
            stoch_golden_cross.append(None)
            stoch_dead_cross.append(None)
            stoch_all_dead_cross.append(None)
            sold.append(None)
            bought.append(None)
        else:
            ##############
            # MACD
            ##############
            macd = indicators_df["macd"][i]
            macd_signal = indicators_df["macd_signal"][i]

            # check golden cross
            if (
                macd < trade_variables[ts_create_utc]["macd_golden_cross_threshold"]
                and indicators_df["macd"][i - trade_variables[ts_create_utc]["cross_span"]]
                <= indicators_df["macd_signal"][i - trade_variables[ts_create_utc]["cross_span"]]
                and indicators_df["macd"][i] >= indicators_df["macd_signal"][i]
            ):
                macd_golden_cross.append(1)
            else:
                macd_golden_cross.append(0)

            # check dead cross
            if (
                macd >= trade_variables[ts_create_utc]["macd_golden_cross_threshold"]
                and indicators_df["macd"][i - trade_variables[ts_create_utc]["cross_span"]]
                >= indicators_df["macd_signal"][i - trade_variables[ts_create_utc]["cross_span"]]
                and indicators_df["macd"][i] <= indicators_df["macd_signal"][i]
            ):
                macd_dead_cross.append(1)
            else:
                macd_dead_cross.append(0)

            # check all dead crosses, not using "stoch_golden_cross_threshold"
            if (
                indicators_df["macd"][i - trade_variables[ts_create_utc]["cross_span"]]
                >= indicators_df["macd_signal"][i - trade_variables[ts_create_utc]["cross_span"]]
                and indicators_df["macd"][i] <= indicators_df["macd_signal"][i]
            ):
                macd_all_dead_cross.append(1)
            else:
                macd_all_dead_cross.append(0)

            ##############
            # Stochastic
            ##############
            stoch_d = indicators_df["stoch_d"][i]
            stoch_k = indicators_df["stoch_k"][i]

            # check golden cross
            if (
                (stoch_k < trade_variables[ts_create_utc]["stoch_golden_cross_threshold"] or stoch_d < trade_variables[ts_create_utc]["stoch_golden_cross_threshold"])
                and indicators_df["stoch_k"][i - trade_variables[ts_create_utc]["cross_span"]]
                <= indicators_df["stoch_d"][i - trade_variables[ts_create_utc]["cross_span"]]
                and indicators_df["stoch_k"][i] >= indicators_df["stoch_d"][i]
            ):
                stoch_golden_cross.append(1)
            else:
                stoch_golden_cross.append(0)

            # check dead cross
            if (
                (
                    stoch_k > 100.0 - trade_variables[ts_create_utc]["stoch_golden_cross_threshold"]
                    or stoch_d > 100.0 - trade_variables[ts_create_utc]["stoch_golden_cross_threshold"]
                )
                and indicators_df["stoch_k"][i - trade_variables[ts_create_utc]["cross_span"]]
                >= indicators_df["stoch_d"][i - trade_variables[ts_create_utc]["cross_span"]]
                and indicators_df["stoch_k"][i] <= indicators_df["stoch_d"][i]
            ):
                stoch_dead_cross.append(1)
            else:
                stoch_dead_cross.append(0)

            # check all dead crosses, not using "stoch_golden_cross_threshold"
            if (
                indicators_df["stoch_k"][i - trade_variables[ts_create_utc]["cross_span"]]
                >= indicators_df["stoch_d"][i - trade_variables[ts_create_utc]["cross_span"]]
                and indicators_df["stoch_k"][i] <= indicators_df["stoch_d"][i]
            ):
                stoch_all_dead_cross.append(1)
            else:
                stoch_all_dead_cross.append(0)

            # # Gradient
            ema30_5_span_grad = indicators_df["ema30_5_span_grad"][i]

            if trade_variables[ts_create_utc]["is_long_position"]:
                if trade_variables[ts_create_utc]["macd_dead_cross_happened"] == False and macd_all_dead_cross[-1] == 1:
                    trade_variables[ts_create_utc]["macd_dead_cross_happened"] = True

                # consider if now is the time to sell

                trade_variables[ts_create_utc]["long_position_period"] += 1

                # calculate current gain
                trade_variables[ts_create_utc]["current_gain"] = calculate_gain(trade_variables[ts_create_utc]["purchase_price"], current_price)
                transaction_current_gain.append(trade_variables[ts_create_utc]["current_gain"])

                should_sell = check_sell_condition(
                    trade_variables[ts_create_utc]["take_profit_threshold"],
                    trade_variables[ts_create_utc]["cut_loss_threshold"],
                    trade_variables[ts_create_utc]["acceptable_gain_loss"],
                    trade_variables[ts_create_utc]["prev_gain"],
                    trade_variables[ts_create_utc]["current_gain"],
                    trade_variables[ts_create_utc]["long_position_period"],
                    trade_variables[ts_create_utc]["long_position_period_threshold"],
                    trade_variables[ts_create_utc]["macd_dead_cross_happened"],
                    macd,
                    macd_signal,
                    trade_variables[ts_create_utc]["macd_golden_cross_threshold"],
                    stoch_k,
                    stoch_d,
                    trade_variables[ts_create_utc]["selling_stoch_threshold"],
                    macd_dead_cross[-trade_variables[ts_create_utc]["num_bins_to_check_dead_cross"]:],
                    macd_all_dead_cross[-trade_variables[ts_create_utc]["num_bins_to_check_dead_cross"]:],
                    stoch_dead_cross[-trade_variables[ts_create_utc]["num_bins_to_check_dead_cross"]:],
                )

                if should_sell:
                    # sell
                    trade_variables[ts_create_utc]["is_long_position"] = False
                    transaction_type.append("sell")
                    sold.append(current_price)
                    transaction_price.append(current_price)
                    trade_variables[ts_create_utc]["purchase_price"] = None

                    pre_current_invest_amount = current_invest_amount
                    current_invest_amount = current_invest_amount * (1.0 + (0.01 * trade_variables[ts_create_utc]["current_gain"]))
                    trade_variables[ts_create_utc]["current_invest_amount"] = current_invest_amount

                    # Calc profit
                    this_profit = current_invest_amount - pre_current_invest_amount
                    profits.append(this_profit)
                    trade_variables[ts_create_utc]["total_profit_in_a_day"] += this_profit

                    # Done trading
                    trade_count.append(1)

                    # initialize to 0
                    trade_variables[ts_create_utc]["long_position_period"] = 0

                    trade_variables[ts_create_utc]["macd_dead_cross_happened"] = False
                else:
                    # not sell now
                    transaction_type.append("long_pos")
                    sold.append(None)
                    transaction_price.append(None)
                    profits.append(None)  # no profit
                    trade_count.append(0)  # no trade
                bought.append(None)
            else:
                trade_variables[ts_create_utc]["macd_dead_cross_happened"] = False

                # consider if now is the time to buy
                should_buy = check_buy_condition(
                    trade_variables[ts_create_utc]["grad_threshold"],
                    ema30_5_span_grad,
                    macd_golden_cross[-trade_variables[ts_create_utc]["num_bins_to_check_macd_golden_cross"]:],
                    stoch_golden_cross[-trade_variables[ts_create_utc]["num_bins_to_check_stoch_golden_cross"]:],
                    stoch_all_dead_cross[-trade_variables[ts_create_utc]["num_bins_to_check_stoch_dead_cross_for_buy"]:],
                    indicators_df["macd_5_span_grad"][i],
                    indicators_df["macd_signal_5_span_grad"][i],
                    trade_variables[ts_create_utc]["macd_signal_grad_threshold"],
                )
                if should_buy:
                    # buy
                    trade_variables[ts_create_utc]["is_long_position"] = True
                    transaction_type.append("buy")
                    bought.append(current_price)
                    transaction_price.append(current_price)
                    transaction_current_gain.append(0.0)
                    trade_variables[ts_create_utc]["purchase_price"] = current_price
                else:
                    # not buy now
                    transaction_type.append("no_entry")
                    bought.append(None)
                    transaction_price.append(None)
                    transaction_current_gain.append(None)
                sold.append(None)
                profits.append(None)  # no profit
                trade_count.append(0)  # no trade

            invest_amount.append(current_invest_amount)

            trade_variables[ts_create_utc]["prev_gain"] = trade_variables[ts_create_utc]["current_gain"]

        trade_variables[ts_create_utc]["previous_date"] = today

    traded_result_dict = {}

    cnt = 0
    for (
        trans_type,
        price,
        gain,
        macd_gc,
        macd_dc,
        stoch_gc,
        stoch_dc,
        stoch_all_dc,
        bought_price,
        sold_price,
        my_money,
        accumulate_gain,
        stop_trade,
        profit,
        trade_cnt,
    ) in zip(
        transaction_type,
        transaction_price,
        transaction_current_gain,
        macd_golden_cross,
        macd_dead_cross,
        stoch_golden_cross,
        stoch_dead_cross,
        stoch_all_dead_cross,
        bought,
        sold,
        invest_amount,
        accumulate_gain_in_a_day,
        stop_trade_flg,
        profits,
        trade_count,
    ):
        traded_result_dict[cnt] = [
            trans_type,
            price,
            gain,
            macd_gc,
            macd_dc,
            stoch_gc,
            stoch_dc,
            stoch_all_dc,
            bought_price,
            sold_price,
            my_money,
            accumulate_gain,
            stop_trade,
            profit,
            trade_cnt,
        ]
        cnt += 1

    target_columns = [
        "transaction_type",
        "transaction_price",
        "transaction_current_gain",
        "macd_golden_cross",
        "macd_dead_cross",
        "stoch_golden_cross",
        "stoch_dead_cross",
        "stoch_all_dead_cross",
        "bought_price",
        "sold_price",
        "my_money",
        "accumulate_gain",
        "stop_trade",
        "profit",
        "trade_count",
    ]
    traded_result_df = pd.DataFrame.from_dict(
        traded_result_dict, orient="index", columns=target_columns
    )

    traded_result_df.index = list(indicators_df.index)
    final_df = pd.merge(indicators_df, traded_result_df, left_index=True, right_index=True)

    return final_df, trade_variables


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

    init_trade_variables = {
        ########################
        # Init variable
        ########################
        "current_invest_amount": 1000000,  # initial price to invest
        "is_long_position": False,  # True if holding crypto
        "long_position_period": 0,  # how many period (N minutes) we keep holding crypto
        "purchase_price": None,  # purchased crypto price
        "stop_trading_today": False,
        "prev_gain": 0.0,
        "current_gain": 0.0,
        "total_profit_in_a_day": 0.0,
        "previous_date": "2000-01-01",  # initialize with a very old day
        "macd_dead_cross_happened": False,
        ########################
        # Common Variables
        ########################
        "acceptable_max_total_gain_loss_in_one_day": (
            -1.0
        ),  # if I lose X% of my money, stop trading in the day.
        "acceptable_max_total_profit_loss_ratio_in_one_day": (
            -0.005  # if I lose X yen, stop trading in the day.
        ),
        "cross_span": 1,  # used to calculate golden cross and dead cross
        "macd_golden_cross_threshold": 5.0,  # Threshold for MACD Golden Cross (macd < ${macd_golden_cross_threshold} -> golden cross, else dead cross)
        "stoch_golden_cross_threshold": 25.0,  # Threshold for Stoch Golden Cross (stoch_k/D < ${stoch_golden_cross_threshold} -> golden cross)
        "num_bins_to_check_macd_golden_cross": (
            1  # used to check if there is a macd golden cross in the recent N period (minute)
        ),
        "num_bins_to_check_stoch_golden_cross": (
            9  # used to check if there is a stoch golden cross in the recent N period (minute)
        ),
        "num_bins_to_check_dead_cross": (
            3  # used to check if there is a dead cross in the recent N period (minute)
        ),
        "macd_signal_grad_threshold": 0.1,
        ########################
        # Variables for checking buying condition function
        ########################
        "grad_threshold": -3.0,  # if <current_grad> > ${grad_threshold}, then buy
        "acceptable_gain_loss": 0.1,  # If you hold long position, you might lose but losing ${acceptable_gain_loss}% is acceptable
        "num_bins_to_check_stoch_dead_cross_for_buy": 5,
        ########################
        # Variables for checking selling condition function
        ########################
        "take_profit_threshold": 0.3,  # %
        "cut_loss_threshold": -0.1,  # %
        "long_position_period_threshold": 50,  # holding long position at most 30 minutes
        "selling_stoch_threshold": (
            65.0  # sell position when stoch value is under ${selling_stoch_threshold}
        ),
    }

    consume_realtime_data(curr_date, curr_timestamp, consumer_id, init_trade_variables)


if __name__ == "__main__":
    main()
