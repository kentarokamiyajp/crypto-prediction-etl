"""
1. Consume topic from the existing topic 'crypto.candles_minute'
2. Aggregation by grouping by "minute"
3. Produce the aggregated data to a new topic
"""

import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import time
import pytz
import pandas as pd
from consumer_operation import KafkaConsumer
from dotenv import load_dotenv
from datetime import datetime, timedelta
from common.indicators import get_indicators, get_quotes
from pprint import pprint
from kafka_producers.KafkaBaseProducer import KafkaBaseProducer

CONF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "conf")

# set the timezone to US/Pacific
os.environ["TZ"] = "Asia/Tokyo"
time.tzset()
TZ_JST = pytz.timezone("Asia/Tokyo")

TRADE_RESULT = pd.DataFrame()


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
    return ((current_price / past_price) - 1.0) * 100.0


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
    stoch_k,
    stoch_d,
    selling_stoch_threshold,
    macd_dead_cross,
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
    ):
        return True

    return False


def consume_realtime_data(curr_date, curr_timestamp, consumer_id):

    # Kafka config
    topic_id = os.environ.get("TOPIC_ID")
    # group_id = os.environ.get("GROUP_ID")
    group_id = "realtime_trading_bot_v2_" + datetime.now().strftime("%Y%m%d%H%M")
    offset_type = os.environ.get("OFFSET_TYPE")

    # Create consumer
    consumer = KafkaConsumer(curr_date, curr_timestamp, consumer_id, group_id, offset_type)

    # Create producer
    producer = KafkaBaseProducer()

    # Get the topic's partitions
    partitions = consumer.get_partitions(topic_id)
    num_partitions = len(partitions)

    # init variables
    prev_candle_minute_data = {}
    prev_candle_close_ts_ymdhm = "2099-01-01 00:00"
    prev_candle_close_ts_ymdhms = None
    indicator_base_df = pd.DataFrame(
        columns=["open", "high", "low", "close", "amount", "ts_create_utc", "dt"]
    )
    indicators_df = pd.DataFrame(
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

        curr_candle_close_ts_ymdhm = datetime.utcfromtimestamp(
            int(consumed_data["data"][0]["closeTime"])
        ).strftime("%Y-%m-%d %H:%M")
        curr_candle_close_ts_ymdhms = datetime.utcfromtimestamp(
            int(consumed_data["data"][0]["closeTime"])
        ).strftime("%Y-%m-%d %H:%M:%S")
        this_candle_ts_send_ymdhms = datetime.utcfromtimestamp(
            int(consumed_data["data"][0]["ts_send"])
        ).strftime("%Y-%m-%d %H:%M:%S")
        dt_today = datetime.utcfromtimestamp(int(consumed_data["data"][0]["ts_send"])).strftime(
            "%Y-%m-%d"
        )

        print(
            f"{crypto_id}: this_candle_ts_send_ymdhms -> {this_candle_ts_send_ymdhms}, curr_candle_close_ts_ymdhms -> {curr_candle_close_ts_ymdhms} ({this_open}, {this_high}, {this_low}, {this_close})"
        )

        # # Skip past date
        # ts_now = datetime.now()
        # N_minus = 6
        # st_now_minus_N_hours = ts_now - timedelta(hours=N_minus + 9)
        # if curr_candle_close_ts_ymdhms < st_now_minus_N_hours.strftime("%Y-%m-%d %H:%M:%S"):
        #     continue

        ########################
        # MAIN PART to get indicators
        ########################
        # Publish candle data to kafka topic if it meets the following conditions
        if prev_candle_minute_data != {}:

            base_message = prev_candle_minute_data["data"][0]
            ts_create_utc = datetime.utcfromtimestamp(int(base_message["closeTime"]))

            if not (indicator_base_df.empty):
                if str(indicator_base_df.iloc[-1]["ts_create_utc"]) == str(ts_create_utc):
                    indicator_base_df = indicator_base_df.iloc[:-1]
                    indicators_df = indicators_df.iloc[:-1]

                # Show result every minute -> str(indicator_base_df.iloc[-1]["ts_create_utc"]) != str(ts_create_utc)
                else:
                    # print(indicator_base_df)
                    # print(indicators_df)
                    print(
                        TRADE_RESULT[
                            [
                                "ts_create_utc",
                                "open",
                                "high",
                                "low",
                                "close",
                                "gain_is_negative_from_prev",
                                "sma5",
                                "macd",
                                "macd_signal",
                                "stoch_d",
                                "stoch_k",
                                "my_money",
                            ]
                        ]
                    )

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
                    "ts_send_utc": str(this_candle_ts_send_ymdhms),
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
                    "ts_send_utc": str(this_candle_ts_send_ymdhms),
                    "dt": dt_today,
                }

            if len(indicators_df) == 0:
                indicators_df = indicators_df.append(message, ignore_index=True)
            else:
                indicators_df = indicators_df.append(message, ignore_index=True)

            indicators_df["ema30_5_span_grad"] = calculate_gradient(
                indicators_df, column_name="sma30", span=5
            )
            indicators_df["macd_5_span_grad"] = calculate_gradient(
                indicators_df, column_name="macd", span=5
            )
            indicators_df["macd_signal_5_span_grad"] = calculate_gradient(
                indicators_df, column_name="macd_signal", span=5
            )

            # print(indicators_df)

            message["ema30_5_span_grad"] = indicators_df["ema30_5_span_grad"].iloc[-1]
            message["macd_5_span_grad"] = indicators_df["ema30_5_span_grad"].iloc[-1]
            message["macd_signal_5_span_grad"] = indicators_df["ema30_5_span_grad"].iloc[-1]

            # trade
            _trade_result = trade_main(indicators_df)

            # Publish the message
            trade_result = {k: str(v) for k, v in _trade_result.items()}
            # pprint(trade_result)
            produce_minute_data(producer, trade_result, key=base_message["id"])

            # prev_message = message

            # keep only latest 200 records
            indicator_base_df = indicator_base_df.iloc[-200:]
            indicators_df = indicators_df.iloc[-200:]

        prev_candle_minute_data = consumed_data


def produce_minute_data(producer, message, key):

    topic_name = "crypto.realtime_trading_bot_v2"
    num_partitions = 1

    producer.produce_message(topic_name, json.dumps(message), num_partitions, key=key)
    producer.poll_message(timeout=10)


def trade_main(indicators_df):
    global TRADE_RESULT
    save_data = False
    trade_result = {
        "initial_invest_amount_today": 1000000,
        "invest_amount": 1000000,
        "transaction_type": None,
        "transaction_price": -99.9,
        "transaction_current_gain": 0.0,
        "profits": 0.0,
        "accumulate_gain_in_a_day": None,
        "stop_trade_flg": None,
        "macd_golden_cross": 0,
        "macd_dead_cross": 0,
        "continuous_macd_golden_cross_count": 0,
        "stoch_golden_cross": 0,
        "stoch_dead_cross": 0,
        "stoch_all_dead_cross": 0,
        "bought_price": -99.9,
        "sold_price": -99.9,
        "my_money": -99.9,
        "trade_count": 0,
        "is_long_position": False,
        "long_position_period": 0,
        "purchase_price": -99.9,
        "stop_trading_this_minute": False,
        "stop_trading_today": False,
        "prev_gain": 0.0,
        "current_gain": 0.0,
        "total_profit_in_a_day": 0.0,
        "gain_is_negative_from_prev": 0,
    }

    all_columns = list(indicators_df.columns) + list(trade_result.keys())

    ########################
    # Common Variables
    ########################
    acceptable_max_total_gain_loss_in_one_day = (
        -1.0
    )  # if I lose X% of my money, stop trading in the day.
    acceptable_max_total_profit_loss_ratio_in_one_day = (
        -0.005  # if I lose X yen, stop trading in the day.
    )
    cross_span = 1  # used to calculate golden cross and dead cross
    macd_golden_cross_threshold = 5.0  # Threshold for MACD Golden Cross (macd < ${macd_golden_cross_threshold} -> golden cross, else dead cross)
    stoch_golden_cross_threshold = 25.0  # Threshold for Stoch Golden Cross (stoch_k/D < ${stoch_golden_cross_threshold} -> golden cross)
    num_bins_to_check_macd_golden_cross = (
        1  # used to check if there is a macd golden cross in the recent N period (minute)
    )
    num_bins_to_check_stoch_golden_cross = (
        9  # used to check if there is a stoch golden cross in the recent N period (minute)
    )
    num_bins_to_check_dead_cross = (
        3  # used to check if there is a dead cross in the recent N period (minute)
    )

    macd_signal_grad_threshold = 0.1

    ########################
    # Variables for checking buying condition function
    ########################
    grad_threshold = -3.0  # if <current_grad> > ${grad_threshold}, then buy
    acceptable_gain_loss = 0.1  # If you hold long position, you might lose but losing ${acceptable_gain_loss}% is acceptable
    num_bins_to_check_stoch_dead_cross_for_buy = 5

    ########################
    # Variables for checking selling condition function
    ########################
    take_profit_threshold = 0.3  # %
    cut_loss_threshold = -0.1  # %
    long_position_period_threshold = 50  # holding long position at most 30 minutes
    selling_stoch_threshold = (
        65.0  # sell position when stoch value is under ${selling_stoch_threshold}
    )

    # Not trading because it's initial step
    if TRADE_RESULT.empty:
        print("TRADE_RESULT is empty so skip trading this time !!!")
        trade_result.update(indicators_df.iloc[-1])
        TRADE_RESULT = pd.DataFrame.from_dict(
            {0: [trade_result[col] for col in all_columns]}, orient="index", columns=all_columns
        )
    # Not trading because less data to use accurate indicators
    elif len(TRADE_RESULT) < 180:
        print("Too few data for trading: len=", len(TRADE_RESULT))
        if str(indicators_df.iloc[-1]["ts_create_utc"]) == str(
            TRADE_RESULT.iloc[-1]["ts_create_utc"]
        ):
            TRADE_RESULT = TRADE_RESULT.iloc[:-1]
        trade_result.update(indicators_df.iloc[-1])
        TRADE_RESULT = TRADE_RESULT.append(trade_result, ignore_index=True).sort_values(
            by=["ts_create_utc"]
        )

    elif (
        len(TRADE_RESULT) >= 180
        and str(indicators_df.iloc[-1]["ts_create_utc"])
        == str(TRADE_RESULT.iloc[-1]["ts_create_utc"])
        and TRADE_RESULT.iloc[-1]["stop_trading_this_minute"] == True
    ):
        print(
            "Stop trading in this minute:",
            str(indicators_df.iloc[-1]["ts_create_utc"]),
            str(TRADE_RESULT.iloc[-1]["ts_create_utc"]),
        )
        # If now in log position, no trading in this minute, waiting for the next minute
        # E.g., if we buy at 23:10:09, we don't sell from 23:10:09 to 23:10:59. We can sell after 23:11:00.
        new_trade_result = {}
        pre_TRADE_RESULT = dict(TRADE_RESULT.iloc[-1])
        for col in trade_result.keys():
            new_trade_result[col] = pre_TRADE_RESULT[col]
        new_trade_result.update(indicators_df.iloc[-1])

        ##############
        # Custom indicator
        ##############
        if indicators_df["close"].iloc[-1] - pre_TRADE_RESULT["close"] >= 0:
            # positive gain
            new_trade_result["gain_is_negative_from_prev"] = 0
        else:
            # negative gain
            new_trade_result["gain_is_negative_from_prev"] = (
                pre_TRADE_RESULT["gain_is_negative_from_prev"] + 1
            )

        # reset values
        new_trade_result["continuous_macd_golden_cross_count"] = 0
        new_trade_result["long_position_period"] = 0

        if str(pre_TRADE_RESULT["dt"]) != str(indicators_df["dt"].iloc[-1]):
            new_trade_result["total_profit_in_a_day"] = 0.0

        TRADE_RESULT = TRADE_RESULT.iloc[:-1]
        TRADE_RESULT = TRADE_RESULT.append(new_trade_result, ignore_index=True).sort_values(
            by=["ts_create_utc"]
        )

        print(
            TRADE_RESULT.iloc[-2:][
                [
                    "ts_create_utc",
                    "close",
                    "macd_golden_cross",
                    "macd_dead_cross",
                    "continuous_macd_golden_cross_count",
                    "transaction_type",
                    "long_position_period",
                    "gain_is_negative_from_prev",
                    "bought_price",
                    "sold_price",
                    "current_gain",
                    "total_profit_in_a_day",
                    "my_money",
                ]
            ]
        )

    # Start Trading
    else:
        print("Start Trading !!!")
        historical_macd_dead_cross = list(TRADE_RESULT["macd_dead_cross"])
        historical_stoch_dead_cross = list(TRADE_RESULT["stoch_dead_cross"])
        historical_macd_golden_cross = list(TRADE_RESULT["macd_golden_cross"])
        historical_stoch_golden_cross = list(TRADE_RESULT["stoch_golden_cross"])
        historical_stoch_all_dead_cross = list(TRADE_RESULT["stoch_all_dead_cross"])

        # Get trade history in the one previous trade
        pre_TRADE_RESULT = dict(TRADE_RESULT.iloc[-1])
        previous_date = pre_TRADE_RESULT["dt"]
        current_invest_amount = float(pre_TRADE_RESULT["invest_amount"])
        is_long_position = bool(pre_TRADE_RESULT["is_long_position"])
        long_position_period = int(pre_TRADE_RESULT["long_position_period"])
        purchase_price = float(pre_TRADE_RESULT["purchase_price"])
        stop_trading_today = bool(pre_TRADE_RESULT["stop_trading_today"])
        prev_gain = float(pre_TRADE_RESULT["prev_gain"])
        current_gain = float(pre_TRADE_RESULT["current_gain"])
        total_profit_in_a_day = float(pre_TRADE_RESULT["total_profit_in_a_day"])
        initial_invest_amount_today = float(pre_TRADE_RESULT["initial_invest_amount_today"])
        transaction_type = pre_TRADE_RESULT["transaction_type"]
        stop_trading_this_minute = pre_TRADE_RESULT["stop_trading_this_minute"]
        gain_is_negative_from_prev = pre_TRADE_RESULT["gain_is_negative_from_prev"]
        bought_price = pre_TRADE_RESULT["bought_price"]
        sold_price = pre_TRADE_RESULT["sold_price"]
        continuous_macd_golden_cross_count = pre_TRADE_RESULT["continuous_macd_golden_cross_count"]

        today = str(indicators_df["dt"].iloc[-1])
        current_price = float(indicators_df["close"].iloc[-1])

        if previous_date != today:
            print("New Day !!!")
            stop_trading_today = False
            initial_invest_amount_today = current_invest_amount
            total_profit_in_a_day = 0.0
        else:
            initial_invest_amount_today = initial_invest_amount_today

        acceptable_max_total_profit_loss_in_one_day = (
            current_invest_amount * acceptable_max_total_profit_loss_ratio_in_one_day
        )

        total_gain_in_a_day = calculate_gain(initial_invest_amount_today, current_invest_amount)

        accumulate_gain_in_a_day = total_gain_in_a_day
        if stop_trading_today == False and (
            total_gain_in_a_day < acceptable_max_total_gain_loss_in_one_day
            or total_profit_in_a_day < acceptable_max_total_profit_loss_in_one_day
        ):
            stop_trade_flg = 1
            stop_trading_today = True
            print("stop trading:", today)
        else:
            stop_trade_flg = 0

        if stop_trading_today:
            # Stop trading today since the total loss is huge !!!
            transaction_type = "stop_trading"
            transaction_price = -99.9
            transaction_current_gain = 0.0
            # invest_amount = current_invest_amount
            profits = 0.0
            trade_count = 0
            macd_golden_cross = 0
            macd_dead_cross = 0
            stoch_golden_cross = 0
            stoch_dead_cross = 0
            stoch_all_dead_cross = 0
            gain_is_negative_from_prev = 0  # 0: zero gain but treats as positive
            continuous_macd_golden_cross_count = 0
        else:
            ##############
            # Custom indicator
            ##############
            # if indicators_df["close"].iloc[-1] - pre_TRADE_RESULT["close"] >= 0:
            if float(indicators_df["close"].iloc[-1]) - float(indicators_df["close"].iloc[-2]) >= 0:
                # positive gain
                gain_is_negative_from_prev = 0
            else:
                # negative gain
                gain_is_negative_from_prev += 1

            ##############
            # MACD
            ##############
            macd = float(indicators_df["macd"].iloc[-1])

            # check golden cross
            if (
                macd < macd_golden_cross_threshold
                and float(pre_TRADE_RESULT["macd"]) < float(pre_TRADE_RESULT["macd_signal"])
                and float(indicators_df["macd"].iloc[-1])
                > float(indicators_df["macd_signal"].iloc[-1])
            ):
                macd_golden_cross = 1
                historical_macd_golden_cross.append(1)
            else:
                macd_golden_cross = 0
                historical_macd_golden_cross.append(0)

            if macd_golden_cross == 1:
                continuous_macd_golden_cross_count = 1
            else:
                if continuous_macd_golden_cross_count > 0:
                    if float(indicators_df["macd"].iloc[-1]) > float(
                        indicators_df["macd_signal"].iloc[-1]
                    ):
                        continuous_macd_golden_cross_count += 1
                    else:
                        continuous_macd_golden_cross_count = 0
                else:
                    continuous_macd_golden_cross_count = 0

            # check dead cross
            if (
                macd >= macd_golden_cross_threshold
                and float(pre_TRADE_RESULT["macd"]) > float(pre_TRADE_RESULT["macd_signal"])
                and float(indicators_df["macd"].iloc[-1])
                < float(indicators_df["macd_signal"].iloc[-1])
            ):
                macd_dead_cross = 1
                historical_macd_dead_cross.append(1)
            else:
                macd_dead_cross = 0
                historical_macd_dead_cross.append(0)

            # if continuous_macd_golden_cross_count == 5:
            #     historical_macd_golden_cross.append(1)
            # else:
            #     historical_macd_golden_cross.append(0)

            ##############
            # Stochastic
            ##############
            stoch_d = float(indicators_df["stoch_d"].iloc[-1])
            stoch_k = float(indicators_df["stoch_k"].iloc[-1])

            # check golden cross
            if (
                (stoch_k < stoch_golden_cross_threshold or stoch_d < stoch_golden_cross_threshold)
                and float(pre_TRADE_RESULT["stoch_k"]) < float(pre_TRADE_RESULT["stoch_d"])
                and float(indicators_df["stoch_k"].iloc[-1])
                > float(indicators_df["stoch_d"].iloc[-1])
            ):
                stoch_golden_cross = 1
                historical_stoch_golden_cross.append(1)
            else:
                stoch_golden_cross = 0
                historical_stoch_golden_cross.append(0)

            # check dead cross
            if (
                (
                    stoch_k > 100.0 - stoch_golden_cross_threshold
                    or stoch_d > 100.0 - stoch_golden_cross_threshold
                )
                and float(pre_TRADE_RESULT["stoch_k"]) > float(pre_TRADE_RESULT["stoch_d"])
                and float(indicators_df["stoch_k"].iloc[-1])
                < float(indicators_df["stoch_d"].iloc[-1])
            ):
                stoch_dead_cross = 1
                historical_stoch_dead_cross.append(1)
            else:
                stoch_dead_cross = 0
                historical_stoch_dead_cross.append(0)

            # check all dead crosses, not using "stoch_golden_cross_threshold"
            if float(pre_TRADE_RESULT["stoch_k"]) > float(pre_TRADE_RESULT["stoch_d"]) and float(
                indicators_df["stoch_k"].iloc[-1]
            ) < float(indicators_df["stoch_d"].iloc[-1]):
                stoch_all_dead_cross = 1
                historical_stoch_all_dead_cross.append(1)
            else:
                stoch_all_dead_cross = 0
                historical_stoch_all_dead_cross.append(0)

            # # Gradient
            ema30_5_span_grad = float(indicators_df["ema30_5_span_grad"].iloc[-1])

            if is_long_position:
                # consider if now is the time to sell

                long_position_period += 1

                # calculate current gain
                current_gain = calculate_gain(bought_price, current_price)
                transaction_current_gain = current_gain

                should_sell = check_sell_condition(
                    take_profit_threshold,
                    cut_loss_threshold,
                    acceptable_gain_loss,
                    prev_gain,
                    current_gain,
                    long_position_period,
                    long_position_period_threshold,
                    stoch_k,
                    stoch_d,
                    selling_stoch_threshold,
                    historical_macd_dead_cross[-num_bins_to_check_dead_cross:],
                    historical_stoch_dead_cross[-num_bins_to_check_dead_cross:],
                )

                if should_sell:
                    # sell
                    is_long_position = False
                    transaction_type = "sell"
                    stop_trading_this_minute = True
                    sold_price = current_price
                    transaction_price = current_price
                    purchase_price = -99.9

                    pre_current_invest_amount = current_invest_amount
                    current_invest_amount = current_invest_amount * (1.0 + (0.01 * current_gain))

                    # Calc profit
                    this_profit = current_invest_amount - pre_current_invest_amount
                    profits = this_profit
                    total_profit_in_a_day += this_profit

                    # Done trading
                    trade_count = 1

                    # initialize to 0
                    long_position_period = 0

                else:
                    # not sell now
                    transaction_type = "long_pos"
                    transaction_price = -99.9
                    profits = 0.0  # no profit
                    trade_count = 0  # no trade
            else:
                # consider if now is the time to buy
                if transaction_type == "no_entry":
                    should_buy = check_buy_condition(
                        grad_threshold,
                        ema30_5_span_grad,
                        historical_macd_golden_cross[-num_bins_to_check_macd_golden_cross:],
                        historical_stoch_golden_cross[-num_bins_to_check_stoch_golden_cross:],
                        historical_stoch_all_dead_cross[
                            -num_bins_to_check_stoch_dead_cross_for_buy:
                        ],
                        float(indicators_df["macd_5_span_grad"].iloc[-1]),
                        float(indicators_df["macd_signal_5_span_grad"].iloc[-1]),
                        macd_signal_grad_threshold,
                    )
                else:
                    # If already bought and sold in this one minute, not buying anymore at this minute.
                    should_buy = False

                if should_buy:
                    # buy
                    is_long_position = True
                    transaction_type = "buy"
                    stop_trading_this_minute = True
                    bought_price = current_price
                    transaction_price = current_price
                    transaction_current_gain = 0.0
                    purchase_price = current_price
                else:
                    # not buy now
                    transaction_type = "no_entry"
                    transaction_price = -99.9
                    transaction_current_gain = 0.0
                profits = 0.0  # no profit
                trade_count = 0  # no trade

        if str(indicators_df.iloc[-1]["ts_create_utc"]) == str(pre_TRADE_RESULT["ts_create_utc"]):
            trade_result["macd_golden_cross"] = max(
                macd_golden_cross, pre_TRADE_RESULT["macd_golden_cross"]
            )
            trade_result["macd_dead_cross"] = max(
                macd_dead_cross, pre_TRADE_RESULT["macd_dead_cross"]
            )
            trade_result["stoch_golden_cross"] = max(
                stoch_golden_cross, pre_TRADE_RESULT["stoch_golden_cross"]
            )
            trade_result["stoch_dead_cross"] = max(
                stoch_dead_cross, pre_TRADE_RESULT["stoch_dead_cross"]
            )
            trade_result["stoch_all_dead_cross"] = max(
                stoch_all_dead_cross, pre_TRADE_RESULT["stoch_all_dead_cross"]
            )
            trade_result["trade_count"] = max(trade_count, pre_TRADE_RESULT["trade_count"])

            trade_result["transaction_price"] = max(
                transaction_price, pre_TRADE_RESULT["transaction_price"]
            )

            if pre_TRADE_RESULT["purchase_price"] != -99.9:
                trade_result["purchase_price"] = pre_TRADE_RESULT["purchase_price"]
            else:
                trade_result["purchase_price"] = purchase_price

            if pre_TRADE_RESULT["transaction_current_gain"] != 0.0:
                trade_result["transaction_current_gain"] = pre_TRADE_RESULT[
                    "transaction_current_gain"
                ]
            else:
                trade_result["transaction_current_gain"] = transaction_current_gain

            if pre_TRADE_RESULT["profits"] != 0.0:
                trade_result["profits"] = pre_TRADE_RESULT["profits"]
            else:
                trade_result["profits"] = profits

            # if same day
            if pre_TRADE_RESULT["dt"] == today:
                if pre_TRADE_RESULT["stop_trading_today"] == True:
                    trade_result["stop_trading_today"] = True
                else:
                    trade_result["stop_trading_today"] = False

                if pre_TRADE_RESULT["transaction_type"] == "buy":
                    trade_result["transaction_type"] = "buy"
                elif pre_TRADE_RESULT["transaction_type"] == "sell":
                    trade_result["transaction_type"] = "sell"
                else:
                    trade_result["transaction_type"] = transaction_type

            # if not same day
            else:
                trade_result["stop_trading_today"] = False
                trade_result["transaction_type"] = "no_entry"

            if pre_TRADE_RESULT["stop_trading_this_minute"] == True:
                trade_result["stop_trading_this_minute"] = True
            else:
                trade_result["stop_trading_this_minute"] = stop_trading_this_minute

            if pre_TRADE_RESULT["transaction_type"] == "sell":
                # if we sold at this time, reset this to 0
                trade_result["long_position_period"] = 0
            else:
                # if we didn't sell at this time, keep using the previous value, not updating
                trade_result["long_position_period"] = pre_TRADE_RESULT["long_position_period"]

            TRADE_RESULT = TRADE_RESULT.iloc[:-1]
        else:
            save_data = True
            trade_result["macd_golden_cross"] = macd_golden_cross
            trade_result["macd_dead_cross"] = macd_dead_cross
            trade_result["stoch_golden_cross"] = stoch_golden_cross
            trade_result["stoch_dead_cross"] = stoch_dead_cross
            trade_result["stoch_all_dead_cross"] = stoch_all_dead_cross
            trade_result["trade_count"] = trade_count
            trade_result["purchase_price"] = purchase_price
            trade_result["transaction_type"] = transaction_type
            trade_result["transaction_current_gain"] = transaction_current_gain
            trade_result["profits"] = profits
            trade_result["stop_trading_today"] = stop_trading_today
            trade_result["long_position_period"] = long_position_period

            if pre_TRADE_RESULT["stop_trading_this_minute"] == True:
                trade_result["stop_trading_this_minute"] = False
            else:
                trade_result["stop_trading_this_minute"] = stop_trading_this_minute

            # reset
            if pre_TRADE_RESULT["transaction_type"] == "sell":
                bought_price = -99.9
                sold_price = -99.9
                current_gain = 0.0

        trade_result["bought_price"] = bought_price
        trade_result["sold_price"] = sold_price
        trade_result["invest_amount"] = current_invest_amount
        trade_result["is_long_position"] = is_long_position
        trade_result["prev_gain"] = prev_gain
        trade_result["current_gain"] = current_gain
        trade_result["total_profit_in_a_day"] = total_profit_in_a_day
        trade_result["accumulate_gain_in_a_day"] = accumulate_gain_in_a_day
        trade_result["stop_trade_flg"] = stop_trade_flg
        trade_result["my_money"] = current_invest_amount
        trade_result["initial_invest_amount_today"] = initial_invest_amount_today
        trade_result["gain_is_negative_from_prev"] = gain_is_negative_from_prev
        trade_result["continuous_macd_golden_cross_count"] = continuous_macd_golden_cross_count

        try:
            if None in list(trade_result.values()):
                print("Failed")
                print(trade_result)
        except:
            pprint(trade_result)
            exit(1)

        trade_result.update(indicators_df.iloc[-1])
        TRADE_RESULT = TRADE_RESULT.append(trade_result, ignore_index=True).sort_values(
            by=["ts_create_utc"]
        )

        print(
            TRADE_RESULT.iloc[-2:][
                [
                    "ts_create_utc",
                    "close",
                    "macd_golden_cross",
                    "macd_dead_cross",
                    "continuous_macd_golden_cross_count",
                    "transaction_type",
                    "long_position_period",
                    "gain_is_negative_from_prev",
                    "bought_price",
                    "sold_price",
                    "current_gain",
                    "total_profit_in_a_day",
                    "my_money",
                ]
            ]
        )

    TRADE_RESULT = TRADE_RESULT.iloc[-10000:]

    if len(TRADE_RESULT) % 10 == 0 and save_data:
        print("SAVING")
        TRADE_RESULT.to_csv(
            "/home/pyuser/git/crypto_prediction_dwh/script/kafka_consumers/trade_variables_v2.csv"
        )

    return dict(TRADE_RESULT.iloc[-1])


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
