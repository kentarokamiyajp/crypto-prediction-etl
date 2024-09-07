"""
1. Consume topic from the existing topic 'crypto.candles_minute'
2. Aggregation by grouping by "minute"
3. Produce the aggregated data to a new topic

indicators_df.columns = [
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
        "ts_send_utc",
        "dt_send_utc",
    ]
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
def calculate_gradient(df, span):
    if len(df) == span and df.iloc[0] != None:
        gradient = (df.iloc[-1] - df.iloc[0]) / span
    else:
        gradient = None

    return gradient


def calculate_gain(past_price, current_price):
    return round(((current_price / past_price) - 1.0) * 100.0, 6)


def check_buy_condition(
    grad_threshold,
    ema30_5_span_grad,
    macd_golden_cross,
    stoch_golden_cross,
):
    if (
        ema30_5_span_grad > grad_threshold
        and 1 == macd_golden_cross[-1]
        and 1 in stoch_golden_cross
    ):
        return True
    return False


def check_sell_condition(
    take_profit_threshold,
    cut_loss_threshold,
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
        or (current_gain < cut_loss_threshold)
        or (long_position_period > long_position_period_threshold)
        or (1 in macd_dead_cross)
        or (
            1 in stoch_dead_cross
            and (stoch_k < selling_stoch_threshold or stoch_d < selling_stoch_threshold)
        )
    ):
        return True

    return False


def trade_main(part_of_indicators_df):
    global TRADE_RESULT
    trade_result = {
        "initial_invest_amount_today": 1000000,
        "invest_amount": 1000000,
        "transaction_type": None,
        "profits": 0.0,
        "accumulate_gain_in_a_day": None,
        "macd_golden_cross": 0,
        "macd_dead_cross": 0,
        "continuous_macd_golden_cross_count": 0,
        "stoch_golden_cross": 0,
        "stoch_dead_cross": 0,
        "stoch_all_dead_cross": 0,
        "bought_price": -99.9,
        "sold_price": -99.9,
        "my_money": -99.9,
        "is_long_position": False,
        "long_position_period": 0,
        "stop_trading_now": False,
        "stop_trading_now_count": 0,
        "stop_trading_today": False,
        "prev_gain": 0.0,
        "current_gain": 0.0,
        "total_profit_in_a_day": 0.0,
        "gain_is_negative_from_prev": 0,
    }

    all_columns = list(part_of_indicators_df.columns) + list(trade_result.keys())

    if not TRADE_RESULT.empty:
        pre_TRADE_RESULT = dict(TRADE_RESULT.iloc[-1])

        stop_trading_now = bool(pre_TRADE_RESULT["stop_trading_now"])

        if str(part_of_indicators_df["dt_send_utc"].iloc[-1]) != str(
            pre_TRADE_RESULT["dt_send_utc"]
        ):
            stop_trading_today = False
        else:
            stop_trading_today = bool(pre_TRADE_RESULT["stop_trading_today"])

    # Not trading because it's initial step
    if TRADE_RESULT.empty:
        # print("TRADE_RESULT is empty so skip trading this time !!!")
        trade_result.update(part_of_indicators_df.iloc[-1])
        TRADE_RESULT = pd.DataFrame.from_dict(
            {0: [trade_result[col] for col in all_columns]}, orient="index", columns=all_columns
        )
    # Not trading because less data to use accurate indicators
    elif len(TRADE_RESULT) < 2000:
        # print("Too few data for trading: len=", len(TRADE_RESULT))
        trade_result.update(part_of_indicators_df.iloc[-1])
        TRADE_RESULT = TRADE_RESULT.append(trade_result, ignore_index=True)
    elif len(TRADE_RESULT) >= 2000 and (stop_trading_now == True or stop_trading_today == True):
        ######################################################
        # Copy values from the previous trade history
        # Basically, just use the previous values in this time as well
        ######################################################
        new_trade_result = {}
        for col in trade_result.keys():
            new_trade_result[col] = pre_TRADE_RESULT[col]
        new_trade_result.update(part_of_indicators_df.iloc[-1])

        ######################################################
        # Update several values from the previous results
        ######################################################
        pre_invest_amount = float(pre_TRADE_RESULT["invest_amount"])

        if float(pre_TRADE_RESULT["bought_price"]) != -99.9:
            current_gain = calculate_gain(
                pre_TRADE_RESULT["bought_price"], part_of_indicators_df["close"].iloc[-1]
            )
            new_trade_result["current_gain"] = current_gain

            new_invest_amount = pre_invest_amount * (1.0 + (0.01 * current_gain))
            profits = new_invest_amount - pre_invest_amount
            new_trade_result["profits"] = profits
            new_trade_result["my_money"] = new_invest_amount
            new_trade_result["long_position_period"] = pre_TRADE_RESULT["long_position_period"] + 1
            new_trade_result["transaction_type"] = "long_pos"
        else:
            current_gain = 0.0
            new_trade_result["current_gain"] = current_gain
            new_trade_result["profits"] = 0.0
            new_trade_result["long_position_period"] = 0
            new_trade_result["transaction_type"] = "no_entry"
            new_trade_result["my_money"] = pre_invest_amount

        if part_of_indicators_df["close"].iloc[-1] - pre_TRADE_RESULT["close"] >= 0:
            # positive gain
            new_trade_result["gain_is_negative_from_prev"] = 0
        else:
            # negative gain
            new_trade_result["gain_is_negative_from_prev"] = (
                pre_TRADE_RESULT["gain_is_negative_from_prev"] + 1
            )
        # reset values
        new_trade_result["continuous_macd_golden_cross_count"] = 0
        new_trade_result["sold_price"] = -99.9

        if str(pre_TRADE_RESULT["dt_send_utc"]) != str(
            part_of_indicators_df["dt_send_utc"].iloc[-1]
        ):
            new_trade_result["total_profit_in_a_day"] = 0.0

        # End stop trading from the next trade
        stop_trading_now_count = TRADE_RESULT.iloc[-1]["stop_trading_now_count"] + 1
        if stop_trading_now_count == max_stop_trading_now_count:
            new_trade_result["stop_trading_now_count"] = 0
            new_trade_result["stop_trading_now"] = False
        else:
            new_trade_result["stop_trading_now_count"] = stop_trading_now_count

        new_trade_result["stop_trading_today"] = stop_trading_today

        ######################################################
        # Append new values
        ######################################################
        TRADE_RESULT = TRADE_RESULT.append(new_trade_result, ignore_index=True)

    # Start Trading
    else:
        # print("Start Trading !!!")
        historical_macd_dead_cross = list(TRADE_RESULT["macd_dead_cross"])
        historical_stoch_dead_cross = list(TRADE_RESULT["stoch_dead_cross"])
        historical_macd_golden_cross = list(TRADE_RESULT["macd_golden_cross"])
        historical_stoch_golden_cross = list(TRADE_RESULT["stoch_golden_cross"])
        historical_stoch_all_dead_cross = list(TRADE_RESULT["stoch_all_dead_cross"])

        # Get trade history in the one previous trade
        pre_TRADE_RESULT = dict(TRADE_RESULT.iloc[-1])
        previous_date = str(pre_TRADE_RESULT["dt_send_utc"])
        current_invest_amount = float(pre_TRADE_RESULT["invest_amount"])
        is_long_position = bool(pre_TRADE_RESULT["is_long_position"])
        long_position_period = int(pre_TRADE_RESULT["long_position_period"])
        stop_trading_today = bool(pre_TRADE_RESULT["stop_trading_today"])
        prev_gain = float(pre_TRADE_RESULT["prev_gain"])
        total_profit_in_a_day = float(pre_TRADE_RESULT["total_profit_in_a_day"])
        initial_invest_amount_today = float(pre_TRADE_RESULT["initial_invest_amount_today"])
        transaction_type = pre_TRADE_RESULT["transaction_type"]
        stop_trading_now = pre_TRADE_RESULT["stop_trading_now"]
        gain_is_negative_from_prev = pre_TRADE_RESULT["gain_is_negative_from_prev"]
        bought_price = pre_TRADE_RESULT["bought_price"]
        sold_price = pre_TRADE_RESULT["sold_price"]
        continuous_macd_golden_cross_count = pre_TRADE_RESULT["continuous_macd_golden_cross_count"]

        stop_trading_now_count = 0
        current_gain = 0.0
        profits = 0.0

        today = str(part_of_indicators_df["dt_send_utc"].iloc[-1])
        current_price = float(part_of_indicators_df["close"].iloc[-1])

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
        if transaction_type == "no_entry" and (
            total_gain_in_a_day < acceptable_max_total_gain_loss_in_one_day
            or total_profit_in_a_day < acceptable_max_total_profit_loss_in_one_day
        ):
            stop_trading_today = True
            print("stop trading today:", today)
            macd_golden_cross = 0
            macd_dead_cross = 0
            stoch_golden_cross = 0
            stoch_dead_cross = 0
            stoch_all_dead_cross = 0
        else:
            ##############
            # Custom indicator
            ##############
            # if part_of_indicators_df["close"].iloc[-1] - pre_TRADE_RESULT["close"] >= 0:
            if (
                float(part_of_indicators_df["close"].iloc[-1])
                - float(part_of_indicators_df["close"].iloc[-2])
                >= 0
            ):
                # positive gain
                gain_is_negative_from_prev = 0
            else:
                # negative gain
                gain_is_negative_from_prev += 1

            ##############
            # MACD
            ##############
            macd = float(part_of_indicators_df["macd"].iloc[-1])

            # check golden cross
            if (
                macd < macd_golden_cross_threshold
                and float(pre_TRADE_RESULT["macd"]) < float(pre_TRADE_RESULT["macd_signal"])
                and float(part_of_indicators_df["macd"].iloc[-1])
                > float(part_of_indicators_df["macd_signal"].iloc[-1])
            ):
                macd_golden_cross = 1
                # historical_macd_golden_cross.append(1)
            else:
                macd_golden_cross = 0
                # historical_macd_golden_cross.append(0)

            if macd_golden_cross == 1:
                continuous_macd_golden_cross_count = 1
            else:
                if continuous_macd_golden_cross_count > 0:
                    if float(part_of_indicators_df["macd"].iloc[-1]) > float(
                        part_of_indicators_df["macd_signal"].iloc[-1]
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
                and float(part_of_indicators_df["macd"].iloc[-1])
                < float(part_of_indicators_df["macd_signal"].iloc[-1])
            ):
                macd_dead_cross = 1
                historical_macd_dead_cross.append(1)
            else:
                macd_dead_cross = 0
                historical_macd_dead_cross.append(0)

            if continuous_macd_golden_cross_count == continuous_macd_golden_cross_count_threshold:
                historical_macd_golden_cross.append(1)
            else:
                historical_macd_golden_cross.append(0)

            ##############
            # Stochastic
            ##############
            stoch_d = float(part_of_indicators_df["stoch_d"].iloc[-1])
            stoch_k = float(part_of_indicators_df["stoch_k"].iloc[-1])

            # check golden cross
            if (
                (stoch_k < stoch_golden_cross_threshold or stoch_d < stoch_golden_cross_threshold)
                and float(pre_TRADE_RESULT["stoch_k"]) < float(pre_TRADE_RESULT["stoch_d"])
                and float(part_of_indicators_df["stoch_k"].iloc[-1])
                > float(part_of_indicators_df["stoch_d"].iloc[-1])
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
                and float(part_of_indicators_df["stoch_k"].iloc[-1])
                < float(part_of_indicators_df["stoch_d"].iloc[-1])
            ):
                stoch_dead_cross = 1
                historical_stoch_dead_cross.append(1)
            else:
                stoch_dead_cross = 0
                historical_stoch_dead_cross.append(0)

            # check all dead crosses, not using "stoch_golden_cross_threshold"
            if float(pre_TRADE_RESULT["stoch_k"]) > float(pre_TRADE_RESULT["stoch_d"]) and float(
                part_of_indicators_df["stoch_k"].iloc[-1]
            ) < float(part_of_indicators_df["stoch_d"].iloc[-1]):
                stoch_all_dead_cross = 1
                historical_stoch_all_dead_cross.append(1)
            else:
                stoch_all_dead_cross = 0
                historical_stoch_all_dead_cross.append(0)

            # # Gradient
            ema30_5_span_grad = float(part_of_indicators_df["ema30_5_span_grad"].iloc[-1])

            if is_long_position:
                # consider if now is the time to sell

                long_position_period += 1

                # calculate current gain
                current_gain = calculate_gain(bought_price, current_price)
                pre_current_invest_amount = current_invest_amount
                tmp_current_invest_amount = current_invest_amount * (1.0 + (0.01 * current_gain))
                profits = tmp_current_invest_amount - pre_current_invest_amount

                should_sell = check_sell_condition(
                    take_profit_threshold,
                    cut_loss_threshold,
                    current_gain,
                    long_position_period,
                    long_position_period_threshold,
                    stoch_k,
                    stoch_d,
                    selling_stoch_threshold,
                    historical_macd_dead_cross[-num_bins_to_check_dead_cross:],
                    historical_stoch_dead_cross[-num_bins_to_check_dead_cross:],
                )

                if should_sell or (
                    should_sell == False
                    and gain_is_negative_from_prev > gain_is_negative_from_prev_threshold
                    and current_gain < selling_current_gain_threshold
                ):
                    # sell
                    is_long_position = False
                    transaction_type = "sold"
                    stop_trading_now = True
                    stop_trading_now_count = 1
                    sold_price = current_price
                    current_invest_amount = tmp_current_invest_amount
                    total_profit_in_a_day += profits

                    # reset
                    long_position_period = 0
                    bought_price = -99.9

                else:
                    # not sell now
                    transaction_type = "long_pos"
            else:
                # consider if now is the time to buy
                if transaction_type == "no_entry":
                    should_buy = check_buy_condition(
                        grad_threshold,
                        ema30_5_span_grad,
                        historical_macd_golden_cross[-num_bins_to_check_macd_golden_cross:],
                        historical_stoch_golden_cross[-num_bins_to_check_stoch_golden_cross:],
                    )
                else:
                    # If already bought and sold in this one minute, not buying anymore at this minute.
                    should_buy = False

                if should_buy:
                    # buy
                    is_long_position = True
                    transaction_type = "bought"
                    stop_trading_now = True
                    bought_price = current_price
                else:
                    # not buy now
                    transaction_type = "no_entry"

                # reset
                sold_price = -99.9

        trade_result["macd_golden_cross"] = macd_golden_cross
        trade_result["macd_dead_cross"] = macd_dead_cross
        trade_result["stoch_golden_cross"] = stoch_golden_cross
        trade_result["stoch_dead_cross"] = stoch_dead_cross
        trade_result["stoch_all_dead_cross"] = stoch_all_dead_cross
        trade_result["transaction_type"] = transaction_type
        trade_result["profits"] = profits
        trade_result["stop_trading_today"] = stop_trading_today
        trade_result["stop_trading_now"] = stop_trading_now
        trade_result["stop_trading_now_count"] = stop_trading_now_count
        trade_result["long_position_period"] = long_position_period
        trade_result["bought_price"] = bought_price
        trade_result["sold_price"] = sold_price
        trade_result["invest_amount"] = current_invest_amount
        trade_result["is_long_position"] = is_long_position
        trade_result["prev_gain"] = prev_gain
        trade_result["current_gain"] = current_gain
        trade_result["total_profit_in_a_day"] = total_profit_in_a_day
        trade_result["accumulate_gain_in_a_day"] = accumulate_gain_in_a_day
        trade_result["my_money"] = current_invest_amount
        trade_result["initial_invest_amount_today"] = initial_invest_amount_today
        trade_result["gain_is_negative_from_prev"] = gain_is_negative_from_prev
        trade_result["continuous_macd_golden_cross_count"] = continuous_macd_golden_cross_count

        trade_result.update(part_of_indicators_df.iloc[-1])
        TRADE_RESULT = TRADE_RESULT.append(trade_result, ignore_index=True)


def main(curr_date, curr_timestamp, consumer_id):
    ########################
    # KAFKA SETUP
    ########################
    # config
    topic_id = os.environ.get("TOPIC_ID")

    # group_id = os.environ.get("GROUP_ID")
    group_id = f"{target_topic}_" + datetime.now().strftime("%Y%m%d%H%M")
    offset_type = os.environ.get("OFFSET_TYPE")

    # Create consumer
    consumer = KafkaConsumer(curr_date, curr_timestamp, consumer_id, group_id, offset_type)

    # Create producer
    producer = KafkaBaseProducer()
    topic_name = f"crypto.{target_topic}"
    num_partitions = 1

    consumer.subscribe([topic_id])
    consumer.logger.info("Start to consume")

    ########################
    # DATAFRAME PREPARATION
    ########################
    quote_base_df = pd.DataFrame(
        columns=["open", "high", "low", "close", "amount", "ts_send_utc", "dt_dummy"]
    )

    indicators_df = pd.DataFrame(
        columns=[
            "ts_send_utc",
            "dt_send_utc",
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
        ]
    )

    ########################
    # INDICATOR PREPARATION
    ########################
    target_indicators = ["macd", "stoch", "sma5", "sma10", "sma30", "sma60"]

    ########################
    # MAIN PROCESSING
    ########################
    while True:
        # consumer new message from kafka topic
        msg = consumer.poll(10.0)
        if msg is None:
            continue
        if msg.error():
            consumer.logger.error("Consumer error: {}".format(msg.error()))
            sys.exit(1)

        # Append the values to dataframe
        # if dataframe has too much records, delete some of them.
        if not quote_base_df.empty and len(quote_base_df) == 2100:
            quote_base_df = quote_base_df.iloc[100:].drop(["dt_dummy"], axis=1)
            quote_base_df["dt_dummy"] = pd.date_range(start="1/1/1800", periods=len(quote_base_df))
            indicators_df = indicators_df.iloc[100:]

        # Extract values for indicators
        consumed_data = json.loads(msg.value().decode("utf-8"))
        open = float(consumed_data["data"][0]["open"])
        high = float(consumed_data["data"][0]["high"])
        low = float(consumed_data["data"][0]["low"])
        close = float(consumed_data["data"][0]["close"])
        amount = float(consumed_data["data"][0]["amount"])
        ts_send_utc = datetime.utcfromtimestamp(int(consumed_data["data"][0]["ts_send"]))
        dt_send_utc = ts_send_utc.date()
        dt_dummy = (
            pd.to_datetime("1800-01-01")
            if quote_base_df.empty
            else quote_base_df.iloc[-1]["dt_dummy"] + pd.Timedelta(days=1)
        )

        quote_base_df = quote_base_df.append(
            {
                "open": open,
                "high": high,
                "low": low,
                "close": close,
                "amount": amount,
                "ts_send_utc": str(ts_send_utc),
                "dt_dummy": dt_dummy,
            },
            ignore_index=True,
        )

        indicators = get_indicators(get_quotes(quote_base_df), target_indicators, periods_used_for_indicators)

        message = {
            "ts_send_utc": str(ts_send_utc),
            "dt_send_utc": str(dt_send_utc),
            "open": open,
            "high": high,
            "low": low,
            "close": close,
            "sma5": indicators["sma5"][-1].sma,
            "sma10": indicators["sma10"][-1].sma,
            "sma30": indicators["sma30"][-1].sma,
            "sma60": indicators["sma60"][-1].sma,
            "macd": indicators["macd"][-1].macd,
            "macd_signal": indicators["macd"][-1].signal,
            "stoch_d": indicators["stoch"][-1].d,
            "stoch_k": indicators["stoch"][-1].k,
            "stoch_j": indicators["stoch"][-1].j,
            "ema30_5_span_grad": None,
            "macd_5_span_grad": None,
            "macd_signal_5_span_grad": None,
        }

        indicators_df = indicators_df.append(message, ignore_index=True)

        _span = 5
        ema30_5_span_grad = calculate_gradient(indicators_df["sma30"].iloc[-_span:], span=_span)
        macd_5_span_grad = calculate_gradient(indicators_df["macd"].iloc[-_span:], span=_span)
        macd_signal_5_span_grad = calculate_gradient(
            indicators_df["macd_signal"].iloc[-_span:], span=_span
        )

        tmp_indicators_df_dict = dict(indicators_df.iloc[-1])
        tmp_indicators_df_dict["ema30_5_span_grad"] = ema30_5_span_grad
        tmp_indicators_df_dict["macd_5_span_grad"] = macd_5_span_grad
        tmp_indicators_df_dict["macd_signal_5_span_grad"] = macd_signal_5_span_grad

        # Update additional grad indicators
        indicators_df = indicators_df.iloc[:-1]
        indicators_df = indicators_df.append(tmp_indicators_df_dict, ignore_index=True)

        message["ema30_5_span_grad"] = ema30_5_span_grad
        message["macd_5_span_grad"] = ema30_5_span_grad
        message["macd_signal_5_span_grad"] = ema30_5_span_grad

        #####################
        # main trade
        #####################
        if len(indicators_df) > 5:
            trade_main(indicators_df.iloc[-5:])
            print(TRADE_RESULT.iloc[-10:])

            # Publish the message
            trade_result = {k: str(v) for k, v in dict(TRADE_RESULT.iloc[-1]).items()}

            # Produce message
            producer.produce_message(topic_name, json.dumps(trade_result), num_partitions)
            producer.poll_message(timeout=10)


if __name__ == "__main__":
    ########################
    # Common Variables
    ########################
    max_stop_trading_now_count = 45
    continuous_macd_golden_cross_count_threshold = 5
    gain_is_negative_from_prev_threshold = 10
    selling_current_gain_threshold = -0.05
    periods_used_for_indicators = 10

    acceptable_max_total_gain_loss_in_one_day = (
        -1.0
    )  # if I lose X% of my money, stop trading in the day.
    acceptable_max_total_profit_loss_ratio_in_one_day = (
        -0.005  # if I lose X yen, stop trading in the day.
    )

    macd_golden_cross_threshold = 5.0  # Threshold for MACD Golden Cross (macd < ${macd_golden_cross_threshold} -> golden cross, else dead cross)
    stoch_golden_cross_threshold = 25.0  # Threshold for Stoch Golden Cross (stoch_k/D < ${stoch_golden_cross_threshold} -> golden cross)

    num_bins_to_check_macd_golden_cross = (
        1  # used to check if there is a macd golden cross in the recent N period (minute)
    )

    num_bins_to_check_stoch_golden_cross = (
        30  # used to check if there is a stoch golden cross in the recent N period (minute)
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

    ########################
    # Variables for checking selling condition function
    ########################
    take_profit_threshold = 0.3  # %
    cut_loss_threshold = -0.1  # %
    long_position_period_threshold = 2000  # number of event for changing new price

    selling_stoch_threshold = (
        65.0  # sell position when stoch value is under ${selling_stoch_threshold}
    )

    ########################
    # Get arguments
    ########################
    args = sys.argv
    curr_date = args[1]
    curr_timestamp = args[2]
    consumer_id = args[3]

    # Load variables from conf file
    load_dotenv(verbose=True)
    conf_file = os.path.join(CONF_DIR, f"{consumer_id}.cf")
    load_dotenv(conf_file)

    target_topic = "realtime_trading_bot_v5"

    main(curr_date, curr_timestamp, consumer_id)
