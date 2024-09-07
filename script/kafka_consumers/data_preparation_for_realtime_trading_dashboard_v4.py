import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import time
import pickle
import pandas as pd
from consumer_operation import KafkaConsumer
from datetime import datetime, timedelta
from pprint import pprint
import ssl

ssl._create_default_https_context = ssl._create_unverified_context

import streamlit as st  # web development
import numpy as np  # np mean, np random
import plotly.express as px  # interactive charts

CONF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "conf")


# Kafka config
curr_date = datetime.now().strftime("%Y-%m-%d")
curr_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
consumer_id = "realtime_trading_dashboard_v4"
topic_id = "crypto.realtime_trading_bot_v4"
# group_id = os.environ.get("GROUP_ID")
group_id = "realtime_trading_dashboard_v4_" + datetime.now().strftime("%Y%m%d%H%M")
offset_type = "latest"

# Create consumer
consumer = KafkaConsumer(
    curr_date,
    curr_timestamp,
    consumer_id,
    group_id,
    offset_type,
    _logdir="/Users/kamiken/git/crypto_prediction_dwh/script/kafka_consumers",
)


consumer.subscribe([topic_id])

consumer.logger.info("Start to consume")


df = pd.DataFrame()
prev_save_time = datetime.now()

while True:
    msg = consumer.poll(10.0)

    if msg is None:
        continue

    if msg.error():
        consumer.logger.error("Consumer error: {}".format(msg.error()))
        sys.exit(1)

    consumed_data = json.loads(msg.value().decode("utf-8"))
    # pprint(consumed_data)

    if df.empty:
        # print("Empty so add new df")
        all_columns = list(consumed_data.keys())
        df = pd.DataFrame.from_dict(
            {0: [consumed_data[col] for col in all_columns]}, orient="index", columns=all_columns
        )
        # print(df)
    else:
        # print("Append new record")
        new_row = pd.DataFrame.from_dict(
            {0: [consumed_data[col] for col in all_columns]}, orient="index", columns=all_columns
        )
        
        if str(consumed_data["ts_create_utc"]) == str(df.iloc[-1]["ts_create_utc"]):
            df = df.iloc[:-1]
            
        df = pd.concat([df, new_row], ignore_index=True).sort_values(by=["ts_create_utc"])
        # print(df)

    save_time_diff = (datetime.now() - prev_save_time).total_seconds()
    if len(df) > 1 and save_time_diff > 5:
        with open(
            "/Users/kamiken/git/crypto_prediction_dwh/script/kafka_consumers/data_for_rt_dashboard_v4.pkl",
            "wb",
        ) as f:
            pickle.dump(df, f)
            
        prev_save_time = datetime.now()

    df = df.iloc[-10000:]
