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
import plotly.graph_objects as go


st.set_page_config(page_title="Real-Time BTC Trading Dashboard", page_icon="✅", layout="wide")

# dashboard title
st.title("Real-Time BTC Trading Dashboard (V2)")

# creating a single-element container.
placeholder = st.empty()

while True:
    try:
        with open(
            "/Users/kamiken/git/crypto_prediction_dwh/script/kafka_consumers/data_for_rt_dashboard_v2.pkl",
            "rb",
        ) as f:
            df = pickle.load(f)
            df["bought_price"] = df["bought_price"].replace(regex=r"-99.9", value=None)
            df.loc[df["transaction_type"] != "buy", "bought_price"] = None # keep bought price only when the transaction value is "buy"
            df["sold_price"] = df["sold_price"].replace(regex=r"-99.9", value=None)
            df["profits_in_this_transaction"] = df["profits"].replace(regex=r"0.0", value=None)
    except:
        time.sleep(5)
        print("Failed to open pickle file !!!")
        continue

    with placeholder.container():
        # create three columns
        kpi01, kpi02, kpi03 = st.columns(3)
        kpi01.metric(label="TS Candle UTC", value=str(df.iloc[-1]["ts_create_utc"]))
        kpi02.metric(label="Trade Status", value=str(df.iloc[-1]["transaction_type"]))
        kpi03.metric(
            label="Grain in this transaction",
            value=round(float(df.iloc[-1]["current_gain"]), 5),
        )

        kpi11, kpi12, kpi13, kpi14, kpi15, kpi16 = st.columns(6)
        kpi11.metric(
            label="Open",
            value=float(df.iloc[-1]["open"]),
            delta=round((float(df.iloc[-1]["open"]) - float(df.iloc[-2]["open"])), 2),
        )
        kpi12.metric(
            label="High",
            value=float(df.iloc[-1]["high"]),
            delta=round((float(df.iloc[-1]["high"]) - float(df.iloc[-2]["high"])), 2),
        )
        kpi13.metric(
            label="Low",
            value=float(df.iloc[-1]["low"]),
            delta=round((float(df.iloc[-1]["low"]) - float(df.iloc[-2]["low"])), 2),
        )
        kpi14.metric(
            label="Close",
            value=float(df.iloc[-1]["close"]),
            delta=round((float(df.iloc[-1]["close"]) - float(df.iloc[-2]["close"])), 2),
        )
        kpi15.metric(
            label="My Money",
            value=round(float(df.iloc[-1]["my_money"]), 2),
            delta=round(
                (
                    float(df.iloc[-1]["my_money"])
                    - float(df.iloc[-1]["initial_invest_amount_today"])
                ),
                2,
            ),
        )
        kpi16.metric(
            label="Today's Profit",
            value=round(float(df.iloc[-1]["total_profit_in_a_day"]), 2),
            delta=round(float(df.iloc[-1]["total_profit_in_a_day"]), 2),
        )

        with st.container():
            st.markdown("### BTC/USDT Price")

            fig_price = px.line(data_frame=df.iloc[-120:], y="close", x="ts_create_utc")

            fig_bought = px.line(
                data_frame=df.iloc[-120:], y="bought_price", x="ts_create_utc", markers=True
            )
            fig_bought.update_traces(
                marker=dict(color="green", size=15), line=dict(color="purple", width=1)
            )

            fig_sold = px.line(
                data_frame=df.iloc[-120:], y="sold_price", x="ts_create_utc", markers=True
            )
            fig_sold.update_traces(
                marker=dict(color="red", size=15), line=dict(color="purple", width=1)
            )

            fig = go.Figure(data=fig_price.data + fig_bought.data + fig_sold.data)

            st.write(fig)

        fig_col2, fig_col3 = st.columns(2)
        with fig_col3:
            today = df["dt"].max()
            st.markdown(f"### Today's Profits ({today})")
            df_profits = df[df["dt"] == today]
            fig3 = px.line(data_frame=df_profits, y="total_profit_in_a_day", x="ts_create_utc")
            st.write(fig3)
        with fig_col2:
            st.markdown("### My Money")
            fig2 = px.line(data_frame=df, y="my_money", x="ts_create_utc")
            st.write(fig2)

        st.markdown("### Base Info")
        st.dataframe(
            df.iloc[-1000:][
                [
                    "ts_send_utc",
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
                    "stoch_k",
                    "stoch_d",
                ]
            ]
        )

        st.markdown("### Buying Condition Indicators")
        st.dataframe(
            df.iloc[-1000:][
                [
                    "ts_create_utc",
                    "ema30_5_span_grad",
                    "macd_golden_cross",
                    "stoch_golden_cross",
                    "transaction_type",
                    "bought_price",
                ]
            ]
        )

        st.markdown("### # Selling Condition Indicators")
        st.dataframe(
            df.iloc[-1000:][
                [
                    "ts_create_utc",
                    "macd_dead_cross",
                    "stoch_dead_cross",
                    "is_long_position",
                    "long_position_period",
                    "transaction_type",
                    "current_gain",
                    "gain_is_negative_from_prev",
                    "profits_in_this_transaction",
                    "sold_price",
                ]
            ]
        )

        st.markdown("### # Transaction Info")
        st.dataframe(
            df.iloc[-1000:][
                [
                    "ts_create_utc",
                    "transaction_type",
                    "bought_price",
                    "sold_price",
                    "current_gain",
                    "profits_in_this_transaction",
                    "accumulate_gain_in_a_day",
                    "total_profit_in_a_day",
                    "stop_trading_this_minute",
                    "stop_trading_today",
                    "my_money",
                ]
            ]
        )
        time.sleep(60)
    # placeholder.empty()
