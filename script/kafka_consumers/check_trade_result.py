import pickle
import pandas as pd
from pprint import pprint

with open(
    f"/home/pyuser/git/crypto_prediction_dwh/script/kafka_consumers/trade_variables.pkl", "rb"
) as f:
    traded_result_df = pickle.load(f)

traded_result_df.to_csv(
    "/home/pyuser/git/crypto_prediction_dwh/script/kafka_consumers/trade_variables.csv"
)
