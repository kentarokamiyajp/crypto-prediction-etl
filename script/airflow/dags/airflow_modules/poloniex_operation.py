import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__)))

import airflow_env_variables

sys.path.append(airflow_env_variables.DWH_SCRIPT)
from poloniex_apis import rest_api


def get_candle_data(asset, interval, start, end):
    polo_operator = rest_api.PoloniexOperator()
    raw_candle_data = polo_operator.get_candles(asset, interval, start, end)
    return raw_candle_data
