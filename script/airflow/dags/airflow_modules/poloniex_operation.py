import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__)))

import airflow_env_variables

sys.path.append(airflow_env_variables.DWH_SCRIPT)
from poloniex_apis import get_request
import time


def get_candle_data(asset, interval, start, end):
    polo_operator = get_request.PoloniexOperator()
    raw_candle_data = polo_operator.get_candles(asset, interval, start, end)
    return raw_candle_data


def test(asset, interval, start, end):
    polo_operator = get_request.PoloniexOperator()
    raw_candle_data = polo_operator.get_candles(asset, interval, start, end)
    print(raw_candle_data)


if __name__ == "__main__":
    assets = ["BTC_USDT"]
    interval = "MINUTE_1"

    res = {}
    period = 60 * 24 * 2  # minute
    end = time.time()
    start = end - 60 * period
    res = get_candle_data(assets[0], interval, 1687990527, 1688090527)
    print(res)

    # 1688190527.7608151
    # 1688765340000
    # 1688765340000
    # 1688765340000
    # 1688765340000
    # 1688765340000
    # 1688765340000
    # 1688765340000
    # 1688160540000
    # 1688060580000
