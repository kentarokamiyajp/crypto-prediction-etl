from dwh_script.poloniex_apis import get_request
import time

def get_candle_data(asset, interval, start, end):
    polo_operator = get_request.PoloniexOperator()
    raw_candle_data = polo_operator.get_candles(asset, interval, start, end)
    return raw_candle_data

def test(asset, interval, start, end):
    polo_operator = get_request.PoloniexOperator()
    raw_candle_data = polo_operator.get_candles(asset, interval, start, end)
    print(raw_candle_data)

if __name__=="__main__":
    assets = [
        "BTC_USDT",
        "ETH_USDT",
        "BNB_USDT",
        "XRP_USDT",
        "ADA_USDT",
        "DOGE_USDT",
        "SOL_USDT",
        "TRX_USDD",
        "UNI_USDT",
        "ATOM_USDT",
        "GMX_USDT",
        "SHIB_USDT",
        "MKR_USDT",
    ]
    interval = "DAY_1"

    res = {}
    period = 60 * 24 * 2  # minute
    end = time.time()
    start = end - 60 * period
    res = get_candle_data(assets, interval, start, end)
    print(res)