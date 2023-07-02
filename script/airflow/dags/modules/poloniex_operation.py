from dwh_script.poloniex_apis import get_request


def get_candle_data(asset, interval, start, end):
    polo_operator = get_request.PoloniexOperator()
    raw_candle_data = polo_operator.get_candles(asset, interval, start, end)
    return raw_candle_data
