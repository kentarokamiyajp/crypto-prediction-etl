import sys, os

sys.path.append("/home/polo_sdk")

import time
from polosdk import RestClient
from pprint import pprint


class PoloniexOperator:
    def __init__(self):
        self.polo = RestClient()

    def get_market(self, asset):
        return self.polo.get_market(asset)

    def get_currency(self, currencty):
        return self.polo.get_currency(currencty)

    def get_price(self, asset):
        return self.polo.markets().get_price(asset)

    def get_orderbook(self, asset, scale=0.01, limit=100):
        return self.polo.markets().get_orderbook(asset, scale=scale, limit=limit)

    def get_candles(self, asset, interval, period):
        end = time.time()
        start = end - 60 * period
        return self.polo.markets().get_candles(
            asset, interval, start_time=int(start) * 1000, end_time=int(end) * 1000
        )


def main():
    polo_operator = PoloniexOperator()
    asset = "BTC_USDT"
    currency = "BTC"
    interval = "MINUTE_1"
    period = 5  # minute
    # pprint(polo_operator.get_market("BTC_USDT"))
    # pprint(polo_operator.get_currency(currency))
    # pprint(polo_operator.get_price(asset))
    # pprint(polo_operator.get_orderbook(asset))
    pprint(polo_operator.get_candles(asset, interval, period))


if __name__ == "__main__":
    main()
