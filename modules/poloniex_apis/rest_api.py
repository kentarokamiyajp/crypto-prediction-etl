import sys
from common import env_variables

sys.path.append(env_variables.POLONIEX_HOME)
from polosdk import RestClient


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

    def get_candles(self, asset, interval, start, end):
        return self.polo.markets().get_candles(
            asset,
            interval,
            start_time=int(start) * 1000,
            end_time=int(end) * 1000,
            limit=500,
        )
