import os,sys
from pycoingecko import CoinGeckoAPI
from pprint import pprint

class CryptoOperator():
    def __init__(self):
        self.coin = CoinGeckoAPI()
        try:
            self.coin.ping()
            print('Ping to CoinGecko was successed.')
        except:
            print('Filed to ping.')
            sys.exit(1)
            
    def get_price(self):
        return self.coin.get_price()
    
    def get_token_price(self):
        return self.coin.get_token_price()
    
    def get_supported_vs_currencies(self):
        return self.coin.get_supported_vs_currencies()
    
    def get_coins_list(self):
        return self.coin.get_coins_list()
    
    def get_coins_markets(self,vs_currency,order,per_page,page):
        return self.coin.get_coins_markets(vs_currency=vs_currency,order=order,per_page=per_page,page=page)
    
    def get_coins_by_id(self,id,tickers,market_data):
        return self.coin.get_coin_by_id(id=id,tickers=tickers,market_data=market_data)
    
    def get_coin_ticker_by_id(self):
        return self.coin.get_coin_ticker_by_id()
    
    def get_coin_history_by_id(self):
        return self.coin.get_coin_history_by_id()
    
    def get_coin_market_chart_by_id(self):
        return self.coin.get_coin_market_chart_by_id()
    
    def get_coin_market_chart_range_by_id(self):
        return self.coin.get_coin_market_chart_range_by_id()
    
    def get_coin_ohlc_by_id(self):
        return self.coin.get_coin_ohlc_by_id()
    
    def get_coins_categories_list(self):
        return self.coin.get_coins_categories_list()
    
    def get_coins_categories(self):
        return self.coin.get_coins_categories()
    
    def get_exchanges_list(self):
        return self.coin.get_exchanges_list()
    
    def get_exchanges_id_name_list(self):
        return self.coin.get_exchanges_id_name_list()
    
    def get_exchanges_by_id(self):
        return self.coin.get_exchanges_by_id()
    
    def get_exchanges_tickers_by_id(self):
        return self.coin.get_exchanges_tickers_by_id()
    
    def get_exchanges_volume_chart_by_id(self):
        return self.coin.get_exchanges_volume_chart_by_id()
    
def main():
    co = CryptoOperator()
    pprint(co.get_coins_list())

if __name__=="__main__":
    main()