from pycoingecko import CoinGeckoAPI
from pprint import pprint

cg = CoinGeckoAPI()

# # get bitcoin USD price
# btc_usd_price = cg.get_price(ids='bitcoin', vs_currencies='usd')
# print(btc_usd_price)

# # get multiple currencies USD price
# usd_price = cg.get_price(ids=['bitcoin', 'ethereum','litecoin'], vs_currencies='usd')
# print(usd_price)

# # get 24 change
# btc_24_change = cg.get_price(ids='bitcoin', 
#             vs_currencies='usd',
#             include_market_cap='true', 
#             include_24hr_vol='true', 
#             include_24hr_change='true', 
#             include_last_updated_at='true')
# pprint(btc_24_change)

# # get historical data
# data = cg.get_coin_history_by_id(id='bitcoin',date='10-11-2020', localization='false')
# pprint(data)

# data_3 = cg.get_coin_market_chart_by_id(id='bitcoin',vs_currency='usd',days='3')
# pprint(data_3)

# data_with_ts = cg.get_coin_market_chart_range_by_id(id='bitcoin',vs_currency='usd',from_timestamp='1605096000',to_timestamp='1605099600')
# pprint(data_with_ts)

# get coin list
clist = cg.get_coins_list()
pprint(clist)