import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__)))

from yahoofinancials import YahooFinancials


def get_data_from_yahoofinancials(symbols, interval, start, end):
    yahoo_financials = YahooFinancials(symbols)
    data = yahoo_financials.get_historical_price_data(start_date=start, end_date=end, time_interval=interval)
    return data
