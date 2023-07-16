import datetime
from yahoofinancials import YahooFinancials


def get_data_from_yahoofinancials(symbols, days):
    start_date = (datetime.datetime.today() + datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    end_date = datetime.datetime.today().strftime("%Y-%m-%d")
    yahoo_financials = YahooFinancials(symbols)
    data = yahoo_financials.get_historical_price_data(start_date=start_date, end_date=end_date, time_interval="daily")
    return data

if __name__ == "__main__":
    ticker = ["^DJI","^NDX"]
    get_data_from_yahoofinancials(ticker)
