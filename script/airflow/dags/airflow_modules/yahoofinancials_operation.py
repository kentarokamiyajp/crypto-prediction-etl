import datetime
from yahoofinancials import YahooFinancials
import pytz


def get_data_from_yahoofinancials(symbols, days, interval):
    tz = pytz.timezone("UTC")
    ts_now = datetime.datetime.now(tz)
    start_date = (ts_now + datetime.timedelta(days=days)).strftime("%Y-%m-%d")
    end_date = ts_now.strftime("%Y-%m-%d")
    yahoo_financials = YahooFinancials(symbols)
    data = yahoo_financials.get_historical_price_data(
        start_date=start_date, end_date=end_date, time_interval=interval
    )
    return data


if __name__ == "__main__":
    ticker = ["^NDX"]
    days = -7
    interval = "daily"
    data = get_data_from_yahoofinancials(ticker, days, interval)
    print(data)
