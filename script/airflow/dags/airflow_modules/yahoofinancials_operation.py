import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__)))

from yahoofinancials import YahooFinancials
import time
import utils
from pprint import pprint


def get_data_from_yahoofinancials(symbols, interval, start, end):
    yahoo_financials = YahooFinancials(symbols)
    data = yahoo_financials.get_historical_price_data(
        start_date=start, end_date=end, time_interval=interval
    )
    return data


if __name__ == "__main__":
    symbol = "CL=F"

    interval = "daily"

    # how many days ago you want to get.
    target_days = 7

    # seconds of one day
    seconds_of_one_day = 60 * 60 * 24
    period = seconds_of_one_day * target_days

    # to this time to get the past data
    to_ts = time.time()

    # from this time to get the past data
    from_ts = to_ts - period

    from_date = utils.get_dt_from_unix_time(from_ts)
    to_date = utils.get_dt_from_unix_time(to_ts)

    data = get_data_from_yahoofinancials([symbol], interval, from_date, to_date)

    pprint(data)
