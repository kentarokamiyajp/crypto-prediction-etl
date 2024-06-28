import sys, os
import requests
import pytz
import pandas_market_calendars as mcal
from datetime import datetime, date, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__)))
import env_variables


def send_line_message(message):
    url = env_variables.LINE_NOTIFY_URL
    access_token = env_variables.LINE_ACCESS_TOKEN
    headers = {"Authorization": "Bearer " + access_token}
    payload = {"message": message}
    requests.post(
        url,
        headers=headers,
        params=payload,
    )


def get_ts_now(_timezone: str):
    """Get current timestamp

    Args:
        _timezone (str): timezone name

    Returns:
        str: current timestamp
    """
    ts_now = datetime.now(pytz.timezone(_timezone)).strftime("%Y-%m-%d %H:%M:%S")
    return ts_now


def get_dt_from_unix_time(dt_unix_time: float):
    """_summary_

    Args:
        dt_unix_time (float): unix time

    Returns:
        str: date, e.g., '2024-06-27'
    """
    ts = datetime.utcfromtimestamp(int(dt_unix_time))
    dt = date(ts.year, ts.month, ts.day).strftime("%Y-%m-%d")
    return dt


def unix_time_millisecond_to_second(unix_time: float):
    """Exchange unix time from millisecond to second

    Args:
        unix_time (float): unix time in millisecond

    Returns:
        int: unix time in second
    """
    return int((unix_time) / 1000.0)


def get_first_day_of_N_months_ago(update_N_months_from):
    today = date.today()
    first_day_of_curr_month = today.replace(day=1)
    for i in range(update_N_months_from):
        # Get the day when one day before <first_day_of_curr_month>
        last_day_of_prev_month = first_day_of_curr_month - timedelta(days=1)
        # Get the 1st day of the previous month.
        first_day_of_curr_month = last_day_of_prev_month.replace(day=1)

    return str(first_day_of_curr_month)


def is_market_open(date: str, market: str):
    """Check if the target market is open on the day

    Args:
        date (str): Check if the market open on this date
        market (str): Target market name

    Returns:
        bool: True if the market is open
    """
    calendar = mcal.get_calendar(market)
    if len(calendar.schedule(start_date=date, end_date=date).index) > 0:
        return True
    else:
        return False
