import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__)))

from datetime import datetime, timezone, date
import pandas_market_calendars as mcal
import pytz
import airflow_env_variables


def _unix_time_millisecond_to_second(unix_time):
    return int((unix_time) / 1000.0)


def get_ts_now(_timezone):
    tz = pytz.timezone(_timezone)
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")


def get_dt_from_unix_time(dt_unix_time):
    dt_with_time = datetime.fromtimestamp(int(dt_unix_time))
    dt = date(dt_with_time.year, dt_with_time.month, dt_with_time.day).strftime(
        "%Y-%m-%d"
    )
    return dt


def process_candle_data_from_poloniex(data):
    timezone = "UTC"
    batch_data = []
    for asset_name, asset_data in data.items():
        for d in asset_data:
            dt_with_time = datetime.fromtimestamp(int(d[12]) / 1000.0)
            dt = date(dt_with_time.year, dt_with_time.month, dt_with_time.day).strftime(
                "%Y-%m-%d"
            )
            try:
                batch_data.append(
                    [
                        asset_name,
                        float(d[0]),
                        float(d[1]),
                        float(d[2]),
                        float(d[3]),
                        float(d[4]),
                        float(d[5]),
                        float(d[6]),
                        float(d[7]),
                        int(d[8]),
                        _unix_time_millisecond_to_second(d[9]),
                        float(d[10]),
                        d[11],
                        _unix_time_millisecond_to_second(d[12]),
                        _unix_time_millisecond_to_second(d[13]),
                        dt,
                        get_ts_now(timezone),
                    ]
                )
            except Exception as error:
                print("Error:".format(error))
                print("asset_data ==>> \n", asset_data)
                sys.exit(1)
    return batch_data


def process_yahoofinancials_data(data):
    timezone = "UTC"
    batch_data = []
    for symbol_name, data in data.items():
        currency = data["currency"]
        prices = data["prices"]
        tz_gmtoffset = data["timeZone"]["gmtOffset"]
        for p in prices:
            try:
                if None not in list(p.values()):
                    batch_data.append(
                        [
                            symbol_name,
                            float(p["low"]),
                            float(p["high"]),
                            float(p["open"]),
                            float(p["close"]),
                            float(p["volume"]),
                            float(p["adjclose"]),
                            currency,
                            int(p["date"]),
                            p["formatted_date"],
                            int(tz_gmtoffset),
                            get_ts_now(timezone),
                        ]
                    )
                else:
                    print("WARN: None data is containing: ", p)
            except Exception as error:
                print("Error:".format(error))
                print("data ==>> \n", p)
                sys.exit(1)
    return batch_data


def is_makert_open(date, market):
    calendar = mcal.get_calendar(market)
    if len(calendar.schedule(start_date=date, end_date=date).index) > 0:
        return True
    else:
        return False


if __name__ == "__main__":
    market = "NYSE"
    date = "2023-07-16"
    print(is_makert_open(date, market))
