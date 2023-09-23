import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__)))

from datetime import datetime, date
import pandas_market_calendars as mcal
import pytz
import airflow_env_variables
import traceback

sys.path.append(airflow_env_variables.DWH_SCRIPT)
from common.utils import send_line_message


def send_notification(dag_id, tags, type, optional_message=None):
    jst = pytz.timezone("Asia/Tokyo")
    ts_now = datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S")
    message = "{} [{}]{}\nAirflow Dags: {}".format(ts_now, ",".join(tags), type, dag_id)
    if optional_message:
        message += "\n\n" + optional_message
    send_line_message(message)


def _unix_time_millisecond_to_second(unix_time):
    return int((unix_time) / 1000.0)


def _get_ts_now(_timezone):
    tz = pytz.timezone(_timezone)
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")


def get_dt_from_unix_time(dt_unix_time):
    ts = datetime.fromtimestamp(int(dt_unix_time))
    dt = date(ts.year, ts.month, ts.day).strftime("%Y-%m-%d")
    return dt


def process_candle_data_from_poloniex(data):
    timezone = "UTC"
    batch_data = []
    for asset_name, asset_data in data.items():
        for d in asset_data:
            ts_create = datetime.fromtimestamp(int(d[12]) / 1000.0)
            dt_create = date(ts_create.year, ts_create.month, ts_create.day).strftime("%Y-%m-%d")
            try:
                batch_data.append(
                    [
                        asset_name,  # id
                        float(d[0]),  # low
                        float(d[1]),  # high
                        float(d[2]),  # open
                        float(d[3]),  # close
                        float(d[4]),  # amount
                        float(d[5]),  # quantity
                        float(d[6]),  # buyTakerAmount
                        float(d[7]),  # buyTakerQuantity
                        int(d[8]),  # tradeCount
                        _unix_time_millisecond_to_second(d[9]),  # ts
                        float(d[10]),  # weightedAverage
                        d[11],  # interval
                        _unix_time_millisecond_to_second(d[12]),  # startTime
                        _unix_time_millisecond_to_second(d[13]),  # closeTime
                        dt_create,  # dt_create_utc
                        _get_ts_now(timezone),  # ts_insert_utc
                    ]
                )
            except Exception as error:
                print("Error:".format(error))
                print(traceback.format_exc())
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
                            symbol_name,  # id
                            float(p["low"]),  # low
                            float(p["high"]),  # high
                            float(p["open"]),  # open
                            float(p["close"]),  # close
                            float(p["volume"]),  # volume
                            float(p["adjclose"]),  # adjclose
                            currency,  # currency
                            int(p["date"]),  # dt_unix
                            p["formatted_date"],  # dt
                            int(tz_gmtoffset),  # tz_gmtoffset
                            _get_ts_now(timezone),  # ts_insert_utc
                        ]
                    )
                else:
                    print("WARN: None data is containing: ", p)
            except Exception as error:
                print("Error:".format(error))
                print(traceback.format_exc())
                print("data ==>> \n", p)
                sys.exit(1)
    return batch_data


def is_market_open(date, market):
    calendar = mcal.get_calendar(market)
    if len(calendar.schedule(start_date=date, end_date=date).index) > 0:
        return True
    else:
        return False
