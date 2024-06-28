import sys
import time
import traceback

sys.path.append("/opt/airflow")
from dwh_modules.common.utils import get_ts_now, get_dt_from_unix_time
from dwh_modules.yahoofinancials_apis.api import get_historical_price_data


def get_yahoofinancials_data(symbols: list, load_from_days: int, interval: str):
    """Get Yahoo Financial data

    Args:
        symbols (list): Financial symbols
        load_from_days (int): How many days before are in the fetching scope
        interval (str): 'daily', 'weekly', or 'monthly'

    Returns:
        _type_: Yahoo Financial data
    """

    # how many days ago you want to get.
    target_days = load_from_days

    # seconds of one day
    seconds_of_one_day = 60 * 60 * 24
    period = seconds_of_one_day * target_days

    # to this time to get the past data
    to_ts = time.time()
    to_date = get_dt_from_unix_time(to_ts)

    # from this time to get the past data
    from_ts = to_ts - period
    from_date = get_dt_from_unix_time(from_ts)

    data = get_historical_price_data(symbols, interval, from_date, to_date)

    return data


def process_yahoofinancials_data(**kwargs):
    """process data for ingestion

    Returns:
        _type_: processed batch data for the ingestion in the next steps
    """
    data = kwargs["ti"].xcom_pull(task_ids=kwargs["task_id_for_xcom_pull"])
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
                            get_ts_now(timezone),  # ts_insert_utc
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
