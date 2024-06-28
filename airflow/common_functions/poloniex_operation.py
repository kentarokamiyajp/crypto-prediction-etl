import sys
import time
import traceback
from datetime import datetime, date
from airflow.exceptions import AirflowFailException

sys.path.append("/opt/airflow")
from dwh_modules.poloniex_apis import rest_api
from dwh_modules.common.utils import unix_time_millisecond_to_second, get_ts_now

DEFAULT_ASSETS = [
    "ADA_USDT",
    "BCH_USDT",
    "BNB_USDT",
    "BTC_USDT",
    "DOGE_USDT",
    "ETH_USDT",
    "LTC_USDT",
    "MKR_USDT",
    "SHIB_USDT",
    "TRX_USDT",
    "XRP_USDT",
]


def get_candle_day_data(custom_assets: list, load_from_days: int):
    """Get Candle data for "every 1 day"

    Args:
        asset (list): Target asset list to get via the Poloniex API
        load_from_days: how many days ago you want to get

    Returns:
        _type_: candle data
    """
    interval = "DAY_1"
    # Create a poloniex session
    polo_operator = rest_api.PoloniexOperator()

    # Merger default and custom assets
    assets = list(set(DEFAULT_ASSETS + custom_assets))

    # Calculate start and end for the loading period
    days = load_from_days  # how many days ago you want to get
    period = 60 * 24 * days  # minute
    end = time.time()
    start = end - 60 * period

    all_candle_data = {}
    for asset in assets:
        print("{}: Load from {} to {}".format(asset, start, end))
        all_candle_data[asset] = polo_operator.get_candles(asset, interval, start, end)
        time.sleep(1)

    return all_candle_data


def get_candle_minute_data(custom_assets: list, load_from_days: int):
    """Get Candle data for "every 1 minute"

    Args:
        asset (list): Target asset list to get via the Poloniex API
        load_from_days: how many days ago you want to get

    Returns:
        _type_: candle data
    """
    interval = "MINUTE_1"
    # Create a poloniex session
    polo_operator = rest_api.PoloniexOperator()

    # Merger default and custom assets
    assets = list(set(DEFAULT_ASSETS + custom_assets))

    # Calculate start and end for the loading period
    target_days = load_from_days  # data of how many days ago you want to get.
    seconds_of_one_day = 60 * 60 * 24  # seconds of one day
    period = seconds_of_one_day * target_days
    to_time = time.time()  # from this time to get the past data
    from_time = to_time - period  # to this time to get the past data

    # Each GET request, only 500 records we can get.
    # This means data for 500 minutes per one request.
    # 1 day = 1440 minutes
    candle_data = {}
    window_size = 60 * 500  # Get data of <window_size> minutes for each time.
    curr_from_time = from_time
    curr_to_time = curr_from_time + window_size
    while True:
        for asset in assets:
            max_retry_cnt = 5
            curr_retry_cnt = 0
            while curr_retry_cnt <= max_retry_cnt:
                try:
                    print("{}: Load from {} to {}".format(asset, curr_from_time, curr_to_time))
                    data = polo_operator.get_candles(asset, interval, curr_from_time, curr_to_time)
                    if data != None:
                        if asset in candle_data:
                            candle_data[asset].extend(data)
                        else:
                            candle_data[asset] = data
                    time.sleep(1)
                    break
                except Exception as error:
                    print("Error: {}".format(error))
                    print(traceback.format_exc())
                    curr_retry_cnt += 1
                    time.sleep(60)

                if curr_retry_cnt > max_retry_cnt:
                    raise AirflowFailException("Poloniex API is dead now.")

        curr_from_time = curr_to_time
        curr_to_time = curr_from_time + window_size

        if curr_from_time > to_time:
            break

    return candle_data


def process_candle_data(**kwargs):
    """Process Poloniex candle data for ingestion in the following steps

    Args:
        data (dict): candle data fetched from Poloniex API

    Returns:
        list: processed data so it can inserted into the target DB such as Cassandra
    """
    data = kwargs["ti"].xcom_pull(task_ids=kwargs["task_id_for_xcom_pull"])
    timezone = "UTC"
    batch_data = []
    for asset_name, asset_data in data.items():
        for d in asset_data:
            ts_create_utc = datetime.utcfromtimestamp(int(d[13]) / 1000.0)
            dt_create_utc = date(
                ts_create_utc.year, ts_create_utc.month, ts_create_utc.day
            ).strftime("%Y-%m-%d")
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
                        unix_time_millisecond_to_second(d[9]),  # ts
                        float(d[10]),  # weightedAverage
                        d[11],  # interval
                        unix_time_millisecond_to_second(d[12]),  # startTime
                        unix_time_millisecond_to_second(d[13]),  # closeTime
                        dt_create_utc,  # dt_create_utc
                        str(ts_create_utc),  # ts_create_utc
                        get_ts_now(timezone),  # ts_insert_utc
                    ]
                )
            except Exception as error:
                print("Error:".format(error))
                print(traceback.format_exc())
                print("asset_data ==>> \n", asset_data)
                sys.exit(1)
    return batch_data
