from datetime import datetime, timezone, date


def get_ts_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def process_candle_data_from_poloniex(data):
    batch_data = []
    for asset_name, asset_data in data.items():
        for d in asset_data:
            dt_with_time = datetime.fromtimestamp(int(d[12]) / 1000.0)
            dt = date(dt_with_time.year, dt_with_time.month, dt_with_time.day).strftime("%Y-%m-%d")
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
                    int(d[9]),
                    float(d[10]),
                    d[11],
                    int(d[12]),
                    int(d[13]),
                    dt,
                    get_ts_now(),
                ]
            )
    return batch_data

def process_yahoofinancials_data(data):
    batch_data = []
    for symbol_name, data in data.items():
        currency = data['currency']
        prices = data['prices']
        for p in prices:
            batch_data.append(
                [
                    symbol_name,
                    float(p['low']),
                    float(p['high']),
                    float(p['open']),
                    float(p['close']),
                    float(p['volume']),
                    float(p['adjclose']),
                    currency,
                    int(p['date']),
                    p['formatted_date'],
                    get_ts_now(),
                ]
            )
    return batch_data
