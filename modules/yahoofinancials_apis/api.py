from yahoofinancials import YahooFinancials


def get_historical_price_data(symbols: list, interval: str, from_date: str, to_date: str):
    """Get Yahoo Financial data

    Args:
        symbols (list): Financial symbols
        interval (str): 'daily', 'weekly', or 'monthly'
        from_date (str): load from this date ('yyyy-mm-dd')
        to_date (str): load to this date ('yyyy-mm-dd')

    Returns:
        _type_: Yahoo Financial data
    """
    print("Load from {} to {}".format(from_date, to_date))
    yahoo_financials = YahooFinancials(symbols)

    res = yahoo_financials.get_historical_price_data(
        start_date=from_date, end_date=to_date, time_interval=interval
    )

    return res
