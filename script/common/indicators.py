import pandas as pd
from stock_indicators import Quote
from stock_indicators.indicators import *


def get_quotes(pandas_df: pd.DataFrame, N_multiple=1.0) -> list:
    """

    Args:
        pandas_df (pd.DataFrame): Dataframe for OHLC data
        N_multiple (float, optional): If values of the OHLC are too small, set large number in N_multiple value. Defaults to 1.0.

    Returns:
        list: _description_
    """
    quotes = [
        Quote(
            d,
            o * N_multiple,
            h * N_multiple,
            l * N_multiple,
            c * N_multiple,
            v,
        )
        for d, o, h, l, c, v in zip(
            pandas_df["dt_dummy"],
            pandas_df["open"],
            pandas_df["high"],
            pandas_df["low"],
            pandas_df["close"],
            pandas_df["amount"],
        )
    ]

    return quotes


def get_indicators(quotes: list, target_indicators: list, periods_used_for_indicators: int) -> dict:
    """

    Args:
        quotes (list): list of quotes which can be created by get_quotes()
        target_indicators (list): list of indicator names

    Returns:
        dict: indicators
    """
    indicators = {
        "macd": get_macd(
            quotes, fast_periods=12*periods_used_for_indicators, slow_periods=26*periods_used_for_indicators, signal_periods=9*periods_used_for_indicators
        ),  # MACD(12,26,9)
        "stoch": get_stoch(
            quotes, lookback_periods=14*periods_used_for_indicators, signal_periods=3*periods_used_for_indicators, smooth_periods=3*periods_used_for_indicators
        ),  # Stochastic Oscillator %K(14),%D(3) (slow)
        "sma5": get_sma(quotes, lookback_periods=5*periods_used_for_indicators),
        "sma10": get_sma(quotes, lookback_periods=10*periods_used_for_indicators),
        "sma30": get_sma(quotes, lookback_periods=30*periods_used_for_indicators),
        "sma60": get_sma(quotes, lookback_periods=60*periods_used_for_indicators),
    }

    indicator_values = {}
    for ind in target_indicators:
        indicator_values[ind] = indicators[ind]

    return indicator_values

