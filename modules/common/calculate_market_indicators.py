from datetime import datetime
from stock_indicators import Quote
from stock_indicators.indicators import *


def get_quotes(pandas_df, N_multiple=1.0):
    quotes = [
        Quote(
            datetime.strptime(d, "%Y-%m-%d"),
            o * N_multiple,
            h * N_multiple,
            l * N_multiple,
            c * N_multiple,
            v,
        )
        for d, o, h, l, c, v in zip(
            pandas_df["dt"],
            pandas_df["open"],
            pandas_df["high"],
            pandas_df["low"],
            pandas_df["close"],
            pandas_df["volume"],
        )
    ]

    return quotes


def get_indicators(quotes):
    indicator_values = {
        "macd": get_macd(
            quotes, fast_periods=12, slow_periods=26, signal_periods=9
        ),  # MACD(12,26,9)
        "bollinger_bands": get_bollinger_bands(
            quotes, lookback_periods=20, standard_deviations=2
        ),  # Bollinger Bands(20, 2)
        "ichimoku": get_ichimoku(
            quotes, tenkan_periods=9, kijun_periods=26, senkou_b_periods=52
        ),  # Ichimoku Cloud (9,26,52)
        "stoch": get_stoch(
            quotes, lookback_periods=14, signal_periods=3, smooth_periods=3
        ),  # Stochastic Oscillator %K(14),%D(3) (slow)
        "rsi": get_rsi(quotes, lookback_periods=14),  # Relative Strength Index (14)
        "obv": get_obv(quotes),  # On-Balance Volume
        "aroon": get_aroon(quotes, lookback_periods=25),  # Aroon
        "sma5": get_sma(quotes, lookback_periods=5),  # Simple Moving Average 5 days
        "sma10": get_sma(quotes, lookback_periods=10),  # Simple Moving Average 10 days
        "sma30": get_sma(quotes, lookback_periods=30),  # Simple Moving Average 30 days
        "ema5": get_ema(quotes, lookback_periods=5),  # Exponential Moving Average 5 days
        "ema10": get_ema(quotes, lookback_periods=10),  # Exponential Moving Average 10 days
        "ema30": get_ema(quotes, lookback_periods=30),  # Exponential Moving Average 30 days
    }

    return indicator_values
