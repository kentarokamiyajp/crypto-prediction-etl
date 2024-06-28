import os, sys
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from stock_indicators import Quote
from stock_indicators import indicators

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import env_variables

# Create a SparkSession with Hive connection
spark = (
    SparkSession.builder.appName("PySpark Hive Example")
    .config(
        "spark.master",
        "spark://{}:{}".format(env_variables.SPARK_MASTER_HOST, env_variables.SPARK_MASTER_PORT),
    )
    .config(
        "spark.hadoop.hive.metastore.uris",
        "thrift://{}:{}".format(
            env_variables.HIVE_METASTORE_HOST, env_variables.HIVE_METASTORE_PORT
        ),
    )
    .config("spark.debug.maxToStringFields", "100")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Get crypto all historical data from hive RAW table
crypto_raw_df = spark.sql(
    f"select id, cast(dt_create_utc as string) as dt, open, high, low, close, amount as volume from crypto_raw.candles_day where dt > '2022-01-01'"
)

# Get distinct crypto symbols.
crypto_symbol_df = spark.sql(f"select distinct id from crypto_raw.candles_day")

final_results = None

# Need to be multiplied symbols due to a too small value.
exceptional_symbol = ["SHIB_USDT"]

# Get crypto indicators for each symbol.
for row in crypto_symbol_df.select(crypto_symbol_df.id).collect():
    print("Target Crypto Symbol:", row.id)
    sp_crypto_history_df = crypto_raw_df.filter(crypto_raw_df.id == row.id)
    pd_crypto_history_df = sp_crypto_history_df.toPandas()

    # Create quotes
    N_mul = 1.0
    if row.id in exceptional_symbol:
        N_mul = 1000.0
    quotes = [
        Quote(
            datetime.strptime(d, "%Y-%m-%d"),
            o * N_mul,
            h * N_mul,
            l * N_mul,
            c * N_mul,
            v,
        )
        for d, o, h, l, c, v in zip(
            pd_crypto_history_df["dt"],
            pd_crypto_history_df["open"],
            pd_crypto_history_df["high"],
            pd_crypto_history_df["low"],
            pd_crypto_history_df["close"],
            pd_crypto_history_df["volume"],
        )
    ]

    indicator_values = {}

    #######################
    # Calculate indicator value
    #######################
    # calculate MACD(12,26,9)
    macd_results = indicators.get_macd(quotes, fast_periods=12, slow_periods=26, signal_periods=9)
    indicator_values["macd"] = macd_results
    # calculate RSI(14)
    rsi_results = indicators.get_rsi(quotes, lookback_periods=14)
    indicator_values["rsi"] = rsi_results
    # calculate BollingerBands(20, 2)
    bollinger_bands_results = indicators.get_bollinger_bands(
        quotes, lookback_periods=20, standard_deviations=2
    )
    indicator_values["bollinger_bands"] = bollinger_bands_results
    # Calculate
    obv_results = indicators.get_obv(quotes)
    indicator_values["obv"] = obv_results
    # calculate ICHIMOKU(9,26,52)
    ichimoku_results = indicators.get_ichimoku(
        quotes, tenkan_periods=9, kijun_periods=26, senkou_b_periods=52
    )
    indicator_values["ichimoku"] = ichimoku_results
    # calculate STO %K(14),%D(3) (slow)
    stoch_results = indicators.get_stoch(
        quotes, lookback_periods=14, signal_periods=3, smooth_periods=3
    )
    indicator_values["stoch"] = stoch_results
    aroon_results = indicators.get_aroon(quotes, lookback_periods=25)
    indicator_values["aroon"] = aroon_results

    ##########################
    # Merge all indicator values
    ##########################
    all_indicaters = {}
    for data in zip(
        indicator_values["macd"],
        indicator_values["rsi"],
        indicator_values["bollinger_bands"],
        indicator_values["obv"],
        indicator_values["ichimoku"],
        indicator_values["stoch"],
    ):
        all_indicaters[data[0].date.strftime("%Y-%m-%d")] = [
            data[0].date.strftime("%Y-%m-%d"),
            pd_crypto_history_df.loc[
                pd_crypto_history_df["dt"] == str(data[0].date.strftime("%Y-%m-%d"))
            ]["open"].values[0],
            pd_crypto_history_df.loc[
                pd_crypto_history_df["dt"] == str(data[0].date.strftime("%Y-%m-%d"))
            ]["close"].values[0],
            pd_crypto_history_df.loc[
                pd_crypto_history_df["dt"] == str(data[0].date.strftime("%Y-%m-%d"))
            ]["high"].values[0],
            pd_crypto_history_df.loc[
                pd_crypto_history_df["dt"] == str(data[0].date.strftime("%Y-%m-%d"))
            ]["low"].values[0],
            pd_crypto_history_df.loc[
                pd_crypto_history_df["dt"] == str(data[0].date.strftime("%Y-%m-%d"))
            ]["volume"].values[0],
            data[0].macd,
            data[1].rsi,
            data[2].sma,
            data[2].lower_band,
            data[2].upper_band,
            data[3].obv,
            data[4].chikou_span,
            data[4].kijun_sen,
            data[4].senkou_span_a,
            data[4].senkou_span_b,
            data[5].d,
            data[5].k,
            data[5].j,
            N_mul,
        ]

    columns = [
        "dt_ind",
        "open",
        "close",
        "high",
        "Llow",
        "volume",
        "macd",
        "rsi",
        "sma",
        "lower_band",
        "upper_band",
        "obv",
        "chikou_span",
        "kijun_sen",
        "senkou_span_a",
        "senkou_span_b",
        "d",
        "k",
        "j",
        "N_multiple",
    ]

    pd_all_indicaters_df = pd.DataFrame.from_dict(all_indicaters, orient="index", columns=columns)
    sp_all_indicaters_df = spark.createDataFrame(pd_all_indicaters_df)

    sp_history_with_indicators_df = sp_crypto_history_df.join(
        sp_all_indicaters_df,
        sp_crypto_history_df.dt == sp_all_indicaters_df.dt_ind,
        "outer",
    )

    if final_results:
        final_results = final_results.unionAll(sp_history_with_indicators_df)
    else:
        final_results = sp_history_with_indicators_df
    break

final_results.filter((final_results.id == "BTC_USDT") & (final_results.dt >= "2023-07-08")).select(
    col("id"), col("dt"), col("macd")
).sort(col("dt")).show(50)
