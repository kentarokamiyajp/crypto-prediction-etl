import sys, os
from pyspark.sql.functions import col
from pyspark.sql.types import *
import pandas as pd

SPARK_CONFIG = {"spark.cores.max": "2", "spark.executor.cores": "2", "spark.executor.memory": "5g"}

HIVE_CONFIG = {
    "source_schema": "gas_raw",
    "source_table": "natural_gas_price_day",
    "target_schema": "gas_mart",
    "target_table": "wrk_natural_gas_indicator_day",
}


def main(dwh_module_home, N_month, update_N_months_from):
    sys.path.append(dwh_module_home)
    from spark_operations.utils.session import BaseSparkSession
    from common.calculate_market_indicators import get_quotes, get_indicators
    from common.utils import get_first_day_of_N_months_ago

    base_spark_app_name = os.path.basename(__file__)
    with BaseSparkSession(SPARK_CONFIG, base_spark_app_name) as spark_session:
        #############################################
        # 1. Load historical data from Hive table
        #############################################
        # Select data from <N_month> months ago to calculate the indicators.
        query = f""" \
            select \
                id, cast(dt_create_utc as string) as dt, open, high, low, close, volume, year, month, day \
            from \
                {HIVE_CONFIG["source_schema"]}.{HIVE_CONFIG["source_table"]} \
            where \
                dt_create_utc >= add_months(current_date, -{N_month})"""
        natural_gas_raw_df = spark_session.run_sql(query)

        # Select distinct symbols. (e.g., CL=F)
        natural_gas_symbol_df = spark_session.run_sql(
            f"""select distinct id from {HIVE_CONFIG["source_schema"]}.{HIVE_CONFIG["source_table"]}"""
        )

        #############################################
        # Calculate indicators
        #############################################
        # Calculate the indicator for each symbol.
        final_results = None
        for row in natural_gas_symbol_df.select(natural_gas_symbol_df.id).collect():
            print("Target Natural Gas Symbol:", row.id)

            ##########################
            # 2-1. Get Indicators
            ##########################
            sp_natural_gas_history_df = natural_gas_raw_df.filter(natural_gas_raw_df.id == row.id)
            pd_natural_gas_history_df = sp_natural_gas_history_df.toPandas()
            N_multiple = 1.0  # If OHLC values are very small, multiple X to increase the all values
            quotes = get_quotes(pd_natural_gas_history_df, N_multiple)
            indicator_values = get_indicators(quotes)

            ##########################
            # 2-2. Merge all indicator values
            ##########################
            all_indicaters = {}
            for data in zip(
                indicator_values["macd"],
                indicator_values["rsi"],
                indicator_values["bollinger_bands"],
                indicator_values["obv"],
                indicator_values["ichimoku"],
                indicator_values["stoch"],
                indicator_values["aroon"],
                indicator_values["sma5"],
                indicator_values["sma10"],
                indicator_values["sma30"],
                indicator_values["ema5"],
                indicator_values["ema10"],
                indicator_values["ema30"],
            ):
                all_indicaters[data[0].date.strftime("%Y-%m-%d")] = [
                    data[0].date,
                    float(data[0].macd) if data[0].macd else None,
                    float(data[0].signal) if data[0].signal else None,
                    float(data[1].rsi) if data[1].rsi else None,
                    float(data[2].sma) if data[2].sma else None,
                    float(data[2].lower_band) if data[2].lower_band else None,
                    float(data[2].upper_band) if data[2].upper_band else None,
                    float(data[3].obv) if data[3].obv else None,
                    float(data[3].obv_sma) if data[3].obv_sma else None,
                    float(data[4].chikou_span) if data[4].chikou_span else None,
                    float(data[4].kijun_sen) if data[4].kijun_sen else None,
                    float(data[4].tenkan_sen) if data[4].tenkan_sen else None,
                    float(data[4].senkou_span_a) if data[4].senkou_span_a else None,
                    float(data[4].senkou_span_b) if data[4].senkou_span_b else None,
                    float(data[5].d) if data[5].d else None,
                    float(data[5].k) if data[5].k else None,
                    float(data[5].j) if data[5].j else None,
                    float(data[6].aroon_up) if data[6].aroon_up else None,
                    float(data[6].aroon_down) if data[6].aroon_down else None,
                    float(data[6].oscillator) if data[6].oscillator else None,
                    float(data[7].sma) if data[7].sma else None,
                    float(data[8].sma) if data[8].sma else None,
                    float(data[9].sma) if data[9].sma else None,
                    float(data[10].ema) if data[10].ema else None,
                    float(data[11].ema) if data[11].ema else None,
                    float(data[12].ema) if data[12].ema else None,
                    N_multiple,
                ]

            columns = [
                "dt_",
                "macd",
                "macd_single",
                "rsi",
                "bollinger_bands_sma",
                "bollinger_bands_lower_band",
                "bollinger_bands_upper_band",
                "obv",
                "obv_sma",
                "ichimoku_chikou_span",
                "ichimoku_kijun_sen",
                "ichimoku_tenkan_sen",
                "ichimoku_senkou_span_a",
                "ichimoku_senkou_span_b",
                "stoch_oscillator",
                "stoch_signal",
                "stoch_percent_j",
                "aroon_up",
                "aroon_down",
                "aroon_oscillator",
                "sma5",
                "sma10",
                "sma30",
                "ema5",
                "ema10",
                "ema30",
                "N_multiple",
            ]

            # Define a schema of spark dataframe for the indicators
            schema = StructType(
                [
                    StructField("dt_", DateType(), False),
                    StructField("macd", FloatType(), True),
                    StructField("macd_single", FloatType(), True),
                    StructField("rsi", FloatType(), True),
                    StructField("bollinger_bands_sma", FloatType(), True),
                    StructField("bollinger_bands_lower_band", FloatType(), True),
                    StructField("bollinger_bands_upper_band", FloatType(), True),
                    StructField("obv", FloatType(), True),
                    StructField("obv_sma", FloatType(), True),
                    StructField("ichimoku_chikou_span", FloatType(), True),
                    StructField("ichimoku_kijun_sen", FloatType(), True),
                    StructField("ichimoku_tenkan_sen", FloatType(), True),
                    StructField("ichimoku_senkou_span_a", FloatType(), True),
                    StructField("ichimoku_senkou_span_b", FloatType(), True),
                    StructField("stoch_oscillator", FloatType(), True),
                    StructField("stoch_signal", FloatType(), True),
                    StructField("stoch_percent_j", FloatType(), True),
                    StructField("aroon_up", FloatType(), True),
                    StructField("aroon_down", FloatType(), True),
                    StructField("aroon_oscillator", FloatType(), True),
                    StructField("sma5", FloatType(), True),
                    StructField("sma10", FloatType(), True),
                    StructField("sma30", FloatType(), True),
                    StructField("ema5", FloatType(), True),
                    StructField("ema10", FloatType(), True),
                    StructField("ema30", FloatType(), True),
                    StructField("N_multiple", FloatType(), True),
                ]
            )

            # Create pandas dataframe from python dict object that contains the indicators.
            pd_all_indicaters_df = pd.DataFrame.from_dict(
                all_indicaters, orient="index", columns=columns
            )

            # Create spark dataframe from pandas dataframe
            sp_all_indicaters_df = spark_session.spark.createDataFrame(
                pd_all_indicaters_df, schema=schema
            )

            # Join the two spark dataframes of historical data and indicator data.
            sp_history_with_indicators_df = sp_natural_gas_history_df.join(
                sp_all_indicaters_df,
                sp_natural_gas_history_df.dt == sp_all_indicaters_df.dt_,
                "outer",
            )

            # Union spark dataframe for each symbol.
            if final_results:
                final_results = final_results.unionAll(sp_history_with_indicators_df)
            else:
                final_results = sp_history_with_indicators_df

        #############################################
        # Insert the calculated indicator values to the hive mart WRK table
        #############################################
        # Update/Insert indicator from <update_N_months_from> months ago.
        target_first_day_to_insert = get_first_day_of_N_months_ago(update_N_months_from)
        insert_data = final_results.select(
            col("id"),
            col("dt_"),
            col("low"),
            col("high"),
            col("open"),
            col("close"),
            col("volume"),
            col("macd"),
            col("macd_single"),
            col("rsi"),
            col("bollinger_bands_sma"),
            col("bollinger_bands_lower_band"),
            col("bollinger_bands_upper_band"),
            col("obv"),
            col("obv_sma"),
            col("ichimoku_chikou_span"),
            col("ichimoku_kijun_sen"),
            col("ichimoku_tenkan_sen"),
            col("ichimoku_senkou_span_a"),
            col("ichimoku_senkou_span_b"),
            col("stoch_oscillator"),
            col("stoch_signal"),
            col("stoch_percent_j"),
            col("aroon_up"),
            col("aroon_down"),
            col("aroon_oscillator"),
            col("sma5"),
            col("sma10"),
            col("sma30"),
            col("ema5"),
            col("ema10"),
            col("ema30"),
            col("N_multiple"),
            col("year"),
            col("month"),
            col("day"),
        ).filter(final_results.dt_ >= target_first_day_to_insert)

        # Insert the calculated indicator values
        insert_data.write.insertInto(
            f"""{HIVE_CONFIG["target_schema"]}.{HIVE_CONFIG["target_table"]}""", overwrite=True
        )


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("[FAILED] Arguments Error !!!")
        print(
            "e.g, python XXX.py <dwh_module_home> <N_months_data_to_calculate_indicator> <update_N_months_from>"
        )
        sys.exit(1)

    dwh_module_home = sys.argv[1]
    N_month = int(sys.argv[2])  # N_months_data_to_calculate_indicator
    update_N_months_from = int(sys.argv[3])
    main(dwh_module_home, N_month, update_N_months_from)
