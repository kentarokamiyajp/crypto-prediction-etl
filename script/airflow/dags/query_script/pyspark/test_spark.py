import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, udf
import pytz
from datetime import datetime

import pandas as pd
import numpy as np

from stock_indicators import Quote
from stock_indicators import indicators
from stock_indicators import CandlePart

from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from sklearn.metrics import fbeta_score
from sklearn.preprocessing import MinMaxScaler

from lightgbm import LGBMRegressor


jst = pytz.timezone("Asia/Tokyo")
ts_now = datetime.now(jst).strftime("%Y-%m-%d %H:%M:%S")

SPARK_MASTER_HOST = sys.argv[1]
SPARK_MASTER_PORT = sys.argv[2]
HIVE_METASTORE_HOST = sys.argv[3]
HIVE_METASTORE_PORT = sys.argv[4]

# Create a SparkSession with Hive support and set the Hive host
spark = (
    SparkSession.builder.appName("PySpark Hive Example {}".format(ts_now))
    .config("spark.master", f"spark://{SPARK_MASTER_HOST}:{SPARK_MASTER_PORT}")
    .config(
        "spark.hadoop.hive.metastore.uris",
        f"thrift://{HIVE_METASTORE_HOST}:{HIVE_METASTORE_PORT}",
    )
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "hdfs://192.168.10.14:9000/user/spark/applicationHistory")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

btc_df = spark.sql("select dt, low, high, open, close, amount from crypto_raw.candles_day limit 10")
btc_df.show()
def get_macd(dt_,open_,high_,low_,close_,volume_):
    print(dt_)
    btc_quotes = [
        Quote(datetime.strptime(d,'%Y-%m-%d'),o,h,l,c,v) 
        for d,o,h,l,c,v 
        in zip(dt_, open_, high_, low_, close_, volume_)
    ]
    
    macd_results = indicators.get_macd(btc_quotes, fast_periods=12, slow_periods=26, signal_periods=9)
    print(macd_results)
    return macd_results

upperCaseUDF = udf(lambda a,b,c,d,e,f:get_macd(a,b,c,d,e,f),DoubleType())  


btc_df.withColumn("macd", upperCaseUDF(col("dt"),col("open"),col("high"),col("low"),col("close"),col("amount"))).show(truncate=False)