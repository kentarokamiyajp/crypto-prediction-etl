import sys
from pyspark.sql import SparkSession
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
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.sql("select count(*) from crypto_raw.candles_day")
df.show()