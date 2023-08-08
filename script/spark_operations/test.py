import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import pandas as pd
from stock_indicators import Quote
from stock_indicators import indicators


# Create a SparkSession with Hive support and set the Hive host
spark = (
    SparkSession.builder.appName("PySpark Hive Example")
    .config("spark.master", "spark://192.168.10.14:7077")
    .config("spark.hadoop.hive.metastore.uris", "thrift://192.168.10.14:9083")
    .config("spark.debug.maxToStringFields", "100")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

from_df = spark.sql(
    f"update crypto_raw.candles_day_acid set open = 0.0 where id = 'BTC_USDT'"
)

# # Create a new DataFrame
# new_data = [[4, 'd'], [5, 'e']]
# df_new = spark.createDataFrame(new_data, ['id', 'attr'])
# df_new.show()

# # Insert into Hive table
# df_new.write.insertInto('test_db.test_table', overwrite=False)