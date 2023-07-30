from pyspark.sql import SparkSession

# Create a SparkSession with Hive support and set the Hive host
spark = (
    SparkSession.builder
    .appName("PySpark Hive Example")
    .config("spark.master", "spark://192.168.10.14:7077")
    .config("spark.hadoop.hive.metastore.uris", "thrift://192.168.10.14:9083")
    .enableHiveSupport()
    .getOrCreate()
)


# data = [("Alice", 28), ("Bob", 36), ("Charlie", 24)]
# columns = ["name", "age"]
# df = spark.createDataFrame(data, columns)

# df.show()

df = spark.sql("select count(*) from crypto_raw.candles_day")
df.show()