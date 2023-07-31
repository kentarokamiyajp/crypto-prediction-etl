import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyspark.sql import SparkSession
from common import env_variables


class Operator:
    def __init__(self):
        SPARK_MASTER_HOST = env_variables.SPARK_MASTER_HOST
        SPARK_MASTER_PORT = env_variables.SPARK_MASTER_PORT
        HIVE_METASTORE_HOST = env_variables.HIVE_METASTORE_HOST
        HIVE_METASTORE_PORT = env_variables.HIVE_METASTORE_PORT

        self.spark = (
            SparkSession.builder.appName("PySpark Hive Operator")
            .config("spark.master", f"spark://{SPARK_MASTER_HOST}:{SPARK_MASTER_PORT}")
            .config(
                "spark.hadoop.hive.metastore.uris",
                f"thrift://{HIVE_METASTORE_HOST}:{HIVE_METASTORE_PORT}",
            )
            .enableHiveSupport()
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("WARN")

    def run_query(self, query):
        return self.spark.sql(query)

