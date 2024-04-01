# TODO

## 1.Poloniex data streaming (Kafka -> Cassandra)

- as-is
  - Kafka Consumer
  - 3 Partitions for each topic
- to-be
  - Spark Streaming
  - Increase number of partitions
    - candles (356,046)
      - Num: 5
    - market trade (780,826)
      - Num: 10
    - order book (1,764,078)
      - Num: 20

## 2.Data ingestion via YahooFinance API (Python -> Cassandra)

- as-is
  - CQL via Cassandra library via Python
- to-be
  - Store data as Parquet files into the HDFS via Python
  - Create external tables in Hive from the Parquet files

## 3.ETL from Cassandra to Hive

- as-is
  - Trino
- to-be
  - Spark
