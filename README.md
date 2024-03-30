# Data Warehouse Architecture for Crypto Price Prediction (DWH)
![image](https://github.com/kamikennn/crypto-prediction-dwh/assets/45506894/31ebd4d5-1fa3-4ee7-9a16-bcae99faffd0)
## System Overview
- https://miro.com/app/board/uXjVMgqA6r8=/?share_link_id=394438367657
## Related Repositories
- DevOps
  - https://github.com/kamikennn/crypto_prediction_devops
- ML/DL
  - https://github.com/kamikennn/crypto_prediction_ml_dl
## Source of Crypto Price and Other Useful Data
- Poloniex (https://api-docs.poloniex.com/spot)
  - Crypto price data
- Yahoo Finance (https://finance.yahoo.com/)
  - Forex price data
  - Stack index data
  - Others
## Realtime Data Streaming
- Data from Poloniex via WebSocket
  - Order book
  - Market trade
  - Crypto price for every minute
- Event streaming platform
  - Kafka
- Streaming Data Processing Tools
  - Kafka Consumer
  - Flink
  - Spark Streaming
## Source Database
- Cassandra
- MongoDB
## Data Warehouse System
- Main DB
  - Hive built on HDFS
- ETL tools
  - Spark
  - Trino
  - DBT(?)
## Data Analyzation
- Druid for real-time analysis
- Trino for ad-hoc analysis
## Job Scheduling Tool
- Airflow
