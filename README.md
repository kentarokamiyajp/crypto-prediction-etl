# Data Warehouse Architecture for Crypto Price Prediction
![Screenshot 2023-09-27 at 21 42 43](https://github.com/kamikennn/crypto_prediction_dwh/assets/45506894/8a1235a8-5f57-4efa-9d5f-53701bc2ebda)
## System Overview
- https://miro.com/app/board/uXjVMgqA6r8=/?share_link_id=394438367657
## Source of Crypto Price and other useful data
- Poloniex (https://api-docs.poloniex.com/spot)
  - Crypto price data
- Yahoo Finance (https://finance.yahoo.com/)
  - Forex price data
  - Stack index data
  - Others
## Realtime Price Data Streaming
- Data from Poloniex Websocket & Rest API
  - Order book
  - Market trade
  - Crypto price for every minute
- Tools
  - Kafka Producers and Consumers
  - 3 Kafka brokers
## Source Database
- Cassandra
  - 3 seed nodes
  - Fast to write real-time data.
## Data Warehouse system
- Data Storage
  - Hadoop
    - 3 data nodes
  - Hive
- ETL tools
  - Spark
    - 5 workers
  - Python
  - Trino
## Job Scheduling tool
- Airflow
