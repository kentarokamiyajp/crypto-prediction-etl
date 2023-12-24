DROP TABLE crypto_raw.candles_realtime;

CREATE TABLE IF NOT EXISTS crypto_raw.candles_realtime (
    id string COMMENT 'id of the crypto currency',
    low float COMMENT 'lowest price over the interval',
    high float COMMENT 'highest price over the interval',
    open float COMMENT 'price at the start time',
    close float COMMENT 'price at the end time',
    amount float COMMENT 'quote(e.g., USDT) units traded over the interval',
    quantity float COMMENT 'base(e.g., BTC) units traded over the interval',
    tradeCount int COMMENT 'count of trades',
    startTime bigint COMMENT 'start time of interval (utc unix timestamp market started)',
    closeTime bigint COMMENT 'close time of interval (utc unix timestamp market closed)',
    ts_send bigint COMMENT 'time the record was pushed',
    dt_create_utc date COMMENT 'date when data was created in a trading system (based on ts_send)',
    ts_create_utc timestamp COMMENT 'timestamp when data was created in a trading system (based on ts_send)',
    ts_insert_utc timestamp COMMENT 'timestamp when data is inserted to table in cassandra',
    minute smallint COMMENT 'minute at the candle data was created (based on ts_send)',
    second smallint COMMENT 'second at the candle data was created (based on ts_send)'
)
COMMENT 'crypto candles data for each minute'
PARTITIONED BY(year smallint COMMENT 'year data was created in a trading system (based on ts_send)',
    month smallint COMMENT 'month data was created in a trading system (based on ts_send)',
    day smallint COMMENT 'day data was created in a trading system (based on ts_send)',
    hour smallint COMMENT 'hour data was created in a trading system (based on ts_send)')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
