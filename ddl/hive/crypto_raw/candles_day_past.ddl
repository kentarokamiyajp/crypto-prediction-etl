DROP TABLE crypto_raw.candles_day_past;

CREATE TABLE IF NOT EXISTS crypto_raw.candles_day_past (
    id string COMMENT 'id of the crypto currency',
    low float COMMENT 'lowest price over the interval',
    high float COMMENT 'highest price over the interval',
    open float COMMENT 'price at the start time',
    close float COMMENT 'price at the end time',
    adjclose float COMMENT 'adjusted price at the end time',
    volume float COMMENT 'quote units traded over the day',
    dt date COMMENT 'date market closed',
    ts_insert_utc timestamp COMMENT 'timestamp when data is inserted to table in cassandra'
)
COMMENT 'crypto candles past data for each day'
PARTITIONED BY(year smallint COMMENT 'year at the market opening', 
    month smallint COMMENT 'month at the market opening', 
    day smallint COMMENT 'day at the market opening')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");