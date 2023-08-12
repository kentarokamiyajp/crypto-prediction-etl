DROP TABLE crypto_raw.candles_minute;

CREATE TABLE IF NOT EXISTS crypto_raw.candles_minute (
    id string COMMENT 'id of the crypto currency',
    low float COMMENT 'lowest price over the interval',
    high float COMMENT 'highest price over the interval',
    open float COMMENT 'price at the start time',
    close float COMMENT 'price at the end time',
    amount float COMMENT 'quote(e.g., USDT) units traded over the interval',
    quantity float COMMENT 'base(e.g., BTC) units traded over the interval',
    buyTakerAmount float COMMENT 'quote units traded over the interval filled by market buy orders',
    buyTakerQuantity float COMMENT 'base units traded over the interval filled by market buy orders',
    tradeCount int COMMENT 'count of trades',
    ts bigint COMMENT 'time the record was pushed',
    weightedAverage float COMMENT 'weighted average over the interval',
    interval_type string COMMENT 'the unit of time to aggregate data by. E.g., MINUTE_1, HOUR_1, DAY_1, WEEK_1 and MONTH_1',
    startTime bigint COMMENT 'start time of interval (utc unix timestamp market started)',
    closeTime bigint COMMENT 'close time of interval (utc unix timestamp market closed)',
    dt date COMMENT 'date market closed',
    ts_insert_utc timestamp COMMENT 'timestamp when data is inserted to table in cassandra'
)
COMMENT 'crypto candles data for each minute'
PARTITIONED BY(year smallint COMMENT 'year at the market opening', 
    month smallint COMMENT 'month at the market opening', 
    day smallint COMMENT 'day at the market opening', 
    hour smallint COMMENT 'hour at the market opening')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
