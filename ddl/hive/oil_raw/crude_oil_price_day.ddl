DROP TABLE oil_raw.crude_oil_price_day;

CREATE TABLE IF NOT EXISTS oil_raw.crude_oil_price_day (
    id string COMMENT 'id of the oil',
    low float COMMENT 'lowest price over the day',
    high float COMMENT 'highest price over the day',
    open float COMMENT 'price at the start time',
    close float COMMENT 'price at the end time',
    volume float COMMENT 'quote units traded over the day',
    adjclose float COMMENT 'adjusted price at the end time',
    currency string COMMENT 'currency of the price',
    dt_unix bigint COMMENT 'unix timestamp on the market opend. this is not alwasy UTC.',
    dt date COMMENT 'date market closed. this is not alwasy UTC.',
    tz_gmtoffset int COMMENT 'GMT offset for dt and dt_unix',
    ts_insert_utc timestamp COMMENT 'timestamp when data is inserted to table in cassandra'
)
COMMENT 'oil price data for each day'
PARTITIONED BY(year smallint COMMENT 'year at the market opening', 
    month smallint COMMENT 'month at the market opening', 
    day smallint COMMENT 'day at the market opening')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
