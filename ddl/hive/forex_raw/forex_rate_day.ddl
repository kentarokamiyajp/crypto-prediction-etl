DROP TABLE forex_raw.forex_rate_day;

CREATE TABLE IF NOT EXISTS forex_raw.forex_rate_day (
    id string COMMENT 'id of the forex',
    low float COMMENT 'lowest price over the day',
    high float COMMENT 'highest price over the day',
    open float COMMENT 'price at the start time',
    close float COMMENT 'price at the end time',
    volume float COMMENT 'quote units traded over the day',
    adjclose float COMMENT 'adjusted price at the end time',
    currency string COMMENT 'currency of the price',
    unixtime_create bigint COMMENT 'unix timestamp on the market opened.',
    dt_create_utc date COMMENT 'date when data was created in a trading system',
    tz_gmtoffset int COMMENT 'GMT offset in seconds for dt_create_utc and unixtime_create',
    ts_insert_utc timestamp COMMENT 'timestamp when data is inserted to table in cassandra'
)
COMMENT 'forex rate data for each day'
PARTITIONED BY(year smallint COMMENT 'year data was created in a trading system', 
    month smallint COMMENT 'month data was created in a trading system', 
    day smallint COMMENT 'day data was created in a trading system')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
