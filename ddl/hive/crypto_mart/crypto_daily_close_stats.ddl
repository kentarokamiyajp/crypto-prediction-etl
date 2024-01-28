DROP TABLE crypto_mart.crypto_daily_close_stats;

CREATE TABLE IF NOT EXISTS crypto_mart.crypto_daily_close_stats (
    symbol_id string,
    stat_range varchar(10),
    close_today float,
    avg_close float,
    roc float,
    dt_start date,
    ts_created timestamp,
    ts_updated timestamp
)
COMMENT 'crypto daily close stats'
PARTITIONED BY(dt_end date)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
