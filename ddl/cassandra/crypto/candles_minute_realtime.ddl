DROP TABLE crypto.candles_minute_realtime;

-- retention period: 864000 (10 days)
CREATE TABLE IF NOT EXISTS crypto.candles_minute_realtime (
    id varchar,
    low float,
    high float,
    open float,
    close float,
    amount float,
    quantity float,
    tradeCount int,
    startTime bigint,
    closeTime bigint,
    ts_send bigint,
    dt_create_utc date,
    ts_create_utc timestamp,
    ts_insert_utc timestamp,
    PRIMARY KEY ((id,dt_create_utc),startTime)
  ) WITH default_time_to_live = 864000;
