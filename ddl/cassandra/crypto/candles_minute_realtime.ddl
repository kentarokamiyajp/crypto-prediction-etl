DROP TABLE crypto.candles_minute_realtime;

CREATE TABLE IF NOT EXISTS crypto.candles_minute_realtime (
    id varchar,
    low float,
    high float,
    open float,
    close float,
    amount float,
    quantity float,
    buyTakerAmount float,
    buyTakerQuantity float,
    tradeCount int,
    ts bigint,
    weightedAverage float,
    interval varchar,
    startTime bigint,
    closeTime bigint,
    dt_insert_utc date,
    ts_insert_utc timestamp,
    PRIMARY KEY ((id,dt_insert_utc),startTime,closeTime)
  ) WITH default_time_to_live = 7776000;
