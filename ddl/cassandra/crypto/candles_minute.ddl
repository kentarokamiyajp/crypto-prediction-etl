DROP TABLE crypto.candles_minute;

CREATE TABLE IF NOT EXISTS crypto.candles_minute (
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
    dt_create_utc date,
    ts_create_utc timestamp,
    ts_insert_utc timestamp,
    PRIMARY KEY ((id,dt_create_utc),startTime,closeTime)
  );
