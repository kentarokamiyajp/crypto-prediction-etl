CREATE TABLE IF NOT EXISTS forex.forex_rate_day (
    id varchar,
    low float,
    high float,
    open float,
    close float,
    volume float,
    adjclose float,
    currency varchar,
    dt_unix bigint,
    dt date,
    tz_gmtoffset int,
    ts_insert_utc timestamp,
    PRIMARY KEY ((id,dt))
  );
