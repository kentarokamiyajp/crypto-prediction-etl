CREATE TABLE IF NOT EXISTS forex_rate_history (
    id varchar,
    low float,
    high float,
    open float,
    close float,
    volume float,
    adjclose float,
    currency varchar,
    date_unix bigint,
    dt varchar,
    ts_insert_utc timestamp,
    PRIMARY KEY ((id,dt))
  );
