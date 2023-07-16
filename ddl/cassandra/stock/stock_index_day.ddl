CREATE TABLE IF NOT EXISTS stock.stock_index_day (
    id varchar,
    low float,
    high float,
    open float,
    close float,
    volume float,
    adjclose float,
    currency varchar,
    date_unix bigint,
    dt date,
    ts_insert_utc timestamp,
    PRIMARY KEY ((id,dt))
  );
