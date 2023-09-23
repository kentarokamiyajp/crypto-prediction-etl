DROP TABLE stock.stock_index_day;

CREATE TABLE IF NOT EXISTS stock.stock_index_day (
    id varchar,
    low float,
    high float,
    open float,
    close float,
    volume float,
    adjclose float,
    currency varchar,
    unixtime_create bigint,
    dt_create_utc date,
    tz_gmtoffset int,
    ts_insert_utc timestamp,
    PRIMARY KEY ((id,dt_create_utc))
  );
