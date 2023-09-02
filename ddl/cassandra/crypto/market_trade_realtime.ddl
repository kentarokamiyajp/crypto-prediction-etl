DROP TABLE crypto.market_trade_realtime;

CREATE TABLE IF NOT EXISTS crypto.market_trade_realtime (
    id varchar,
    trade_id bigint,
    takerSide varchar,
    amount float,
    quantity float,
    price float,
    createTime bigint,
    ts_send bigint,
    dt_insert_utc date,
    ts_insert_utc timestamp,
    PRIMARY KEY ((id, trade_id, takerSide), createTime)
) WITH default_time_to_live = 7776000;
