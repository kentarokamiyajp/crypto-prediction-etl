DROP TABLE crypto.market_trade_realtime;

-- retention period: 864000 (10 days)
CREATE TABLE IF NOT EXISTS crypto.market_trade_realtime (
    id varchar,
    trade_id bigint,
    takerSide varchar,
    amount float,
    quantity float,
    price float,
    createTime bigint,
    ts_send bigint,
    dt_create_utc date,
    ts_create_utc timestamp,
    ts_insert_utc timestamp,
    PRIMARY KEY ((id, dt_create_utc),trade_id)
) WITH default_time_to_live=2592000 and gc_grace_seconds=3600;
