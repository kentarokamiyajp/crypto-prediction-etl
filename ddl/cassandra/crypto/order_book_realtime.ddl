DROP TABLE crypto.order_book_realtime;

-- retention period: 864000 (10 days)
CREATE TABLE IF NOT EXISTS crypto.order_book_realtime (
    id varchar,
    seqid bigint,
    order_type varchar,
    quote_price float,
    base_amount float,
    pos int,
    createTime bigint,
    ts_send bigint,
    dt_create_utc date,
    ts_create_utc timestamp,
    ts_insert_utc timestamp,
    PRIMARY KEY ((id, dt_create_utc),seqid,order_type,pos)
) WITH default_time_to_live=2592000 and gc_grace_seconds=3600;
