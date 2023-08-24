CREATE TABLE IF NOT EXISTS crypto.order_book_realtime (
    id varchar,
    seqid bigint,
    order_type varchar,
    quote_price float,
    base_amount float,
    order_rank int,
    createTime bigint,
    ts_send bigint,
    dt_insert_utc date,
    ts_insert_utc timestamp,
    PRIMARY KEY ((id, dt_insert_utc, order_type, seqid), order_rank)
);
