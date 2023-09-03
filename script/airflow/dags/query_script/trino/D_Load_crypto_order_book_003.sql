INSERT INTO
    hive.crypto_raw.order_book (
        id,
        seqid,
        order_type,
        quote_price,
        base_amount,
        order_rank,
        createTime,
        ts_send,
        dt,
        ts_insert_utc,
        year,
        month,
        day,
        hour
    )
SELECT
    id,
    seqid,
    order_type,
    quote_price,
    base_amount,
    order_rank,
    createTime,
    ts_send,
    dt_insert_utc,
    ts_insert_utc,
    year (from_unixtime (ts_send)),
    month (from_unixtime (ts_send)),
    day (from_unixtime (ts_send)),
    hour (from_unixtime (ts_send))
FROM
    cassandra.crypto.order_book_realtime
WHERE
    dt_insert_utc >= (date_add('day',${N},current_date))
