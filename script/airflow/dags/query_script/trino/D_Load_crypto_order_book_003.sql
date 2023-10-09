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
        dt_create_utc,
        ts_create_utc,
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
    dt_create_utc,
    ts_create_utc,
    ts_insert_utc,
    year(ts_create_utc),
    month(ts_create_utc),
    day(ts_create_utc),
    hour(ts_create_utc)
FROM
    cassandra.crypto.order_book_realtime
WHERE
    id = '${symbol}'
    and dt_create_utc = (date_add('day',${N},current_date))
