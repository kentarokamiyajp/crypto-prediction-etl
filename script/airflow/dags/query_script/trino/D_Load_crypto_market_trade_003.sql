INSERT INTO
    hive.crypto_raw.market_trade (
        id,
        trade_id,
        takerSide,
        amount,
        quantity,
        price,
        createTime,
        ts_send,
        dt_insert_utc,
        ts_insert_utc,
        year,
        month,
        day,
        hour
    )
SELECT
    id,
    trade_id,
    takerSide,
    amount,
    quantity,
    price,
    createTime,
    ts_send,
    dt_insert_utc,
    ts_insert_utc,
    year (from_unixtime (ts_send)),
    month (from_unixtime (ts_send)),
    day (from_unixtime (ts_send)),
    hour (from_unixtime (ts_send))
FROM
    cassandra.crypto.market_trade_realtime
WHERE
    dt_insert_utc >= (date_add('day',${N},current_date))
