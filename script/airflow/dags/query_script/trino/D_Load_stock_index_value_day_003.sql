INSERT INTO
    hive.stock_raw.stock_index_day (
        id,
        low,
        high,
        open,
        close,
        volume,
        adjclose,
        currency,
        dt_unix,
        dt,
        tz_gmtoffset,
        ts_insert_utc,
        year,
        month,
        day
    )
SELECT
    id,
    low,
    high,
    open,
    close,
    volume,
    adjclose,
    currency,
    dt_unix,
    dt,
    tz_gmtoffset,
    ts_insert_utc,
    year (from_unixtime (dt_unix)),
    month (from_unixtime (dt_unix)),
    day (from_unixtime (dt_unix))
FROM
    cassandra.stock.stock_index_day
WHERE
    dt >= (date_add('day',${N},current_date))
