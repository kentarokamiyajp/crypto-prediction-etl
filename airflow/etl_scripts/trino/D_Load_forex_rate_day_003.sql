INSERT INTO
    hive.forex_raw.forex_rate_day (
        id,
        low,
        high,
        open,
        close,
        volume,
        adjclose,
        currency,
        unixtime_create,
        dt_create_utc,
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
    unixtime_create,
    dt_create_utc,
    tz_gmtoffset,
    ts_insert_utc,
    year (dt_create_utc),
    month (dt_create_utc),
    day (dt_create_utc)
FROM
    cassandra.forex.forex_rate_day
WHERE
    dt_create_utc >= (date_add('day',${N},current_date))
