INSERT INTO
    hive.forex_raw.forex_rate_day (
        id,
        low,
        high,
        open,
        CLOSE,
        volume,
        adjclose,
        currency,
        dt_unix,
        dt,
        tz_gmtoffset,
        ts_insert_utc,
        YEAR,
        MONTH,
        DAY
    )
SELECT
    id,
    low,
    high,
    open,
CLOSE,
volume,
adjclose,
currency,
dt_unix,
dt,
tz_gmtoffset,
ts_insert_utc,
YEAR (from_unixtime (dt_unix)),
MONTH (from_unixtime (dt_unix)),
DAY (from_unixtime (dt_unix))
FROM
    cassandra.forex.forex_rate_day
WHERE
    dt > (
        SELECT
            COALESCE(MAX(dt), CAST('1111-01-01' AS date))
        FROM
            hive.forex_raw.forex_rate_day
    )