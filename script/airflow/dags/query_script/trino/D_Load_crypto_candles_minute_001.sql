INSERT INTO
    hive.crypto_raw.candles_minute (
        id,
        low,
        high,
        open,
        CLOSE,
        amount,
        quantity,
        buyTakerAmount,
        buyTakerQuantity,
        tradeCount,
        ts,
        weightedAverage,
        interval_type,
        startTime,
        closeTime,
        dt,
        ts_insert_utc,
        YEAR,
        MONTH,
        DAY,
        HOUR
    )
SELECT
    id,
    low,
    high,
    open,
CLOSE,
amount,
quantity,
buyTakerAmount,
buyTakerQuantity,
tradeCount,
ts,
weightedAverage,
INTERVAL,
startTime,
closeTime,
dt,
ts_insert_utc,
YEAR (from_unixtime (closeTime)),
MONTH (from_unixtime (closeTime)),
DAY (from_unixtime (closeTime)),
HOUR (from_unixtime (closeTime))
FROM
    cassandra.crypto.candles_minute
WHERE
    dt > (
        SELECT
            COALESCE(MAX(dt), CAST('1111-01-01' AS date))
        FROM
            hive.crypto_raw.candles_minute
    )