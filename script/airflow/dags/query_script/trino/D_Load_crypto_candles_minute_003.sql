INSERT INTO
    hive.crypto_raw.candles_minute (
        id,
        low,
        high,
        open,
        close,
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
        year,
        month,
        day,
        hour
    )
SELECT
    id,
    low,
    high,
    open,
    close,
    amount,
    quantity,
    buyTakerAmount,
    buyTakerQuantity,
    tradeCount,
    ts,
    weightedAverage,
    interval,
    startTime,
    closeTime,
    dt,
    ts_insert_utc,
    year (from_unixtime (closeTime)),
    month (from_unixtime (closeTime)),
    day (from_unixtime (closeTime)),
    hour (from_unixtime (closeTime))
FROM
    cassandra.crypto.candles_minute
WHERE
    dt >= (date_add('day',${N},current_date))
