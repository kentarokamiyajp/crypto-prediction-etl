DELETE FROM
    hive.crypto_raw.candles_minute
WHERE
    year = year(date_add('day',${N},current_date))
    and month = month(date_add('day',${N},current_date))
    and day = day(date_add('day',${N},current_date))
