SELECT 
    COUNT(*) 
FROM
    hive.crypto_raw.candles_day
WHERE
    dt_create_utc >= (date_add('day',${N},current_date))
