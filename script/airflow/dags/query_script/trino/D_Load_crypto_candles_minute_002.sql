SELECT 
    COUNT(*) 
FROM
    hive.crypto_raw.candles_minute
WHERE
    dt >= (date_add('day',${N},current_date))
