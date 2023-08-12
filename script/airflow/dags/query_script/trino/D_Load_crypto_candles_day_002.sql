SELECT 
    COUNT(*) 
FROM
    hive.crypto_raw.candles_day
WHERE
    dt >= (date_add('day',${N},current_date))
