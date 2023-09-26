SELECT 
    COUNT(*) 
FROM
    hive.crypto_raw.market_trade
WHERE
    dt_insert_utc >= (date_add('day',${N},current_date))
