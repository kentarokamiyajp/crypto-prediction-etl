SELECT 
    COUNT(*) 
FROM
    hive.crypto_raw.market_trade
WHERE
    dt_create_utc >= (date_add('day',${N},current_date))
