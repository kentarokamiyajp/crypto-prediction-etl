SELECT 
    COUNT(*) 
FROM
    hive.crypto_raw.market_trade
WHERE
    dt >= (date_add('day',${N},current_date))
