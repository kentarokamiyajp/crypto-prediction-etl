SELECT 
    COUNT(*) 
FROM
    hive.crypto_raw.order_book
WHERE
    dt_create_utc >= (date_add('day',${N},current_date))
