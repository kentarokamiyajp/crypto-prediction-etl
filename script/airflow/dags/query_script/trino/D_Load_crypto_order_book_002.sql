SELECT 
    COUNT(*) 
FROM
    hive.crypto_raw.order_book
WHERE
    dt >= (date_add('day',${N},current_date))
