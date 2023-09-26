SELECT 
    COUNT(*) 
FROM
    hive.stock_raw.stock_index_day
WHERE
    dt_create_utc >= (date_add('day',${N},current_date))
