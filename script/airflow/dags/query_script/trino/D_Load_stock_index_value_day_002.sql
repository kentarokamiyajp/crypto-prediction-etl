SELECT 
    COUNT(*) 
FROM
    hive.stock_raw.stock_index_day
WHERE
    dt >= (date_add('day',${N},current_date))
