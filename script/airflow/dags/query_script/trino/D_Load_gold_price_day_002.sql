SELECT 
    COUNT(*) 
FROM
    hive.gold_raw.gold_price_day
WHERE
    dt >= (date_add('day',${N},current_date))
