SELECT 
    COUNT(*) 
FROM
    hive.gold_raw.gold_price_day
WHERE
    dt_create_utc >= (date_add('day',${N},current_date))
