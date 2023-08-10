SELECT 
    COUNT(*) 
FROM
    hive.forex_raw.forex_rate_day
WHERE
    dt >= (date_add('day',${N},current_date))
