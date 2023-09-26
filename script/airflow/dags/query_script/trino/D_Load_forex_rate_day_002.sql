SELECT 
    COUNT(*) 
FROM
    hive.forex_raw.forex_rate_day
WHERE
    dt_create_utc >= (date_add('day',${N},current_date))
