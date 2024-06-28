SELECT 
    COUNT(*) 
FROM
    hive.gas_raw.natural_gas_price_day
WHERE
    dt_create_utc >= (date_add('day',${N},current_date))
