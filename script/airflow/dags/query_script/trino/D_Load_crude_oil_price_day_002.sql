SELECT 
    COUNT(*) 
FROM
    hive.oil_raw.crude_oil_price_day
WHERE
    dt >= (date_add('day',${N},current_date))
