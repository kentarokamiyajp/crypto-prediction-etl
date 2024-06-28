DELETE FROM
    hive.gas_raw.natural_gas_price_day
WHERE
    year = year(date_add('day',${N},current_date))
    and month = month(date_add('day',${N},current_date))
    and day = day(date_add('day',${N},current_date))
