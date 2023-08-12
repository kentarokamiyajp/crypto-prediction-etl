DELETE FROM
    hive.forex_raw.forex_rate_day
WHERE
    year = year(date_add('day',${N},current_date))
    and month = month(date_add('day',${N},current_date))
    and day = day(date_add('day',${N},current_date))
