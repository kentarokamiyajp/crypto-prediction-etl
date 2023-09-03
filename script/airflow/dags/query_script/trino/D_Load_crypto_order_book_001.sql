DELETE FROM
    hive.crypto_raw.order_book
WHERE
    year = year(date_add('day',${N},current_date))
    and month = month(date_add('day',${N},current_date))
    and day = day(date_add('day',${N},current_date))
