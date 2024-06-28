DELETE FROM
    hive.oil_mart.crude_oil_indicator_day
WHERE
    year = year(cast('${target_date}' as date))
    and month = month(cast('${target_date}' as date))
