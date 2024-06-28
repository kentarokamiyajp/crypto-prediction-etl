DELETE FROM
    hive.gold_mart.gold_indicator_day
WHERE
    year = year(cast('${target_date}' as date))
    and month = month(cast('${target_date}' as date))
