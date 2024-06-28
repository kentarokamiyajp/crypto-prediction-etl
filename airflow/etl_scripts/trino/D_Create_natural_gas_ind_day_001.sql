DELETE FROM
    hive.gas_mart.natural_gas_indicator_day
WHERE
    year = year(cast('${target_date}' as date))
    and month = month(cast('${target_date}' as date))
