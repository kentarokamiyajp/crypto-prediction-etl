insert into hive.forex_raw.forex_rate_day
(
    id,
    low,
    high,
    open,
    close,
    volume,
    adjclose,
    currency,
    dt_unix,
    dt,
    tz_gmtoffset,
    ts_insert_utc,
    year,
    month,
    day
)
select
    id,
    low,
    high,
    open,
    close,
    volume,
    adjclose,
    currency,
    dt_unix,
    dt,
    tz_gmtoffset,
    ts_insert_utc,
    year(from_unixtime(dt_unix)),
    month(from_unixtime(dt_unix)),
    day(from_unixtime(dt_unix))
from cassandra.forex.forex_rate_day
where 
    dt > (select COALESCE(max(dt), cast('1111-01-01' as date)) from hive.forex_raw.forex_rate_day)