{{
    config(
        materialized = 'incremental',
        on_schema_change='append_new_columns',
        incremental_strategy = 'append',
        delete_key = 'dt_end',
        delete_days= 7,
    )

}}

{% do trino_delete_records(config.get('delete_key'), config.get('delete_days')) %}

with 
    min_max_dt as (
        select
            min(dt_create_utc) as min_dt,
            max(dt_create_utc) as max_dt
        from
            {{ ref('src_crypto_raw__candles_day') }}
    ),
    close_stats_1 as (
        select
            base.id as symbol_id,
            '1' as stat_range,
            base.close as close_today,
            AVG(base.close) OVER (PARTITION BY id ORDER BY base.dt_create_utc ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_close,
            CASE
                WHEN LAG(base.close, 1) OVER (PARTITION BY id ORDER BY base.dt_create_utc) IS NOT NULL THEN
                (base.close - LAG(base.close, 1) OVER (PARTITION BY id ORDER BY base.dt_create_utc)) / LAG(base.close, 1) OVER (PARTITION BY id ORDER BY base.dt_create_utc) * 100
                ELSE
                NULL
            END AS roc,
            date_add('day', -1, calendar.date_day) as dt_start,
            calendar.date_day as dt_end,
            localtimestamp(3) as ts_created,
            localtimestamp(3) as ts_updated
        from
            {{ ref('calendar') }} as calendar
        left outer join {{ ref('src_crypto_raw__candles_day') }} as base
            on calendar.date_day = base.dt_create_utc
        where
            calendar.date_day >= (select min_dt from min_max_dt)
            and calendar.date_day <= (select max_dt from min_max_dt)
    ),
    close_stats_3 as (
        select
            base.id as symbol_id,
            '3' as stat_range,
            base.close as close_today,
            AVG(base.close) OVER (PARTITION BY id ORDER BY base.dt_create_utc ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS avg_close,
            CASE
                WHEN LAG(base.close, 3) OVER (PARTITION BY id ORDER BY base.dt_create_utc) IS NOT NULL THEN
                (base.close - LAG(base.close, 3) OVER (PARTITION BY id ORDER BY base.dt_create_utc)) / LAG(base.close, 3) OVER (PARTITION BY id ORDER BY base.dt_create_utc) * 100
                ELSE
                NULL
            END AS roc,
            date_add('day', -3, calendar.date_day) as dt_start,
            calendar.date_day as dt_end,
            localtimestamp(3) as ts_created,
            localtimestamp(3) as ts_updated
        from
            {{ ref('calendar') }} as calendar
        left outer join {{ ref('src_crypto_raw__candles_day') }} as base
            on calendar.date_day = base.dt_create_utc
        where
            calendar.date_day >= (select min_dt from min_max_dt)
            and calendar.date_day <= (select max_dt from min_max_dt)
    ),
    close_stats_5 as (
        select
            base.id as symbol_id,
            '5' as stat_range,
            base.close as close_today,
            AVG(base.close) OVER (PARTITION BY id ORDER BY base.dt_create_utc ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS avg_close,
            CASE
                WHEN LAG(base.close, 5) OVER (PARTITION BY id ORDER BY base.dt_create_utc) IS NOT NULL THEN
                (base.close - LAG(base.close, 5) OVER (PARTITION BY id ORDER BY base.dt_create_utc)) / LAG(base.close, 5) OVER (PARTITION BY id ORDER BY base.dt_create_utc) * 100
                ELSE
                NULL
            END AS roc,
            date_add('day', -5, calendar.date_day) as dt_start,
            calendar.date_day as dt_end,
            localtimestamp(3) as ts_created,
            localtimestamp(3) as ts_updated
        from
            {{ ref('calendar') }} as calendar
        left outer join {{ ref('src_crypto_raw__candles_day') }} as base
            on calendar.date_day = base.dt_create_utc
        where
            calendar.date_day >= (select min_dt from min_max_dt)
            and calendar.date_day <= (select max_dt from min_max_dt)
    ),
    close_stats_7 as (
        select
            base.id as symbol_id,
            '7' as stat_range,
            base.close as close_today,
            AVG(base.close) OVER (PARTITION BY id ORDER BY base.dt_create_utc ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS avg_close,
            CASE
                WHEN LAG(base.close, 7) OVER (PARTITION BY id ORDER BY base.dt_create_utc) IS NOT NULL THEN
                (base.close - LAG(base.close, 7) OVER (PARTITION BY id ORDER BY base.dt_create_utc)) / LAG(base.close, 7) OVER (PARTITION BY id ORDER BY base.dt_create_utc) * 100
                ELSE
                NULL
            END AS roc,
            date_add('day', -7, calendar.date_day) as dt_start,
            calendar.date_day as dt_end,
            localtimestamp(3) as ts_created,
            localtimestamp(3) as ts_updated
        from
            {{ ref('calendar') }} as calendar
        left outer join {{ ref('src_crypto_raw__candles_day') }} as base
            on calendar.date_day = base.dt_create_utc
        where
            calendar.date_day >= (select min_dt from min_max_dt)
            and calendar.date_day <= (select max_dt from min_max_dt)
    ),
    close_stats_10 as (
        select
            base.id as symbol_id,
            '10' as stat_range,
            base.close as close_today,
            AVG(base.close) OVER (PARTITION BY id ORDER BY base.dt_create_utc ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS avg_close,
            CASE
                WHEN LAG(base.close, 10) OVER (PARTITION BY id ORDER BY base.dt_create_utc) IS NOT NULL THEN
                (base.close - LAG(base.close, 10) OVER (PARTITION BY id ORDER BY base.dt_create_utc)) / LAG(base.close, 10) OVER (PARTITION BY id ORDER BY base.dt_create_utc) * 100
                ELSE
                NULL
            END AS roc,
            date_add('day', -10, calendar.date_day) as dt_start,
            calendar.date_day as dt_end,
            localtimestamp(3) as ts_created,
            localtimestamp(3) as ts_updated
        from
            {{ ref('calendar') }} as calendar
        left outer join {{ ref('src_crypto_raw__candles_day') }} as base
            on calendar.date_day = base.dt_create_utc
        where
            calendar.date_day >= (select min_dt from min_max_dt)
            and calendar.date_day <= (select max_dt from min_max_dt)
    ),
    close_stats_30 as (
        select
            base.id as symbol_id,
            '30' as stat_range,
            base.close as close_today,
            AVG(base.close) OVER (PARTITION BY id ORDER BY base.dt_create_utc ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS avg_close,
            CASE
                WHEN LAG(base.close, 30) OVER (PARTITION BY id ORDER BY base.dt_create_utc) IS NOT NULL THEN
                (base.close - LAG(base.close, 30) OVER (PARTITION BY id ORDER BY base.dt_create_utc)) / LAG(base.close, 30) OVER (PARTITION BY id ORDER BY base.dt_create_utc) * 100
                ELSE
                NULL
            END AS roc,
            date_add('day', -30, calendar.date_day) as dt_start,
            calendar.date_day as dt_end,
            localtimestamp(3) as ts_created,
            localtimestamp(3) as ts_updated
        from
            {{ ref('calendar') }} as calendar
        left outer join {{ ref('src_crypto_raw__candles_day') }} as base
            on calendar.date_day = base.dt_create_utc
        where
            calendar.date_day >= (select min_dt from min_max_dt)
            and calendar.date_day <= (select max_dt from min_max_dt)
    ),
    tmp_crypto_daily_close_stats as (
        select * from close_stats_1
        union all
        select * from close_stats_3
        union all
        select * from close_stats_5
        union all
        select * from close_stats_7
        union all
        select * from close_stats_10
        union all
        select * from close_stats_30
    ),
    crypto_daily_close_stats as (
        select
            *
        from
            tmp_crypto_daily_close_stats
        where
            dt_end >= date_add('day', {{ config.get('delete_days') }}, current_date)
    )
select * from crypto_daily_close_stats
