with cross_table_cnt as
(
    select count(*) as cnt, 'crypto_raw.candles_day' as table_name from {{ source('cross_use__crypto_raw', 'candles_day') }}
    union all
    select count(*) as cnt, 'forex_raw.forex_rate_day' as table_name from {{ source('cross_use__forex_raw', 'forex_rate_day') }}
    union all
    select count(*) as cnt, 'gas_raw.natural_gas_price_day' as table_name from {{ source('cross_use__gas_raw', 'natural_gas_price_day') }}
    union all
    select count(*) as cnt, 'gold_raw.gold_price_day' as table_name from {{ source('cross_use__gold_raw', 'gold_price_day') }}
    union all
    select count(*) as cnt, 'oil_raw.crude_oil_price_day' as table_name from {{ source('cross_use__oil_raw', 'crude_oil_price_day') }}
    union all
    select count(*) as cnt, 'stock_raw.stock_index_day' as table_name from {{ source('cross_use__stock_raw', 'stock_index_day') }}
)
select * from cross_table_cnt