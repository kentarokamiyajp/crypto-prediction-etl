{{ config(materialized='table') }}

select 'crypto_raw.candles_day' as source_tbl, count(*) as record_count from {{ ref('stg_crypto_raw__candles_day') }}
union all
select 'forex_raw.forex_rate_day' as source_tbl, count(*) as record_count from {{ ref('stg_forex_raw__forex_rate_day') }}
union all
select 'gas_raw.natural_gas_price_day' as source_tbl, count(*) as record_count from {{ ref('stg_gas_raw__natural_gas_price_day') }}
union all
select 'gold_raw.gold_price_day' as source_tbl, count(*) as record_count from {{ ref('stg_gold_raw__gold_price_day') }}
union all
select 'oil_raw.crude_oil_price_day' as source_tbl, count(*) as record_count from {{ ref('stg_oil_raw__crude_oil_price_day') }}
union all
select 'stock_raw.stock_index_day' as source_tbl, count(*) as record_count from {{ ref('stg_stock_raw__stock_index_day') }}
