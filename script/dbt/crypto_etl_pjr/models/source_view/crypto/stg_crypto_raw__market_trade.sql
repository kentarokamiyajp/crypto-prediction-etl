{{ config(materialized='view') }}

select * from {{ source('crypto_raw', 'market_trade') }}
