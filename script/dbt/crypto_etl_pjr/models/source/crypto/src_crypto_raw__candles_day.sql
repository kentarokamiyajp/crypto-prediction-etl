{{ config(materialized='view') }}

select * from {{ source('crypto_raw', 'candles_day') }}
