{{ config(materialized='view') }}

select * from {{ source('gold_raw', 'gold_price_day') }}
