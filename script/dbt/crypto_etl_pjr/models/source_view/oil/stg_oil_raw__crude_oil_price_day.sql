{{ config(materialized='view') }}

select * from {{ source('oil_raw', 'crude_oil_price_day') }}
