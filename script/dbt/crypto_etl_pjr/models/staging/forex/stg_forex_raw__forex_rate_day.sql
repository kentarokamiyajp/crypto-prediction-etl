{{ config(materialized='view') }}

select * from {{ source('forex_raw', 'forex_rate_day') }}
