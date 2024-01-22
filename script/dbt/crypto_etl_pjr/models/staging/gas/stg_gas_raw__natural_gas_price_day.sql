{{ config(materialized='view') }}

select * from {{ source('gas_raw', 'natural_gas_price_day') }}
