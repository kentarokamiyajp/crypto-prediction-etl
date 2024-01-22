{{ config(materialized='view') }}

select * from {{ source('stock_raw', 'stock_index_day') }}
