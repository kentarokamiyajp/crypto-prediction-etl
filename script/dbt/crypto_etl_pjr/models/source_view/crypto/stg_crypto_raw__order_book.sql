{{ config(materialized='view') }}

select * from {{ source('crypto_raw', 'order_book') }}
