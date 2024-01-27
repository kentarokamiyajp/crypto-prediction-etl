# NEW MART CREATION with DBT

## Daily Report Specification

### crypto_daily_close_stats

- New table name
  - crypto_daily_close_stats
- Table description
  - Crypto daily stats for 1,3,5,7,10,30 days of scope.
- New column specification
  - symbol_id
    - E.g., BTC_USDT
  - dt_start
    - Start date which is used for the stat calculation.
  - dt_end
    - End date which is used for the stat calculation.
  - stat_range
    - Range category when calculating stats.
    - Values might be; 3,5,7,10,30
  - avg_close
    - sum(close) / int(stat_range)
      - sum(close): close-day-1 + ... + close-day-N
      - N: stat_range
  - roc_close
    - roc: Rate of Change
    - ((close-today / close-stat_range-ago) - 1.0) * 100.0
  - ts_created
  - ts_updated
- Source table
  - database: crypto_raw
  - table: candles_day
- Insert new data
  - Incrementally insert new data
    - Use 'incremental' materialization
  - Not dropping table and re-creating table
