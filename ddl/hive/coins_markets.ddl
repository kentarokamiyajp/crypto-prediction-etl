CREATE TABLE IF NOT EXISTS crypto.coins_markets (
    last_updated string,
    ath float,
    ath_change_percentage float,
    ath_date string,
    atl float,
    atl_change_percentage float,
    atl_date string,
    circulating_supply float,
    current_price float,
    fully_diluted_valuation float,
    high_24h float,
    image string,
    low_24h float,
    market_cap float,
    market_cap_change_24h float,
    market_cap_change_percentage_24h float,
    market_cap_rank float,
    max_supply float,
    name string,
    price_change_24h float,
    price_change_percentage_24h float,
    roi string,
    symbol string,
    total_supply float,
    total_volume float)
COMMENT 'Coins markets table'
PARTITIONED BY(id string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';