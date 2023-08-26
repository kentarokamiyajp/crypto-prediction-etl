DROP TABLE crypto_mart.crypto_indicator_day;

CREATE TABLE IF NOT EXISTS crypto_mart.crypto_indicator_day (
    id string COMMENT 'id of the crypto currency',
    dt date COMMENT 'market opening date',
    low float COMMENT 'lowest price',
    high float COMMENT 'highest price',
    open float COMMENT 'price at the start time',
    close float COMMENT 'price at the end time',
    volume float COMMENT 'volume of the day',
    macd float COMMENT 'The MACD line is the difference between slow and fast moving averages (macd = fast_ema - slow_ema)',
    macd_single float COMMENT 'Moving average of the macd line',
    rsi float COMMENT 'Relative Strength Index (RSI)',
    bollinger_bands_sma float COMMENT 'Simple moving average (SMA) of Close price (center line)',
    bollinger_bands_lower_band float COMMENT 'Upper line is D standard deviations above the SMA',
    bollinger_bands_upper_band float COMMENT 'Lower line is D standard deviations below the SMA',
    obv float COMMENT 'On-balance Volume',
    obv_sma float COMMENT 'Moving average (SMA) of OBV based on sma_periods periods, if specified',
    ichimoku_chikou_span float COMMENT 'Lagging span',
    ichimoku_kijun_sen float COMMENT 'Base line',
    ichimoku_tenkan_sen float COMMENT 'Conversion / signal line',
    ichimoku_senkou_span_a float COMMENT 'Leading span A',
    ichimoku_senkou_span_b float COMMENT 'Leading span B',
    stoch_oscillator float COMMENT '%K Oscillator over prior N lookback periods',
    stoch_signal float COMMENT '%D Simple moving average of Oscillator',
    stoch_percent_j float COMMENT '%J is the weighted divergence of %K and %D: %J=kFactor×%K-dFactor×%D',
    aroon_up float COMMENT 'Based on last High price',
    aroon_down float COMMENT 'Based on last Low price',
    aroon_oscillator float COMMENT 'AroonUp - AroonDown',
    N_multiple float COMMENT 'If the OHLC value is too small, it is multiplied by N'
)
COMMENT 'crypto indicator for candles data for each day'
PARTITIONED BY(year smallint COMMENT 'year at the market opening', 
    month smallint COMMENT 'month at the market opening', 
    day smallint COMMENT 'day at the market opening')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");
