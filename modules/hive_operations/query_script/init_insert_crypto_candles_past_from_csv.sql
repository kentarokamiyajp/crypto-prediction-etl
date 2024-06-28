-- 1. Drop tmp table if exists
DROP TABLE IF EXISTS $SCHEMA_NAME.${TABLE_NAME}_past_${SYMBOL};

-- 2. Create tmp table
CREATE EXTERNAL TABLE IF NOT EXISTS $SCHEMA_NAME.${TABLE_NAME}_past_${SYMBOL} (
    D_Date string,
    AdjClose string,
    Close string,
    High string,
    Low string,
    Open string,
    Volume string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '$HADOOP_HDFS_EXT_DATASET/ext_tables/$SCHEMA_NAME/$TABLE_NAME.db/${SYMBOL}'
TBLPROPERTIES ('skip.header.line.count'='1');

-- 3. Insert data from csv to tmp table.
LOAD DATA INPATH '$SRC_FILE' INTO TABLE $SCHEMA_NAME.${TABLE_NAME}_past_${SYMBOL};


-- 3. Insert data from tmp to main table.
insert into table 
    $SCHEMA_NAME.${TABLE_NAME}_past
partition
(
    year,
    month,
    day,
    hour
)
select
    '$SYMBOL' as id,
    Low as low,
    High as high,
    Open as open,
    Close as close,
    AdjClose as adjclose,
    Volume as volume,
    D_Date as dt,
    CURRENT_TIMESTAMP as ts_insert_utc,
    year(TO_DATE(D_Date)) as year,
    month(TO_DATE(D_Date)) as month,
    day(TO_DATE(D_Date)) as day,
    0 as hour
from 
    $SCHEMA_NAME.${TABLE_NAME}_past_${SYMBOL}
where open != ''
;