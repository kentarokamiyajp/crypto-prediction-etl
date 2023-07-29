#!/bin/bash

#########################################
#
#   file name:  init_create_ext_table_for_crypto_past_data_002.sh
#   function:   copy past crypto data file from local to hdfs
#   usage:      init_create_ext_table_for_crypto_past_data_002.sh
#
#########################################

# 1. file check
DEFAULT_CONF="../common/default_conf.sh"
if [ ! -f "$DEFAULT_CONF" ]; then
    echo "##############################################"
    echo "### READ Failded !!! ($DEFAULT_CONF)"
    echo "### Wrong working directory or file not found !!!"
    echo "### pwd: $(pwd)"
    echo "##############################################"
    exit 1
fi

. $DEFAULT_CONF

if [ ! -d "$HIVECL_LOG_DIR" ]; then
    mkdir $HIVECL_LOG_DIR
fi

SUB_LOG_DIR=$HIVECL_LOG_DIR/$(date +'%m%d%Y')
if [ ! -d "$SUB_LOG_DIR" ]; then
    mkdir $SUB_LOG_DIR
fi

LOG_MAIN_NAME="init_create_ext_table_for_crypto_past_data_002"

# 2. Create external table by loading data from HDFS
SCHEMA_NAME="crypto_raw"
export SCHEMA_NAME
QUERY_SCRIPT="init_insert_crypto_candles_past_from_csv.sql"

for TABLE_NAME in "candles_day" "candles_minute"; do
    for SRC_PATH in $(hadoop fs -ls -R /data/src_data/crypto_past_data/$TABLE_NAME); do
        if [[ $SRC_PATH = *.csv ]]; then
            SRC_FILE=$SRC_PATH
            export SRC_FILE
            export TABLE_NAME

            # set log file path
            LOG_FILE="${SUB_LOG_DIR}/${LOG_MAIN_NAME}_$(TZ=Japan date +'%Y%m%d-%H%M%S').log"
            export LOG_FILE

            # set crypto symbol
            SYMBOL=$(basename $SRC_FILE .csv | awk -F '-' '{ print $1 "-" $2 }')
            export SYMBOL

            echo "##############################################" >>$LOG_FILE
            echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') START to create external table !!!" >>$LOG_FILE
            echo "##############################################" >>$LOG_FILE

            # main
            /bin/bash $HIVECL_HIVE_OPERATIONS/functions/run_hiveql.sh $QUERY_SCRIPT 1>>$LOG_FILE 2>>$LOG_FILE

            if [ $? -ne 0 ]; then
                echo "##############################################" >>$LOG_FILE
                echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded to create external table !!!" >>$LOG_FILE
                echo "##############################################" >>$LOG_FILE
                exit 1
            fi

            echo "##############################################" >>$LOG_FILE
            echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') END to create external table !!!" >>$LOG_FILE
            echo "##############################################" >>$LOG_FILE

            sleep 5
        fi
    done
done
