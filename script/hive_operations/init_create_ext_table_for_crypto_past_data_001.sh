#!/bin/bash

#########################################
#
#   file name:  init_create_ext_table_for_crypto_past_data_001.sh
#   function:   Create Hive tables for crypto past data
#   usage:      init_create_ext_table_for_crypto_past_data_001.sh
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

# set log file path
LOG_MAIN_NAME="init_create_ext_table_for_crypto_past_data_001"
LOG_FILE="${SUB_LOG_DIR}/${LOG_MAIN_NAME}_$(TZ=Japan date +'%Y%m%d-%H%M%S').log"
export LOG_FILE

QUERY_SCRIPT="init_create_crypto_candles_past_table.sql"

# 2. Create Hive tables with no data
echo "##############################################" >>$LOG_FILE
echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') START to create Hive tables !!!" >>$LOG_FILE
echo "##############################################" >>$LOG_FILE

# main
/bin/bash $HIVECL_HIVE_OPERATIONS/functions/run_hiveql.sh $QUERY_SCRIPT 1>>$LOG_FILE 2>>$LOG_FILE

if [ $? -ne 0 ]; then
    echo "##############################################" >>$LOG_FILE
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded to create Hive tables !!!" >>$LOG_FILE
    echo "##############################################" >>$LOG_FILE
    exit 1
fi

echo "##############################################" >>$LOG_FILE
echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') END to create Hive tables !!!" >>$LOG_FILE
echo "##############################################" >>$LOG_FILE
