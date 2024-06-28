#!/bin/bash

#########################################
#
#   file name:  run_hiveql.sh
#   function:   create external table
#   usage:      run_hiveql.sh <query_file>
#
#########################################

if [ $# != 1 ]; then
    echo "##############################################"
    echo "### Argments Eroor !!!"
    echo "### usage: run_hiveql.sh <query_file>"
    echo "##############################################"
    exit 1
fi

QUERY_FILE=$1

if [ -z "$LOG_FILE" ]; then
    echo "##############################################"
    echo "### Failed LOG_FILE is not set !!!"
    echo "##############################################"
    exit 1
fi

# 1. file check
DEFAULT_CONF="../common/default_conf.sh"
if [ ! -f "$DEFAULT_CONF" ]; then
    echo "##############################################"
    echo "### READ Failded !!! ($DEFAULT_CONF)"
    echo "### Wrong working directory or file not found !!!"
    echo "##############################################"
    exit 1
fi

. $DEFAULT_CONF

QUERY_SCRIPT="${HIVECL_HIVE_OPERATIONS}/etl_scripts/${QUERY_FILE}"

if [ ! -f "$QUERY_SCRIPT" ]; then
    echo "##############################################"
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') READ Failded !!! ($QUERY_SCRIPT )"
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Wrong working directory or file not found !!!"
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') pwd: $(pwd)"
    echo "##############################################"
    exit 1
fi

# main
echo "#####################################################" >>$LOG_FILE
echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') START HiveQL Execution !!!" >>$LOG_FILE
echo "#####################################################" >>$LOG_FILE

query_tmp_file=${HIVECL_LOCAL_DIR}/QUERY_SCRIPT_TMP.sql
eval "echo \"$(cat $QUERY_SCRIPT)\"" >$query_tmp_file

cat $query_tmp_file >>$LOG_FILE

# main
hive --hiveconf hive.exec.max.dynamic.partitions=500000 \
    --hiveconf hive.exec.max.dynamic.partitions.pernode=500000 \
    -f $query_tmp_file 1>>$LOG_FILE 2>>$LOG_FILE

if [ $? -ne 0 ]; then
    echo "##############################################" >>$LOG_FILE
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded HiveQL Execution !!!" >>$LOG_FILE
    echo "##############################################" >>$LOG_FILE
    exit 1
fi

echo "#####################################################" >>$LOG_FILE
echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') END HiveQL Execution !!!" >>$LOG_FILE
echo "#####################################################" >>$LOG_FILE
