#!/bin/bash

#########################################
#
#   file name:  main_offset_lags_check.sh
#   function:   Check consumer offset lags.
#   usage:      main_offset_lags_check.sh <consumer_goup_id>
#   example:    sh main_offset_lags_check.sh order-book-consumer
#
#########################################

if [ $# != 1 ]; then
    echo "##############################################"
    echo "Argments Eroor !!!"
    echo "usage: main_offset_lags_check.sh <consumer_goup_id>"
    echo "##############################################"
    exit 1
fi

CONSUMER_GROUP_ID=$1

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

OFFSET_LAGS_CHECK_CONF="./conf/${CONSUMER_GROUP_ID}_offset_lags_check.cf"
if [ ! -f "$DEFAULT_CONF" ]; then
    echo "##############################################"
    echo "### READ Failded !!! ($OFFSET_LAGS_CHECK_CONF)"
    echo "### Wrong working directory or file not found !!!"
    echo "### pwd: $(pwd)"
    echo "##############################################"
    exit 1
fi

# Logging setup
DT_TODAY=$(TZ="Asia/Tokyo" date +'%Y%m%d')
TS_NOW=$(TZ="Asia/Tokyo" date +'%Y-%m-%d_%H:%M:%S')
LOGDIR=${KAFKA_LOG_HOME}/${DT_TODAY}

if [ ! -d "$LOGDIR" ]; then
    mkdir "$LOGDIR"
fi
if [ $? -ne 0 ]; then
    echo "##############################################"
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded to create a directory: ${LOGDIR} !!!"
    echo "##############################################"
    exit 1
fi

LOG_FILE=${LOGDIR}/offset_lags_check_${CONSUMER_GROUP_ID}_${TS_NOW}.log

# Start to check consumer lags
MAIN_SCRIPT=./offset_lags_check.py
if [ ! -f "$MAIN_SCRIPT" ]; then
    echo "##############################################"
    echo "### READ Failded !!! ($MAIN_SCRIPT)"
    echo "### Wrong working directory or file not found !!!"
    echo "### pwd: $(pwd)"
    echo "##############################################"
    exit 1
fi

python "${MAIN_SCRIPT}" "${DT_TODAY}" "${TS_NOW}" "${CONSUMER_GROUP_ID}" >> $LOG_FILE 2>&1 &

if [ $? -ne 0 ]; then
    echo "##############################################" >>$LOG_FILE
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded to start offset lags check !!!" >>$LOG_FILE
    echo "##############################################" >>$LOG_FILE
    exit 1
fi

echo "##############################################" >>$LOG_FILE
echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Completed to start offset lags check !!!" >>$LOG_FILE
echo "##############################################" >>$LOG_FILE
