#!/bin/bash

#########################################
#
#   file name:  exec_consumer.sh
#   function:   Create a Kafka Consumer and start consuming.
#   usage:      exec_consumer.sh <consumer_id>
#   example:    sh exec_consumer.sh candles_minute_consumer
#
#########################################

if [ $# != 1 ]; then
    echo "##############################################"
    echo "Argments Eroor !!!"
    echo "usage: exec_consumer.sh <consumer_id>"
    echo "##############################################"
    exit 1
fi

CONSUMER_ID=$1

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

CONSUMER_CONF="./conf/${CONSUMER_ID}.cf"
if [ ! -f "$DEFAULT_CONF" ]; then
    echo "##############################################"
    echo "### READ Failded !!! ($CONSUMER_CONF)"
    echo "### Wrong working directory or file not found !!!"
    echo "### pwd: $(pwd)"
    echo "##############################################"
    exit 1
fi

. $CONSUMER_CONF


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

LOG_FILE=${LOGDIR}/${CONSUMER_ID}_${TS_NOW}.log

# Start a consumer
MAIN_SCRIPT=./${CONSUMER_ID}.py
if [ ! -f "$MAIN_SCRIPT" ]; then
    echo "##############################################"
    echo "### READ Failded !!! ($MAIN_SCRIPT)"
    echo "### Wrong working directory or file not found !!!"
    echo "### pwd: $(pwd)"
    echo "##############################################"
    exit 1
fi

python "${MAIN_SCRIPT}" "${DT_TODAY}" "${TS_NOW}" "${CONSUMER_ID}"  >> $LOG_FILE 2>&1 &

if [ $? -ne 0 ]; then
    echo "##############################################" >>$LOG_FILE
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded to start a Kafka Consumer !!!" >>$LOG_FILE
    echo "##############################################" >>$LOG_FILE
    exit 1
fi

echo "##############################################" >>$LOG_FILE
echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Completed to start a Kafka Consumer !!!" >>$LOG_FILE
echo "##############################################" >>$LOG_FILE
