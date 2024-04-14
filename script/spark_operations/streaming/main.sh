#!/bin/bash

#########################################
#
#   file name:  main.sh
#   function:   Start Saprk Streaming
#   usage:      main.sh <stream_target>
#               -> <stream_target>.py
#   example:    sh main.sh candles
#
#########################################

if [ $# != 1 ]; then
    echo "##############################################"
    echo "Argments Eroor !!!"
    echo "usage: main.sh <stream_target>"
    echo "     : -> <stream_target>.py"
    echo "##############################################"
    exit 1
fi

STREAM_TARGET=$1

DEFAULT_CONF="../../common/default_conf.sh"
if [ ! -f "$DEFAULT_CONF" ]; then
    echo "##############################################"
    echo "### READ Failded !!! ($DEFAULT_CONF)"
    echo "### Wrong working directory or file not found !!!"
    echo "### pwd: $(pwd)"
    echo "##############################################"
    exit 1
fi

. $DEFAULT_CONF

# Logging setup
DT_TODAY=$(TZ="Asia/Tokyo" date +'%Y%m%d')
TS_NOW=$(TZ="Asia/Tokyo" date +'%Y-%m-%d_%H:%M:%S')
LOGDIR=${SPARK_LOG_HOME}/${DT_TODAY}

if [ ! -d "$LOGDIR" ]; then
    mkdir -p "$LOGDIR"
fi
if [ $? -ne 0 ]; then
    echo "##############################################"
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded to create a directory: ${LOGDIR} !!!"
    echo "##############################################"
    exit 1
fi

LOG_FILE=${LOGDIR}/spark_streaming_${STREAM_TARGET}_${TS_NOW}.log

# Check main script
MAIN_SCRIPT=./${STREAM_TARGET}.py
if [ ! -f "$MAIN_SCRIPT" ]; then
    echo "##############################################"
    echo "### READ Failded !!! ($MAIN_SCRIPT)"
    echo "### Wrong working directory or file not found !!!"
    echo "### pwd: $(pwd)"
    echo "##############################################"
    exit 1
fi

echo "##############################################"
echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Start Spark Streaming (${STREAM_TARGET}.py) !!!"
echo "##############################################"

/usr/local/bin/python "${MAIN_SCRIPT}" >>$LOG_FILE 2>&1 &

spark_result=$?
if [ $spark_result -ne 0 ]; then
    echo "##############################################" >>$LOG_FILE
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded Spark Streaming !!!" >>$LOG_FILE
    echo "##############################################" >>$LOG_FILE
    exit 1
fi

echo "##############################################" >>$LOG_FILE
echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Completed Spark Streaming !!!" >>$LOG_FILE
echo "##############################################" >>$LOG_FILE

sh offset_check.sh "${STREAM_TARGET}" &

exit 0
