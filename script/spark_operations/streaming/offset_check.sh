#!/bin/bash

#########################################
#
#   file name:  offset_check.sh
#   function:   offset check (Kafka <-> Spark)
#   usage:      offset_check.sh <stream_target>
#               -> <stream_target>.py
#   input:      conf/<stream_target>_offset_check.cf
#   example:    sh offset_check.sh candles
#
#########################################

if [ $# != 1 ]; then
    echo "##############################################"
    echo "Argments Eroor !!!"
    echo "usage: main.sh <stream_target>"
    echo "     :-> <stream_target>_offset_check.cf"
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

JOB_CONF="./conf/${STREAM_TARGET}_offset_check.cf"
if [ ! -f "$JOB_CONF" ]; then
    echo "##############################################"
    echo "### READ Failded !!! ($JOB_CONF)"
    echo "### Wrong working directory or file not found !!!"
    echo "### pwd: $(pwd)"
    echo "##############################################"
    exit 1
fi

. $JOB_CONF

# Set current date
CURRENT_DATE=$(TZ="Asia/Tokyo" date +'%Y%m%d')

# Create tmp directory
TMP_DIR="$SPARK_VOLUME_HOME/tmp/$CURRENT_DATE"
mkdir -p "$TMP_DIR"
if [ $? -ne 0 ]; then
    echo "##############################################"
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded to create a directory: ${TMP_DIR} !!!"
    echo "##############################################"
    exit 1
fi

# Create output directory if necessary
OUT_DIR="$SPARK_STREAM_OFFSET_DIFF_HOME/$CURRENT_DATE"
mkdir -p "$OUT_DIR"
if [ $? -ne 0 ]; then
    echo "##############################################"
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded to create a directory: ${OUT_DIR} !!!"
    echo "##############################################"
    exit 1
fi

# Remove existing tmp files
rm -rf "${TMP_DIR:?}/"*"${STREAM_TARGET}"*

# Set a tmp file to write latest offsets in spark streaming
SPARK_OFFSET_TMP="${TMP_DIR}/spark_latest_offsets_${STREAM_TARGET}.txt"

# Set a output file to keep displaying the current offset diff
TS_FOR_FILE=$(TZ="Asia/Tokyo" date +'%Y-%m-%dT%H:%M:%S+0900')
OFFSET_DIFF_OUT="${OUT_DIR}/offset_diff_${STREAM_TARGET}_${TS_FOR_FILE}.txt"

echo "$(TZ="Asia/Tokyo" date +'%Y-%m-%d_%H:%M:%S') Offset Diff (<Kafka Last Offset> - <Spark Last Offset>)" >>"$OFFSET_DIFF_OUT"

while :; do
    # If the date has changed, update $CURRENT_DATE to the next day
    if [ ! "$CURRENT_DATE" -eq "$(TZ="Asia/Tokyo" date +'%Y%m%d')" ]; then
        CURRENT_DATE=$(TZ="Asia/Tokyo" date +'%Y%m%d')
        # Update tmp directory
        TMP_DIR="$SPARK_VOLUME_HOME/tmp/$CURRENT_DATE"
        mkdir -p "$TMP_DIR"
        if [ $? -ne 0 ]; then
            echo "##############################################"
            echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded to create a directory: ${TMP_DIR} !!!"
            echo "##############################################"
            exit 1
        fi
        SPARK_OFFSET_TMP="${TMP_DIR}/spark_latest_offsets_${STREAM_TARGET}.txt"
        echo "$(TZ="Asia/Tokyo" date +'%Y-%m-%d_%H:%M:%S') Offset Diff (<Kafka Last Offset> - <Spark Last Offset>)" >>"$OFFSET_DIFF_OUT"

        # Update output directory
        OUT_DIR="$SPARK_STREAM_OFFSET_DIFF_HOME/$CURRENT_DATE"
        mkdir -p "$OUT_DIR"
        if [ $? -ne 0 ]; then
            echo "##############################################"
            echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded to create a directory: ${OUT_DIR} !!!"
            echo "##############################################"
            exit 1
        fi
        OFFSET_DIFF_OUT="${OUT_DIR}/offset_diff_${STREAM_TARGET}_${TS_FOR_FILE}.txt"
    fi

    #################################
    # Get Spark streaming offsets
    #################################
    LATEST_COMMITED_LOG=$(/home/batch/hadoop-3.3.6/bin/hdfs dfs -ls "/spark/checkpoints/$SPARK_STREAM/commits" | sort -k6,7 | tail -n 1 | awk '{print $NF}')
    LATEST_OFFSET_LOG="$(echo "$LATEST_COMMITED_LOG" | sed "s/commits/offsets/g")"

    /home/batch/hadoop-3.3.6/bin/hdfs dfs -cat "$LATEST_OFFSET_LOG" >"$SPARK_OFFSET_TMP"
    hdfs_result=$?
    if [ $hdfs_result -ne 0 ]; then
        echo "##############################################"
        echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded HDFS command !!!"
        echo "### Executed command: /home/batch/hadoop-3.3.6/bin/hdfs dfs -cat $LATEST_OFFSET_LOG > $SPARK_OFFSET_TMP"
        echo "##############################################"
        exit 1
    fi

    #################################
    # Get Diff betweeen Spark and Kafka offsets
    #################################
    /home/batch/pyvenv/bin/python "../utils/check_offset_diff.py" "$SPARK_OFFSET_TMP" "$KAFKA_TOPIC" >>"$OFFSET_DIFF_OUT" 2>&1
    return_status=$?
    if [ $return_status -ne 0 ]; then
        echo "Failed to check offset diff !!!"
        echo "Check the log file: $OFFSET_DIFF_OUT"
        exit 1
    fi

    sleep 60
done
