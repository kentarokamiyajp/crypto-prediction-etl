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

# set date
CURRENT_DAY=$(TZ="Asia/Tokyo" date +'%Y%m%d')
OUTDIR="$HOME/spark/checkpoints/$CURRENT_DAY"

# init directory
mkdir -p "$OUTDIR"
TS_FOR_FILE=$(TZ="Asia/Tokyo" date +'%Y-%m-%dT%H:%M:%S+0900')
SPARK_OFFSET_OUT="${OUTDIR}/spark_latest_offsets_${STREAM_TARGET}_${TS_FOR_FILE}.txt"
OFFSET_DIFF_OUT="${OUTDIR}/offset_diff_${STREAM_TARGET}_${TS_FOR_FILE}.txt"

echo "$(TZ="Asia/Tokyo" date +'%Y-%m-%d_%H:%M:%S') Offset Diff (<Kafka Last Offset> - <Spark Last Offset>)" >>"$OFFSET_DIFF_OUT"

while :; do
    if [ ! "$CURRENT_DAY" -eq "$(TZ="Asia/Tokyo" date +'%Y%m%d')" ]; then
        CURRENT_DAY=$(TZ="Asia/Tokyo" date +'%Y%m%d')
        OUTDIR="$HOME/spark/checkpoints/$CURRENT_DAY"
        mkdir -p "$OUTDIR"
        SPARK_OFFSET_OUT="${OUTDIR}/spark_latest_offsets_${STREAM_TARGET}.txt"
        echo "$(TZ="Asia/Tokyo" date +'%Y-%m-%d_%H:%M:%S') Offset Diff (<Kafka Last Offset> - <Spark Last Offset>)" >>"$OFFSET_DIFF_OUT"
    fi

    #################################
    # Get Spark streaming offsets
    #################################
    LATEST_COMMITED_LOG=$(/home/batch/hadoop-3.3.6/bin/hdfs dfs -ls "/spark/checkpoints/$SPARK_STREAM/commits" | sort -k6,7 | tail -n 1 | awk '{print $NF}')
    LATEST_OFFSET_LOG="$(echo "$LATEST_COMMITED_LOG" | sed "s/commits/offsets/g")"

    /home/batch/hadoop-3.3.6/bin/hdfs dfs -cat "$LATEST_OFFSET_LOG" >"$SPARK_OFFSET_OUT"

    #################################
    # Get Diff betweeen Spark and Kafka offsets
    #################################
    /home/batch/pyvenv/bin/python "../utils/get_offset_diff.py" "$SPARK_OFFSET_OUT" "$KAFKA_TOPIC" >>"$OFFSET_DIFF_OUT" 2>&1

    sleep 60
done
