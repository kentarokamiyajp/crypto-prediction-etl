#!/bin/bash

#########################################
#
#   file name:  exec_producer.sh
#   function:   Create a Kafka Producer and start producing.
#   usage:      exec_producer.sh <producer_id>
#   example:    sh exec_producer.sh candles_minute_producer
#
#########################################

if [ $# != 1 ]; then
    echo "##############################################"
    echo "Argments Eroor !!!"
    echo "usage: exec_producer.sh <producer_id>"
    echo "##############################################"
    exit 1
fi

PRODUCER_ID=$1

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

PRODUCER_CONF="./conf/${PRODUCER_ID}.cf"
if [ ! -f "$DEFAULT_CONF" ]; then
    echo "##############################################"
    echo "### READ Failded !!! ($PRODUCER_CONF)"
    echo "### Wrong working directory or file not found !!!"
    echo "### pwd: $(pwd)"
    echo "##############################################"
    exit 1
fi

. $PRODUCER_CONF


# Logging setup
DT_TODAY=$(TZ="Asia/Tokyo" date +'%Y%m%d')
TS_NOW=$(TZ="Asia/Tokyo" date +'%Y-%m-%d_%H:%M:%S')
LOGDIR=${KAFKA_LOG_HOME}/${DT_TODAY}

if [ ! -d "$LOGDIR" ]; then
    mkdir -p "$LOGDIR"
fi
if [ $? -ne 0 ]; then
    echo "##############################################"
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded to create a directory: ${LOGDIR} !!!"
    echo "##############################################"
    exit 1
fi

LOG_FILE=${LOGDIR}/${PRODUCER_ID}_${TS_NOW}.log

# 1 day = 86400000ms
RETENTION_MS=$((RETENTION_DAYS*86400000))
SEGMENT_MS=86400000

# Create a topic if not exist.
kafka-topics.sh --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
    --topic "${TOPIC_ID}" \
    --create \
    --partitions "${NUM_PARTITIONS}" \
    --replication-factor "${REPLICATION_FOCTOR}" \
    --if-not-exists \
    --config retention.ms="${RETENTION_MS}" \
    --config segment.ms="${SEGMENT_MS}" \
    --config cleanup.policy="${CLEANUP_POLICY}" \
    1>>$LOG_FILE 2>>$LOG_FILE

if [ $? -ne 0 ]; then
    echo "##############################################" >>$LOG_FILE
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded to create a Kafka Producer !!!" >>$LOG_FILE
    echo "##############################################" >>$LOG_FILE
    exit 1
fi

# Start a producer
MAIN_SCRIPT=./${PRODUCER_ID}.py
if [ ! -f "$MAIN_SCRIPT" ]; then
    echo "##############################################"
    echo "### READ Failded !!! ($MAIN_SCRIPT)"
    echo "### Wrong working directory or file not found !!!"
    echo "### pwd: $(pwd)"
    echo "##############################################"
    exit 1
fi

python "${MAIN_SCRIPT}" "${DT_TODAY}" "${TS_NOW}" "${PRODUCER_ID}"  >> $LOG_FILE 2>&1 &

if [ $? -ne 0 ]; then
    echo "##############################################" >>$LOG_FILE
    echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Failded to start a Kafka Producer !!!" >>$LOG_FILE
    echo "##############################################" >>$LOG_FILE
    exit 1
fi

echo "##############################################" >>$LOG_FILE
echo "### $(TZ=Japan date +'%Y-%m-%d %H:%M:%S') Completed to start a Kafka Producer !!!" >>$LOG_FILE
echo "##############################################" >>$LOG_FILE



##########################
# Change retention period
##########################

# kafka-configs.sh --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
#     --alter \
#     --topic "${TOPIC_ID}" \
#     --add-config "retention.ms=${RETENTION_MS}, segment.ms=${SEGMENT_MS}"