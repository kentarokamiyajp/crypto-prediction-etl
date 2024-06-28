#!/bin/bash

#########################################
#
#   file name:  init_copy_crypto_past_data_to_hdfs.sh
#   function:   copy past crypto data file from local to hdfs
#   usage:      init_copy_crypto_past_data_to_hdfs.sh
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

COPY_TO_HDFS="functions/copy_to_hdfs.sh"
if [ ! -f "$COPY_TO_HDFS" ]; then
    echo "##############################################"
    echo "### READ Failded !!! ($COPY_TO_HDFS)"
    echo "### Wrong working directory or file not found !!!"
    echo "##############################################"
    exit 1
fi

# 2. Copy directory that contains target files from local (hadoop docker container) to HDFS
SRC_DIR="$HADOOP_LOCAL_EXT_DATASET/crypto_past_data"
DEST_DIR="$HADOOP_HDFS_EXT_DATASET/src_data"
SRC_TYPE="-d"

/bin/bash $COPY_TO_HDFS $SRC_TYPE $SRC_DIR $DEST_DIR

if [ $? -ne 0 ]; then
    echo "##############################################"
    echo "### Failded to copy local data (${SRC_DIR}) to hdfs (${DEST_DIR}) !!!"
    echo "##############################################"
    exit 1
fi
