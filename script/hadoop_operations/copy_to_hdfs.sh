#!/bin/bash

#########################################
#
#   file name:  copy_to_hdfs.sh
#   function:   copy file from local to hdfs
#   usage:      copy_to_hdfs.sh <src_type> <src_file> <dst_dir>
#
#########################################

if [ $# != 3 ]; then
    echo "##############################################"
    echo "Argments Eroor !!!"
    echo "usage: copy_to_hdfs.sh <src_type> <src_file/dir> <dst_dir>"
    echo "src_type: dir -> directory, fl -> file"
    echo "##############################################"
    exit 1
fi

SRC_TYEP=$1
SRC_NAME=$2
DEST_DIR=$3

DEFAULT_ENV="${COMMON_HOME}/env_variables.sh"

if [ ! -f "${DEFAULT_ENV}" ]; then
    echo "##############################################"
    echo "${DEFAULT_ENV} READ Failded !!!"
    echo "##############################################"
    exit 1
fi

. "${DEFAULT_ENV}"

SRC_PATH="${EXT_DATASET}/${SRC_NAME}"

# Source file/dir type check
src_type_list=("dir" "fl")
if ! [[ $(echo "${src_type_list[@]}" | fgrep -w "${SRC_TYEP}") ]]; then
    echo "##############################################"
    echo "src_type '${SRC_TYEP}' is wrong !!!"
    echo "src_type: dir -> directory, fl -> file"
    echo "##############################################"
    exit 1
fi

# Source file existance check
if [ "${SRC_TYEP}" = "fl" ]; then
    if [ ! -f "${SRC_PATH}" ]; then
        echo "##############################################"
        echo "${SRC_PATH} READ Failded !!!"
        echo "Check if the src_type is correnct !!! "
        echo "##############################################"
        exit 1
    fi
else
    if [ ! -d "${SRC_PATH}" ]; then
        echo "##############################################"
        echo "${SRC_PATH} READ Failded !!!"
        echo "Check if the src_type is correnct !!! "
        echo "##############################################"
        exit 1
    fi
fi

# Destination directory existance check
hadoop fs -test -d "${DEST_DIR}"
if [ $? -ne 0 ]; then
    echo "Create parent directry[ies]: ${DEST_DIR}"
    hadoop fs -mkdir -p "${DEST_DIR}"

    if [ $? -ne 0 ]; then
        echo "##############################################"
        echo "Failed to create ${DEST_DIR} !!!"
        echo "##############################################"
        exit 1
    fi

fi

########
# main #
########
hadoop fs -copyFromLocal "${SRC_PATH}" "${DEST_DIR}"
if [ $? -ne 0 ]; then
    echo "##############################################"
    echo "Failded to copy ${SRC_PATH} to ${DEST_DIR} !!!"
    echo "##############################################"
    exit 1
fi

echo "COMPLETED !!!"
echo "(copy from ${SRC_PATH} to ${DEST_DIR})"
