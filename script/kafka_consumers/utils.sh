#!/bin/bash

################################
# Delete consumer group
################################
KAFKA_BOOTSTRAP_SERVERS="192.168.10.4:9081,192.168.10.4:9082,192.168.10.4:9083"
CONSUMER_GROUP="candles-group-by-minute"
kafka-consumer-groups.sh --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" --delete --group "${CONSUMER_GROUP}"


################################
# Create a new topic
################################
KAFKA_BOOTSTRAP_SERVERS="192.168.10.4:9081,192.168.10.4:9082,192.168.10.4:9083"
TOPIC_ID="crypto.realtime_trading_bot_v5"
NUM_PARTITIONS=1
REPLICATION_FOCTOR=1
RETENTION_DAYS=3
CLEANUP_POLICY=delete
RETENTION_MS=$((RETENTION_DAYS*86400000)) # 1 day = 86400000ms
SEGMENT_MS=86400000

kafka-topics.sh --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
    --topic "${TOPIC_ID}" \
    --create \
    --partitions "${NUM_PARTITIONS}" \
    --replication-factor "${REPLICATION_FOCTOR}" \
    --if-not-exists \
    --config retention.ms="${RETENTION_MS}" \
    --config segment.ms="${SEGMENT_MS}" \
    --config cleanup.policy="${CLEANUP_POLICY}"
