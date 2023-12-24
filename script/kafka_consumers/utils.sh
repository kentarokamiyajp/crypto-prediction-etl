#!/bin/bash

################################
# Delelte consumer group
################################
KAFKA_BOOTSTRAP_SERVERS=""
CONSUMER_GROUP=""
kafka-consumer-groups.sh --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" --delete --group "${CONSUMER_GROUP}"
