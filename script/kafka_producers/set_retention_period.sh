#!/bin/bash

# Enter the kafka brocker contianer
docker exec -it kafka1 /bin/bash

# set retention config
# 1 day: 86400000
TOPIC_NAME="crypto.order_book"
RETENSION_MS=86400000
kafka-configs --alter --bootstrap-server localhost:9081 --topic ${TOPIC_NAME} --add-config retention.ms=${RETENSION_MS}
