#!/bin/bash

set -e

KAFKA_BROKER="kafka:9092"
TOPIC="orders_events_v2"
MAX_RETRIES=20
RETRY_DELAY=5

echo "🔍 Checking Kafka topic: $TOPIC on $KAFKA_BROKER"

for i in $(seq 1 $MAX_RETRIES); do

    TOPIC_EXISTS=$(python3 -c "
from kafka import KafkaAdminClient
try:
    admin = KafkaAdminClient(bootstrap_servers='$KAFKA_BROKER', request_timeout_ms=5000)
    topics = admin.list_topics()
    print('yes' if '$TOPIC' in topics else 'no')
    admin.close()
except Exception as e:
    print('no')
" 2>/dev/null)

    if [ "$TOPIC_EXISTS" = "yes" ]; then
        echo "✅ Topic $TOPIC confirmed. Starting Flink job..."
        exec python3 flink-jobs/flink_avg_job.py
        exit 0
    fi

    echo "⏳ Attempt $i/$MAX_RETRIES — Topic not ready yet. Retrying in ${RETRY_DELAY}s..."
    sleep $RETRY_DELAY

done

echo "❌ Topic $TOPIC not found after $MAX_RETRIES attempts. Exiting."
exit 1