#!/bin/bash
set -e

for TOPIC in transactions fraud-rules fraud-alerts; do
  docker exec fraud-kafka kafka-topics \
    --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic $TOPIC \
    --partitions 6 \
    --replication-factor 1
  echo "Ready: $TOPIC"
done

echo ""
docker exec fraud-kafka kafka-topics \
  --list --bootstrap-server localhost:9092
