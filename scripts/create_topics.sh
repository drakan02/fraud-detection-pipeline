#!/bin/bash
# Creates Kafka topics inside the Kubernetes Kafka pod.
# Usage: ./scripts/create_topics.sh
set -e

for TOPIC in transactions fraud-alerts; do
  kubectl exec deploy/kafka -- kafka-topics \
    --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC" \
    --partitions 6 \
    --replication-factor 1
  echo "Ready: $TOPIC"
done

echo ""
kubectl exec deploy/kafka -- kafka-topics \
  --list --bootstrap-server localhost:9092
