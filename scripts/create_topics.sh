#!/usr/bin/env bash
# Create the input and output topics in the running kafka container.
set -euo pipefail

PARTITIONS="${TOPIC_PARTITIONS:-24}"
TOPIC_IN="${TOPIC_IN:-agent_events}"
TOPIC_OUT="${TOPIC_OUT:-agent_metrics}"

for topic in "$TOPIC_IN" "$TOPIC_OUT"; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions "$PARTITIONS" \
    --topic "$topic"
  echo "ok: $topic"
done

echo "--- topics ---"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
