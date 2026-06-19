#!/usr/bin/env bash
# Read the per-minute aggregates from the output topic (to check results).
set -euo pipefail

TOPIC_OUT="${TOPIC_OUT:-agent_metrics}"

docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC_OUT" \
  --from-beginning
