#!/usr/bin/env bash
# Download the Kafka connector for Flink into flink/jars/.
# Connector version matches the course (Flink 2.0): flink-sql-connector-kafka-4.0.1-2.0.jar
set -euo pipefail

JAR="flink-sql-connector-kafka-4.0.1-2.0.jar"
URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/${JAR}"
DEST="flink/jars/${JAR}"

mkdir -p flink/jars
if [[ -f "$DEST" ]]; then
  echo "already present: $DEST"
  exit 0
fi

echo "downloading $URL"
curl -fSL "$URL" -o "$DEST"
echo "saved: $DEST"
