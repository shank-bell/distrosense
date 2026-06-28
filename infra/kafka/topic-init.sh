#!/bin/bash

KAFKA_BROKER="kafka:29092"
REPLICATION=1

echo "[topic-init] Waiting for Kafka to be ready..."
until kafka-topics --bootstrap-server $KAFKA_BROKER --list > /dev/null 2>&1; do
  echo "[topic-init] Kafka not ready yet, retrying in 5s..."
  sleep 5
done

echo "[topic-init] Kafka is ready. Creating topics..."

kafka-topics --bootstrap-server $KAFKA_BROKER \
  --create --if-not-exists \
  --topic raw-telemetry \
  --partitions 16 \
  --replication-factor $REPLICATION

kafka-topics --bootstrap-server $KAFKA_BROKER \
  --create --if-not-exists \
  --topic aggregated-metrics \
  --partitions 8 \
  --replication-factor $REPLICATION

kafka-topics --bootstrap-server $KAFKA_BROKER \
  --create --if-not-exists \
  --topic anomaly-events \
  --partitions 4 \
  --replication-factor $REPLICATION

kafka-topics --bootstrap-server $KAFKA_BROKER \
  --create --if-not-exists \
  --topic incident-alerts \
  --partitions 2 \
  --replication-factor $REPLICATION

echo "[topic-init] All topics created. Listing:"
kafka-topics --bootstrap-server $KAFKA_BROKER --list