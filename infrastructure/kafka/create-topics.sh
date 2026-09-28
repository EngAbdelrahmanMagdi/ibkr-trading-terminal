#!/bin/sh
# Creates the Kafka topics (idempotent: existing topics are left unchanged). Brokers never auto-create topics.
#
#   topic                          owner              key                ordering
#   trading.order-events.v1        Trading Core       accountId:orderId  all events of one order, in order
#   trading.execution-events.v1    Trading Core       accountId:orderId  fills of one order, in order
#
# Local single broker: replication factor 1. Retention 7 days; the outbox in PostgreSQL, not Kafka retention,
# is the durability guarantee for these events.
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:29092}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
REPLICATION="${KAFKA_TOPIC_REPLICATION:-1}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:-604800000}"

for topic in trading.order-events.v1 trading.execution-events.v1; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$PARTITIONS" --replication-factor "$REPLICATION" \
    --config cleanup.policy=delete --config retention.ms="$RETENTION_MS" --config min.insync.replicas=1
  echo "topic ready: $topic"
done
