#!/bin/sh
set -eu
broker=${KAFKA_BOOTSTRAP:-kafka:29092}
acl() { /opt/kafka/bin/kafka-acls.sh --bootstrap-server "$broker" --command-config /scripts/client.properties --add "$@" >/dev/null; }
produce() { principal=$1; shift; for topic in "$@"; do acl --allow-principal "User:$principal" --operation Write --operation Describe --topic "$topic"; done; }
consume() { principal=$1; group=$2; shift 2; acl --allow-principal "User:$principal" --operation Read --operation Describe --group "$group"; for topic in "$@"; do acl --allow-principal "User:$principal" --operation Read --operation Describe --topic "$topic"; done; }
produce trading-core trading.order-events.v1 trading.execution-events.v1 news.raw.v1 news.enriched.v1.dlq
consume trading-core trading-core-broker-updates broker.order-updates.v1
consume trading-core trading-core-news-enrichment news.enriched.v1
consume realtime-gateway realtime-gateway-order-notifications trading.order-events.v1
produce realtime-gateway broker.order-updates.v1
consume ai-insights ai-insights-news news.raw.v1
consume ai-insights ai-insights-news-restore news.ai-state.v1
produce ai-insights news.enriched.v1 news.ai-state.v1
acl --allow-principal User:ai-insights --operation Write --operation Describe --transactional-id ai-insights-news-singleton
for principal in trading-core realtime-gateway; do acl --allow-principal "User:$principal" --operation IdempotentWrite --cluster; done
printf '%s\n' 'Service ACLs ready'
