#!/bin/sh
set -eu
# Trusted cluster identities exist before the authorizer starts. No transient allow-everyone mode.
export KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer
export KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=false
export KAFKA_SUPER_USERS='User:kafka-broker;User:kafka-operator'
# Kafka's replacement group is literal, not a shell variable.
# shellcheck disable=SC2016
export KAFKA_SSL_PRINCIPAL_MAPPING_RULES='RULE:^CN=(kafka-broker|kafka-operator|trading-core|realtime-gateway|ai-insights)$/$1/,DEFAULT'
export KAFKA_SSL_CLIENT_AUTH=required
export KAFKA_SSL_ENABLED_PROTOCOLS=TLSv1.2,TLSv1.3
export KAFKA_SSL_KEYSTORE_TYPE=PEM
export KAFKA_SSL_KEYSTORE_LOCATION=/run/secrets/kafka_broker_keystore
export KAFKA_SSL_TRUSTSTORE_TYPE=PEM
export KAFKA_SSL_TRUSTSTORE_LOCATION=/run/secrets/kafka_ca
export KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=https
export KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/scripts/log4j2.yaml'
exec /etc/kafka/docker/run
