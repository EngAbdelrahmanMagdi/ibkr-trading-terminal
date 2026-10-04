#!/bin/sh
# Local mTLS identities. The CA key is never mounted into a runtime service.
set -eu
out=${1:-/out}
mkdir -p "$out"
umask 077
if [ ! -s "$out/ca.pem" ]; then
  openssl req -x509 -newkey rsa:3072 -nodes -sha256 -days 3650 -subj '/CN=MarketPulse Kafka Local CA' -keyout "$out/ca.key" -out "$out/ca.pem" 2>/dev/null
fi
for identity in kafka-broker kafka-operator trading-core realtime-gateway ai-insights; do
  if [ -s "$out/$identity.pem" ] && [ -s "$out/$identity.key" ]; then continue; fi
  openssl req -new -newkey rsa:3072 -nodes -sha256 -subj "/CN=$identity" -keyout "$out/$identity.key" -out "$out/$identity.csr" 2>/dev/null
  if [ "$identity" = kafka-broker ]; then
    printf '%s\n' 'subjectAltName=DNS:kafka,DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1' 'extendedKeyUsage=serverAuth,clientAuth' > "$out/$identity.ext"
  else
    printf '%s\n' 'extendedKeyUsage=clientAuth' > "$out/$identity.ext"
  fi
  openssl x509 -req -sha256 -days 365 -in "$out/$identity.csr" -CA "$out/ca.pem" -CAkey "$out/ca.key" -CAcreateserial -extfile "$out/$identity.ext" -out "$out/$identity.pem" 2>/dev/null
  rm "$out/$identity.csr" "$out/$identity.ext"
done
# PEM stores avoid distributing Java keystore passwords. Runtime mounts remain read-only and service-specific.
for identity in kafka-broker kafka-operator trading-core realtime-gateway ai-insights; do
  if [ ! -s "$out/$identity.keystore" ]; then
    cat "$out/$identity.key" "$out/$identity.pem" > "$out/$identity.keystore"
  fi
done
# The host directory protects keys; individual Docker secret mounts must be readable by non-root services.
chmod 700 "$out"
chmod 600 "$out/ca.key"
chmod 444 "$out/ca.pem"
for identity in kafka-broker kafka-operator trading-core realtime-gateway ai-insights; do
  chmod 444 "$out/$identity.key" "$out/$identity.pem" "$out/$identity.keystore"
done
printf '%s\n' 'Kafka certificates ready (existing identities preserved)'
