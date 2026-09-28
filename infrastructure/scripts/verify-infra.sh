#!/usr/bin/env bash
# Verifies the local infrastructure: connectivity, security posture, and expected behavior.
#
#   make verify                     non-disruptive checks
#   VERIFY_RESTARTS=1 make verify   additionally restarts PostgreSQL and Redis to prove that
#                                   durable data survives and disposable data does not
#
# Observability checks run only when the `observability` profile is running.
# Requirements on the host: bash, docker (Compose v2), curl.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.." || exit 2
# Resolve services of every profile (checks for services that are not running are skipped).
export COMPOSE_PROFILES=observability,gateway,core
# Git Bash on Windows: don't rewrite container paths such as /opt/kafka/... into Windows paths.
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'

[[ -f .env ]] || { echo "verify: .env not found - run 'make bootstrap' first" >&2; exit 2; }
while IFS='=' read -r key value; do
  [[ "$key" =~ ^[A-Z][A-Z0-9_]*$ ]] && printf -v "$key" '%s' "$value"
done < <(grep -E '^[A-Z][A-Z0-9_]*=' .env)

PROJECT="${COMPOSE_PROJECT_NAME:-trading-platform}"
PG_IMAGE="postgres:18.6"
KAFKA_BOOTSTRAP="localhost:29092"
PASS=0 FAIL=0 SKIP=0

secret() { tr -d '\r\n' < "secrets/$1"; }
cid() { docker compose ps -q "$1" 2> /dev/null; }
running() { [[ -n "$(docker compose ps -q --status running "$1" 2> /dev/null)" ]]; }

# check <description> <command...>: runs the command and records PASS/FAIL with its output on failure.
check() {
  local desc="$1"; shift
  local out
  if out="$("$@" 2>&1)"; then
    PASS=$((PASS + 1)); printf '  PASS  %s\n' "$desc"
  else
    FAIL=$((FAIL + 1)); printf '  FAIL  %s\n' "$desc"
    printf '%s\n' "$out" | tail -n 8 | sed 's/^/        | /'
  fi
}
skip() { SKIP=$((SKIP + 1)); printf '  SKIP  %s\n' "$1"; }

# retry <attempts> <sleep-seconds> <command...>: bounded retry for eventually-consistent checks.
retry() {
  local attempts="$1" pause="$2"; shift 2
  local i
  for ((i = 1; i <= attempts; i++)); do
    "$@" && return 0
    sleep "$pause"
  done
  "$@"
}

wait_healthy() {
  local svc="$1" status
  status="$(docker inspect -f '{{.State.Health.Status}}' "$(cid "$svc")" 2> /dev/null)"
  [[ "$status" == "healthy" ]] || { echo "$svc health: ${status:-not running}"; return 1; }
}

# ------------------------------------------------------------------ helpers per component
psql_net() { # psql_net <role> <password> <sql>: connects over the Compose network with password auth
  local role="$1" password="$2" sql="$3"
  PGPASSWORD="$password" docker run --rm --network "${PROJECT}_data" -e PGPASSWORD "$PG_IMAGE" \
    psql --no-psqlrc -h postgres -U "$role" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -tAc "$sql"
}
owner_sql() { psql_net "$TRADING_OWNER_ROLE" "$(secret trading_owner_password)" "$1"; }
app_sql() { psql_net "$TRADING_APP_ROLE" "$(secret trading_app_password)" "$1"; }

redis_app() {
  REDISCLI_AUTH="$(secret redis_app_password)" docker exec -i -e REDISCLI_AUTH "$(cid redis)" \
    redis-cli --no-auth-warning --user "$REDIS_APP_USER" "$@"
}
redis_anon() { docker exec -i "$(cid redis)" redis-cli "$@"; }

kafka_tool() { # kafka_tool <script> <args...>
  local tool="$1"; shift
  docker exec -i -e KAFKA_HEAP_OPTS=-Xmx128m "$(cid kafka)" "/opt/kafka/bin/${tool}" "$@"
}

http_code() { curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$@"; }

# ------------------------------------------------------------------ checks: posture
check_ports_loopback() {
  local ids bad=0 id binding
  ids="$(docker compose ps -q)"
  [[ -n "$ids" ]] || { echo "no containers running"; return 1; }
  for id in $ids; do
    for binding in $(docker inspect -f '{{range $p, $b := .NetworkSettings.Ports}}{{range $b}}{{.HostIp}}:{{.HostPort}} {{end}}{{end}}' "$id"); do
      if [[ "$binding" != 127.0.0.1:* ]]; then
        echo "$(docker inspect -f '{{.Name}}' "$id") publishes $binding"; bad=1
      fi
    done
  done
  return "$bad"
}

check_non_root() {
  # Checks the steady state after startup. Some official images (e.g. PostgreSQL) start their entrypoint as
  # root to prepare data directories and then drop privileges; that initialization phase is not covered here.
  local bad=0 svc id uids
  for svc in $(docker compose ps --services --status running); do
    id="$(cid "$svc")"
    # Default `docker top` output; the first column is the UID (numeric or user name).
    uids="$(docker top "$id" 2> /dev/null | awk 'NR > 1 {print $1}' | sort -u | tr '\n' ' ')"
    if [[ -z "$uids" ]]; then
      echo "$svc: could not read process uids"; bad=1
    elif [[ " $uids " == *" 0 "* || " $uids " == *" root "* ]]; then
      echo "$svc: process running as uid 0 (uids: $uids)"; bad=1
    else
      echo "$svc: uids $uids"
    fi
  done
  return "$bad"
}

# ------------------------------------------------------------------ checks: PostgreSQL
check_pg_app_connect() {
  local row
  row="$(app_sql "SELECT current_user || '|' || current_setting('TimeZone') || '|' || current_setting('search_path')")" || return 1
  echo "$row"
  [[ "$row" == "${TRADING_APP_ROLE}|UTC|${TRADING_SCHEMA}" ]]
}

check_pg_wrong_password() {
  local out
  if out="$(psql_net "$TRADING_APP_ROLE" "definitely-wrong-password" "SELECT 1" 2>&1)"; then
    echo "connection with a wrong password succeeded"; return 1
  fi
  [[ "$out" == *"password authentication failed"* ]] || { echo "unexpected error: $out"; return 1; }
}

check_pg_least_privilege() {
  local tbl="${TRADING_SCHEMA}.verify_probe_$$"
  owner_sql "CREATE TABLE ${tbl} (id int PRIMARY KEY)" > /dev/null || return 1
  local ok=0 count
  app_sql "INSERT INTO ${tbl} VALUES (1)" > /dev/null || ok=1
  count="$(app_sql "SELECT count(*) FROM ${tbl}")"
  [[ "$count" == "1" ]] \
    || { echo "app role cannot use a table created by the owner (default privileges): $count"; ok=1; }
  if app_sql "CREATE TABLE ${TRADING_SCHEMA}.app_should_not_create (id int)" > /dev/null 2>&1; then
    echo "app role was able to CREATE TABLE in ${TRADING_SCHEMA}"; ok=1
  fi
  if app_sql "CREATE TABLE public.app_should_not_create (id int)" > /dev/null 2>&1; then
    echo "app role was able to CREATE TABLE in public"; ok=1
  fi
  if app_sql "DROP TABLE ${tbl}" > /dev/null 2>&1; then
    echo "app role was able to DROP a table"; ok=1
  fi
  owner_sql "DROP TABLE IF EXISTS ${tbl}" > /dev/null || ok=1
  return "$ok"
}

# ------------------------------------------------------------------ checks: Redis
check_redis_anonymous_rejected() {
  local out
  out="$(redis_anon PING 2>&1)"
  [[ "$out" == *NOAUTH* ]] || { echo "unauthenticated PING returned: $out"; return 1; }
}

check_redis_app_rw_ttl() {
  local key="verify:probe:$$" ttl
  [[ "$(redis_app PING)" == "PONG" ]] || return 1
  [[ "$(redis_app SET "$key" value EX 60)" == "OK" ]] || return 1
  ttl="$(redis_app TTL "$key")"
  redis_app DEL "$key" > /dev/null
  echo "ttl=$ttl"
  [[ "$ttl" =~ ^[0-9]+$ ]] && ((ttl > 0 && ttl <= 60))
}

redis_denied() { # redis_denied <command> [args...]: the ACL must reject the command
  local out
  out="$(redis_app "$@" 2>&1)"
  [[ "$out" == *NOPERM* ]] || { echo "'$*' was not denied by the ACL: $out"; return 1; }
}

check_redis_dangerous_denied() {
  local bad=0
  redis_denied FLUSHALL || bad=1
  redis_denied FLUSHDB || bad=1
  redis_denied CONFIG GET maxmemory || bad=1
  redis_denied KEYS '*' || bad=1
  return "$bad"
}

# ------------------------------------------------------------------ checks: Kafka
# Tool output is captured before matching: piping `docker exec` into `grep -q` breaks the pipe on Windows.
check_kafka_no_auto_create_config() {
  local out
  out="$(kafka_tool kafka-configs.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --describe \
    --entity-type brokers --entity-name 1 --all)" || return 1
  [[ "$out" == *"auto.create.topics.enable=false"* ]] || { echo "auto.create.topics.enable=false not found"; return 1; }
}

topic_absent() {
  local topics
  topics="$(kafka_tool kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --list | tr -d '\r')" || return 1
  ! grep -qx -- "$1" <<< "$topics"
}

kafka_smoke_roundtrip() { # kafka_smoke_roundtrip <topic>
  local topic="$1" payload="probe-$RANDOM-$RANDOM" received
  kafka_tool kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --create \
    --topic "$topic" --partitions 1 --replication-factor 1 > /dev/null || return 1
  printf '%s\n' "$payload" | kafka_tool kafka-console-producer.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic "$topic" > /dev/null || return 1
  received="$(kafka_tool kafka-console-consumer.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic "$topic" --from-beginning --max-messages 1 --timeout-ms 30000 2> /dev/null | tr -d '\r')"
  [[ "$received" == "$payload" ]] || { echo "sent '$payload', received '$received'"; return 1; }
}

check_kafka_smoke_roundtrip() {
  local topic result=0
  topic="infra-smoke-$(date +%s)-$RANDOM"
  kafka_smoke_roundtrip "$topic" || result=1
  # Always remove the temporary topic, whether or not the roundtrip succeeded.
  kafka_tool kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --delete --topic "$topic" > /dev/null 2>&1
  retry 10 1 topic_absent "$topic" || { echo "smoke topic $topic still present after deletion"; return 1; }
  [[ "$result" -eq 0 ]] && echo "roundtrip ok; topic $topic created, used, and deleted"
  return "$result"
}

check_kafka_missing_topic_rejected() {
  local topic out
  topic="infra-missing-$(date +%s)-$RANDOM"
  out="$(printf 'x\n' | kafka_tool kafka-console-producer.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --topic "$topic" --producer-property max.block.ms=5000 2>&1)"
  topic_absent "$topic" || { echo "producing created topic $topic implicitly"; kafka_tool kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" --delete --topic "$topic" > /dev/null 2>&1; return 1; }
  [[ "$out" == *"not present in metadata"* || "$out" == *UNKNOWN_TOPIC* || "$out" == *TimeoutException* ]] \
    || { echo "unexpected producer output: $out"; return 1; }
}

check_kafka_host_listener() {
  (exec 3<> "/dev/tcp/127.0.0.1/${KAFKA_HOST_PORT}") 2> /dev/null \
    || { echo "127.0.0.1:${KAFKA_HOST_PORT} not reachable"; return 1; }
}

# ------------------------------------------------------------------ checks: observability
prometheus_ready() { [[ "$(http_code "http://127.0.0.1:${PROMETHEUS_HOST_PORT}/-/ready")" == "200" ]]; }

prometheus_scalar() { # prometheus_scalar <promql>: prints the value of a single-sample instant query
  curl -s --max-time 5 --get --data-urlencode "query=$1" "http://127.0.0.1:${PROMETHEUS_HOST_PORT}/api/v1/query" \
    | grep -o '"value":\[[^]]*\]' | sed -E 's/.*,"([0-9.]+)"\]/\1/'
}

prometheus_all_targets_up() {
  # Application targets are scraped only while their profiles run.
  local selector='job=~".+"' expected=4 up down svc
  for svc in realtime-gateway trading-core; do
    if running "$svc"; then expected=$((expected + 1)); else selector="${selector},job!=\"${svc}\""; fi
  done
  up="$(prometheus_scalar "count(up{${selector}} == 1) or vector(0)")"
  down="$(prometheus_scalar "count(up{${selector}} == 0) or vector(0)")"
  echo "targets up=${up:-?} down=${down:-?} (expected up=${expected})"
  [[ "${up:-0}" -ge "$expected" && "${down:-1}" -eq 0 ]]
}

grafana_ds_ok() { # grafana_ds_ok <uid>
  local body
  # Credentials go through curl's config on stdin, so they never appear in the process list.
  body="$(curl -s --max-time 10 -K - "http://127.0.0.1:${GRAFANA_HOST_PORT}/api/datasources/uid/$1/health" \
    <<< "user = \"${GRAFANA_ADMIN_USER}:$(secret grafana_admin_password)\"")"
  echo "$1: $body"
  [[ "$body" == *'"status":"OK"'* ]]
}

check_grafana() {
  local health
  health="$(curl -s --max-time 5 "http://127.0.0.1:${GRAFANA_HOST_PORT}/api/health")" || return 1
  [[ "$health" == *'"database"'*'"ok"'* ]] || { echo "health: $health"; return 1; }
  [[ "$(http_code "http://127.0.0.1:${GRAFANA_HOST_PORT}/api/datasources")" == "401" ]] \
    || { echo "anonymous API access was not rejected"; return 1; }
  retry 10 3 grafana_ds_ok prometheus && retry 10 3 grafana_ds_ok tempo
}

tempo_ready() { [[ "$(curl -s --max-time 5 "http://127.0.0.1:${TEMPO_HOST_PORT}/ready")" == *ready* ]]; }

trace_found() { [[ "$(http_code "http://127.0.0.1:${TEMPO_HOST_PORT}/api/v2/traces/$1")" == "200" ]]; }

random_hex() { od -An -tx1 -N"$1" /dev/urandom | tr -d ' \n'; }

check_trace_end_to_end() {
  local trace_id span_id now_ns code
  trace_id="$(random_hex 16)"; span_id="$(random_hex 8)"
  now_ns="$(date +%s)000000000"
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -H 'Content-Type: application/json' \
    -X POST "http://127.0.0.1:${OTLP_HTTP_HOST_PORT}/v1/traces" --data @- << JSON
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"infra-verify"}}]},
"scopeSpans":[{"scope":{"name":"verify-infra"},"spans":[{"traceId":"${trace_id}","spanId":"${span_id}",
"name":"verify-span","kind":1,"startTimeUnixNano":"${now_ns}","endTimeUnixNano":"${now_ns}"}]}]}]}
JSON
)"
  [[ "$code" == "200" ]] || { echo "collector OTLP/HTTP returned $code"; return 1; }
  retry 30 2 trace_found "$trace_id" || { echo "trace ${trace_id} not found in Tempo within 60s"; return 1; }
  echo "trace ${trace_id}: collector -> tempo ok"
}

# ------------------------------------------------------------------ checks: realtime gateway (gateway profile)
GATEWAY_URL="http://127.0.0.1:${GATEWAY_HOST_PORT:-18090}"
GATEWAY_HEALTH_URL="http://127.0.0.1:${GATEWAY_HEALTH_HOST_PORT:-18091}"

# gateway_source prints the active market-data source (MOCK or IBKR) reported by /health.
gateway_source() {
  curl -s --max-time 5 "${GATEWAY_HEALTH_URL}/health" | grep -o '"source":"[A-Z]*"' | cut -d'"' -f4
}

check_gateway_health() {
  local body
  [[ "$(http_code "${GATEWAY_HEALTH_URL}/readiness")" == "200" ]] || { echo "readiness not 200"; return 1; }
  body="$(curl -s --max-time 5 "${GATEWAY_HEALTH_URL}/health")"
  [[ "$body" == *'"connectionState":"READY"'* ]] || { echo "health: $body"; return 1; }
  docker exec "$(cid realtime-gateway)" /usr/local/bin/realtime-gateway healthcheck || { echo "healthcheck subcommand failed"; return 1; }
}

check_gateway_bars() {
  local body
  local src; src="$(gateway_source)"
  [[ "$src" == "MOCK" || "$src" == "IBKR" ]] || { echo "unknown source '$src'"; return 1; }
  body="$(curl -s --max-time 10 "${GATEWAY_URL}/api/v1/market/bars?symbol=NVDA&interval=1m&range=1d")" || return 1
  [[ "$body" == *"\"source\":\"${src}\""* && "$body" == *'"bars":[{'* ]] || { echo "unexpected body: ${body:0:200}"; return 1; }
  [[ "$(http_code "${GATEWAY_URL}/api/v1/market/bars?symbol=NOPE&interval=1m&range=1d")" == "404" ]] || { echo "unknown symbol not 404"; return 1; }
}

check_gateway_origin_rejected() {
  local code nonce
  nonce="$(head -c 16 /dev/urandom | base64)" # fresh handshake nonce per request, as RFC 6455 requires
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
    -H 'Connection: Upgrade' -H 'Upgrade: websocket' -H 'Sec-WebSocket-Version: 13' \
    -H "Sec-WebSocket-Key: ${nonce}" -H 'Origin: http://evil.example' \
    "${GATEWAY_URL}/ws")"
  [[ "$code" == "403" ]] || { echo "disallowed origin returned $code"; return 1; }
}

check_gateway_stream() {
  docker exec "$(cid realtime-gateway)" /usr/local/bin/stream-probe -url ws://127.0.0.1:8090/ws \
    -symbols NVDA,AAPL,MSFT -quotes 3 -timeout 30s
}

check_gateway_metrics() {
  local body
  body="$(curl -s --max-time 5 "${GATEWAY_HEALTH_URL}/metrics")" || return 1
  [[ "$body" == *'realtime_gateway_connection_state{state="READY"} 1'* ]] || { echo "connection_state READY missing"; return 1; }
  [[ "$body" == *'realtime_gateway_quotes_received_total'* && "$body" == *'go_goroutines'* ]] || { echo "metrics incomplete"; return 1; }
}

gateway_quote_cached() {
  local key value ttl
  key="quote:$(gateway_source):NVDA" # keys are namespaced by source
  value="$(redis_app GET "$key")"
  ttl="$(redis_app TTL "$key")"
  [[ "$value" == *'"type":"quote"'*'"symbol":"NVDA"'* ]] || { echo "$key = '${value:0:120}'"; return 1; }
  [[ "$ttl" =~ ^[0-9]+$ && "$ttl" -gt 0 && "$ttl" -le 30 ]] || { echo "$key TTL = '$ttl'"; return 1; }
}

check_gateway_hot_cache() {
  # The stream probe above subscribed NVDA; the latest quote is written to Redis about once per second.
  retry 10 1 gateway_quote_cached
}

check_gateway_bars_cached() {
  local url="${GATEWAY_URL}/api/v1/market/bars?symbol=AAPL&interval=15m&range=5d" first second
  first="$(curl -s --max-time 30 "$url")" || return 1
  second="$(curl -s --max-time 30 "$url")" || return 1
  [[ "$first" == *'"bars":[{'* ]] || { echo "unexpected body: ${first:0:200}"; return 1; }
  [[ "$second" == *'"cached":true'* ]] || { echo "second response not served from the cache: ${second:0:200}"; return 1; }
  [[ "$(redis_app TTL "bars:$(gateway_source):AAPL:15m:5d")" =~ ^[0-9]+$ ]] || { echo "bars cache key has no TTL"; return 1; }
}

# ------------------------------------------------------------------ checks: trading core (core profile)
CORE_URL="http://127.0.0.1:${TRADING_CORE_HOST_PORT:-18080}"
CORE_MGMT_URL="http://127.0.0.1:${TRADING_CORE_MANAGEMENT_HOST_PORT:-18081}"

new_uuid() {
  local h; h="$(random_hex 16)"
  printf '%s-%s-4%s-a%s-%s' "${h:0:8}" "${h:8:4}" "${h:13:3}" "${h:17:3}" "${h:20:12}"
}

json_field() { grep -o "\"$1\":\"[^\"]*\"" | head -n 1 | cut -d'"' -f4; }

check_core_health() {
  [[ "$(http_code "${CORE_MGMT_URL}/actuator/health/readiness")" == "200" ]] || { echo "readiness not 200"; return 1; }
  [[ "$(http_code "${CORE_MGMT_URL}/actuator/health/liveness")" == "200" ]] || { echo "liveness not 200"; return 1; }
  [[ "$(curl -s --max-time 5 "${CORE_MGMT_URL}/actuator/prometheus")" == *orders_submitted_total* ]] \
    || { echo "order metrics missing"; return 1; }
  [[ "$(http_code "${CORE_URL}/actuator/health")" == "404" ]] || { echo "actuator reachable on the API port"; return 1; }
}

check_core_migrations() {
  local n
  n="$(app_sql "SELECT count(*) FROM flyway_schema_history WHERE success")" || return 1
  echo "applied migrations: $n"
  [[ "$n" -ge 4 ]] || return 1
  if app_sql "ALTER TABLE orders ADD COLUMN verify_probe int" > /dev/null 2>&1; then
    echo "the application role altered a table"; return 1
  fi
}

check_core_api_posture() {
  local id headers body
  id="$(new_uuid)"
  headers="$(curl -s -D - -o /dev/null --max-time 5 -H "X-Correlation-Id: ${id}" "${CORE_URL}/api/v1/watchlist")"
  [[ "$headers" == *"$id"* ]] || { echo "correlation id not echoed"; return 1; }
  [[ "$(http_code -X OPTIONS -H 'Origin: https://evil.example' -H 'Access-Control-Request-Method: POST' \
    "${CORE_URL}/api/v1/orders")" == "403" ]] || { echo "disallowed CORS origin not rejected"; return 1; }
  headers="$(curl -s -D - -o /dev/null --max-time 5 -X OPTIONS -H 'Origin: http://localhost:3000' \
    -H 'Access-Control-Request-Method: POST' "${CORE_URL}/api/v1/orders")"
  grep -qi '^access-control-allow-origin: http://localhost:3000' <<< "$headers" \
    || { echo "allowed origin not granted"; return 1; }
  body="$(curl -s --max-time 5 -X POST -H 'Content-Type: application/json' -d '{}' "${CORE_URL}/api/v1/orders")"
  [[ "$body" == *'"category":"VALIDATION"'* && "$body" != *Exception* && "$body" != *trace* ]] \
    || { echo "unexpected error body: $body"; return 1; }
}

check_core_fails_closed_in_paper_mode() {
  local out
  if out="$(docker run --rm --network none -e APP_RUNTIME_MODE=IBKR_PAPER trading-terminal/trading-core:local 2>&1)"; then
    echo "trading core started in IBKR_PAPER mode"; return 1
  fi
  [[ "$out" == *"Unsupported runtime mode"* ]] || { echo "$out" | tail -n 3; return 1; }
}

core_order() { # core_order <idempotency-key> <json>
  curl -s --max-time 10 -X POST -H 'Content-Type: application/json' -H "Idempotency-Key: $1" -d "$2" \
    "${CORE_URL}/api/v1/orders"
}

mock_quote_fresh() { # the gateway writes quotes only for subscribed symbols
  local ts
  ts="$(redis_app GET quote:MOCK:NVDA | json_field timestamp)"
  [[ -n "$ts" ]] || { echo "no quote:MOCK:NVDA"; return 1; }
  (( $(date -u +%s) - $(date -u -d "$ts" +%s) <= 5 )) || { echo "quote too old: $ts"; return 1; }
}

check_core_market_order_fills_from_live_quotes() {
  local key body buy retry sell id
  # Keep NVDA subscribed for the duration of the check, as a terminal watching the symbol would.
  docker exec -d "$(cid realtime-gateway)" /usr/local/bin/stream-probe -url ws://127.0.0.1:8090/ws \
    -symbols NVDA -quotes 30 || return 1
  retry 15 1 mock_quote_fresh > /dev/null || { mock_quote_fresh; return 1; }
  key="$(new_uuid)"
  body='{"symbol":"NVDA","intent":"BUY","orderType":"MARKET","quantity":"1","timeInForce":"DAY"}'
  buy="$(core_order "$key" "$body")" || return 1
  id="$(json_field id <<< "$buy")"
  echo "buy: status=$(json_field status <<< "$buy") averageFillPrice=$(json_field averageFillPrice <<< "$buy")"
  [[ "$(json_field status <<< "$buy")" == "FILLED" && -n "$(json_field averageFillPrice <<< "$buy")" ]] \
    || { echo "$buy"; return 1; }
  retry="$(core_order "$key" "$body")"
  [[ "$(json_field id <<< "$retry")" == "$id" ]] || { echo "an idempotent retry returned another order: $retry"; return 1; }
  sell="$(core_order "$(new_uuid)" '{"symbol":"NVDA","intent":"SELL","orderType":"MARKET","quantity":"1","timeInForce":"DAY"}')"
  [[ "$(json_field status <<< "$sell")" == "FILLED" ]] || { echo "sell: $sell"; return 1; }
  [[ "$(curl -s --max-time 5 "${CORE_URL}/api/v1/executions?orderId=${id}")" == *"\"orderId\":\"${id}\""* ]] \
    || { echo "execution not recorded"; return 1; }
}

# ------------------------------------------------------------------ checks: restarts (opt-in)
check_pg_durable_across_restart() {
  local tbl="${TRADING_SCHEMA}.verify_durable_$$"
  owner_sql "CREATE TABLE ${tbl} (v text); INSERT INTO ${tbl} VALUES ('kept')" > /dev/null || return 1
  docker compose restart postgres > /dev/null 2>&1 || return 1
  retry 30 2 wait_healthy postgres > /dev/null || { echo "postgres not healthy after restart"; return 1; }
  local v; v="$(owner_sql "SELECT v FROM ${tbl}")"
  owner_sql "DROP TABLE ${tbl}" > /dev/null
  [[ "$v" == "kept" ]] || { echo "row lost across restart (got '$v')"; return 1; }
}

check_redis_disposable_across_restart() {
  local key="verify:disposable:$$" v
  redis_app SET "$key" value EX 300 > /dev/null || return 1
  docker compose restart redis > /dev/null 2>&1 || return 1
  retry 30 2 wait_healthy redis > /dev/null || { echo "redis not healthy after restart"; return 1; }
  v="$(redis_app GET "$key")"
  [[ -z "$v" ]] || { echo "key survived restart - Redis must not persist data"; return 1; }
}

# ================================================================== run
echo "verify: project '${PROJECT}'"

echo "[posture]"
check "published ports are bound to 127.0.0.1 only" check_ports_loopback
check "running application processes are non-root (images may start as root during init, then drop privileges)" check_non_root

echo "[health]"
for svc in postgres redis kafka; do
  if running "$svc"; then
    check "$svc is healthy" retry 30 2 wait_healthy "$svc"
  else
    check "$svc is running" false
  fi
done

echo "[postgresql]"
if running postgres; then
  check "app role connects over the network with password auth; timezone UTC; search_path" check_pg_app_connect
  check "wrong password is rejected" check_pg_wrong_password
  check "least privilege: app role has DML via default privileges, no DDL" check_pg_least_privilege
else skip "postgresql checks (not running)"; fi

echo "[redis]"
if running redis; then
  check "unauthenticated access is rejected (default user disabled)" check_redis_anonymous_rejected
  check "app user can read/write keys with TTL" check_redis_app_rw_ttl
  check "dangerous commands are denied by the ACL (FLUSHALL, FLUSHDB, CONFIG, KEYS)" check_redis_dangerous_denied
else skip "redis checks (not running)"; fi

echo "[kafka]"
if running kafka; then
  check "broker has auto.create.topics.enable=false" check_kafka_no_auto_create_config
  check "smoke topic: create -> produce -> consume -> delete" check_kafka_smoke_roundtrip
  check "producing to a missing topic fails and creates nothing" check_kafka_missing_topic_rejected
  check "host listener reachable on 127.0.0.1:${KAFKA_HOST_PORT}" check_kafka_host_listener
else skip "kafka checks (not running)"; fi

echo "[observability]"
if running prometheus; then
  check "prometheus is ready" retry 15 2 prometheus_ready
  check "prometheus scrapes all targets successfully" retry 30 3 prometheus_all_targets_up
  check "tempo is ready" retry 30 2 tempo_ready
  check "grafana healthy; anonymous API rejected; datasources Prometheus and Tempo healthy" check_grafana
  check "trace end-to-end: OTLP/HTTP -> collector -> tempo" check_trace_end_to_end
else skip "observability checks (profile not running; use 'make up-obs')"; fi

echo "[realtime-gateway]"
if running realtime-gateway; then
  check "realtime gateway is healthy" retry 30 2 wait_healthy realtime-gateway
  check "readiness, detailed health and healthcheck subcommand" check_gateway_health
  check "historical bars from the active source (MOCK or IBKR) and 404 for unknown symbols" check_gateway_bars
  check "WebSocket upgrade from a disallowed origin is rejected (403)" check_gateway_origin_rejected
  check "stream probe: connection, snapshots, ordered quotes for 3 symbols" check_gateway_stream
  check "Prometheus metrics on the internal port" check_gateway_metrics
  if running redis; then
    check "latest quote is written to Redis (quote:{SOURCE}:NVDA, stream quote shape, TTL <= 30s)" check_gateway_hot_cache
    check "historical bars are cached in Redis and marked cached on the next request" check_gateway_bars_cached
  else skip "gateway Redis checks (redis not running)"; fi
else skip "realtime gateway checks (gateway profile not running; use 'make up-mock')"; fi

echo "[trading-core]"
if running trading-core; then
  check "trading core is healthy" retry 30 2 wait_healthy trading-core
  check "liveness, readiness and metrics on the management port only" check_core_health
  check "schema migrations applied; the application role cannot alter the schema" check_core_migrations
  check "correlation id echoed; CORS allowlist enforced; problem body without internals" check_core_api_posture
  check "IBKR_PAPER mode fails closed at startup" check_core_fails_closed_in_paper_mode
  if running realtime-gateway && running redis; then
    check "market BUY and SELL fill immediately from live MOCK quotes; idempotent retry" check_core_market_order_fills_from_live_quotes
  else skip "trading core order check (needs the realtime gateway and redis)"; fi
else skip "trading core checks (core profile not running; use 'make up-mock')"; fi

echo "[restarts]"
if [[ "${VERIFY_RESTARTS:-0}" == "1" ]]; then
  check "postgresql data survives a restart" check_pg_durable_across_restart
  check "redis data does not survive a restart (disposable)" check_redis_disposable_across_restart
else skip "restart checks (set VERIFY_RESTARTS=1)"; fi

echo
echo "verify: ${PASS} passed, ${FAIL} failed, ${SKIP} skipped"
[[ "$FAIL" -eq 0 ]]
