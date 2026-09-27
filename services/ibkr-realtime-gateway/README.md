# Realtime Gateway

This Go service streams market data to browsers over WebSocket and serves historical bars over HTTP. In `MOCK` mode it is backed by a deterministic market simulator, so the feed runs 24/7 without any brokerage credentials.

## How it works

### Market data

- **`MarketDataSource`** is the only way the gateway obtains market data. `SimulatorMarketDataSource` is the `MOCK` implementation; a broker-backed source can replace it without changing any consumer. A source also reports its connection state (`CONNECTING`, `AUTHENTICATING`, `READY`, `DEGRADED`, `RECONNECTING`, `DISCONNECTED`).
- **The simulator is deterministic and stateless.** Every price is a pure function of `(seed, symbol, time)`, computed with integer fixed-point arithmetic only. That means:
  - the same seed produces the same prices on every machine and after every restart;
  - historical bars and live quotes agree at the same timestamps;
  - long histories cost nothing to store.
- **Live quotes** are emitted on a fixed time grid (every `GATEWAY_TICK_INTERVAL`). Quote *k* is taken at time *k* × interval and carries `sequence` *k*, so sequences increase monotonically and any quote can be reproduced from its timestamp.
- **Bars** are built from the same price function:
  - `1m` bars sample each second, so a completed minute equals the aggregate of the 1-second live quotes of that minute;
  - `5m`, `15m` and `1h` bars aggregate their 1m bars exactly;
  - `1d` bars sample each minute of the UTC day.
- **Synthetic instruments** `SYN001`…`SYNnnn` (`GATEWAY_SIM_SYNTHETIC_SYMBOLS`) exist only for load testing. They are just as deterministic.

### Shared subscriptions and fan-out

- **One upstream subscription per symbol**, however many clients want it. It is reference counted, and when the last client leaves it is closed after `GATEWAY_UNSUBSCRIBE_GRACE`, so quick re-subscriptions cause no churn.
- The number of symbols with an upstream subscription is capped by `GATEWAY_MAX_ACTIVE_SYMBOLS` (a broker limits market-data lines). Symbols over the cap get a `SUBSCRIPTION_LIMIT` error.
- Each quote is encoded once and shared by every subscriber. Subscriber sets are copy-on-write, so fan-out takes no lock.

### Backpressure and slow consumers

Each connection has two outbound paths:

1. **Control queue.** A bounded queue carries `connection`, `snapshot`, `stale` and `error` messages. It is always written first and never drops messages. Overflow evicts the client.
2. **Quote slots.** Each subscribed symbol has one latest-value slot. A newer quote replaces an unsent one (counted as coalesced), and each symbol is sent to a client at most once per `GATEWAY_FLUSH_INTERVAL`. Symbols don't delay each other.

Memory per connection is therefore bounded by the control-queue capacity plus one pending quote per subscribed symbol. A symbol's snapshot always precedes its quotes, and sequences stay monotonic.

A slow client receives fewer, coalesced updates and never stalls the fan-out. It is evicted (close code 1013, counted by reason) when any of the following happens:
- a write exceeds `GATEWAY_WRITE_TIMEOUT`;
- the peer doesn't answer a ping in time;
- the control queue overflows;
- more than `GATEWAY_SLOW_CONSUMER_MAX_LAGGING_FLUSHES` consecutive flushes deliver data older than `GATEWAY_SLOW_CONSUMER_LAG`.

Unresponsive peers are dropped without waiting for a close handshake.

### Staleness and connection state

- A symbol with no update for `GATEWAY_STALE_AFTER` is reported in a `stale` message (reason `NO_UPDATES`).
- When the source leaves `READY`, clients first receive a `connection` message with the new state, then a `stale` message for their symbols (reason `SOURCE_DISCONNECTED` or `SOURCE_DEGRADED`).
- Snapshots of stale symbols carry `stale: true`. The next fresh quote clears the flag.
- When the source returns to `READY`, every active symbol is resubscribed upstream.
- A generic, bounded reconnect framework (full-jitter exponential backoff, maximum attempts and maximum total time, then `DISCONNECTED`) is ready for broker-backed sources.

### Redis hot state (optional, never critical)

- **Latest quote per symbol.** `quote:{SYMBOL}` holds the same JSON as a stream `quote` message, with a TTL. It is written at most once per `GATEWAY_QUOTE_CACHE_INTERVAL` in one pipelined request.
- **Bars cache.** `bars:{SYMBOL}:{interval}:{range}` has a TTL of a quarter of the interval, capped at `GATEWAY_BARS_CACHE_MAX_TTL`. Responses served from it have `cached: true`.
  - Concurrent identical requests share one computation.
  - At most `GATEWAY_MAX_BAR_COMPUTATIONS` run at once; further requests get a `RATE_LIMITED` problem (HTTP 429).
- **Failure handling.** Every Redis call has a timeout. A failure pauses Redis use for `GATEWAY_REDIS_COOLDOWN`. Streaming, bars and readiness keep working without Redis.

### Protocol and connection handling

- **WebSocket protocol:** see [`contracts/asyncapi/market-stream.yaml`](../../contracts/asyncapi/market-stream.yaml).
  - On connect the server sends a `connection` message with the source state and the effective limits.
  - `subscribe` gets a `snapshot` for each symbol, followed by `quote` messages; `unsubscribe` stops them.
  - `heartbeat` messages keep the connection alive; `error` messages report problems.
- **Connection handling:**
  - browser origins must be on an allowlist;
  - inbound messages are size-limited, with close code 1009 when the limit is exceeded;
  - the number of concurrent connections is capped;
  - on shutdown, clients are closed with 1001.

## Endpoints

| Port | Path | Purpose |
|---|---|---|
| 8090 | `GET /ws` | Market stream (WebSocket) |
| 8090 | `GET /api/v1/market/bars?symbol=&interval=&range=` | Historical OHLCV bars ([OpenAPI](../../contracts/openapi/realtime-gateway-api.yaml)) |
| 8091 | `GET /liveness`, `/readiness`, `/ready`, `/health` | Health (internal port). Readiness requires that the source has been `READY` and hasn't given up reconnecting. Redis is not a readiness dependency. |
| 8091 | `GET /metrics` | Prometheus metrics (internal port) |

Supported interval/range combinations:

| Interval | Ranges |
|---|---|
| `1m` | `1d`, `5d` |
| `5m` | `1d`, `5d`, `1mo` |
| `15m` | `5d`, `1mo` |
| `1h` | `5d`, `1mo` |
| `1d` | `1mo`, `3mo`, `1y` |

## Metrics

Every metric carries the `realtime_gateway_` prefix. The Go runtime and process collectors are included too.

| Metric | Type | Meaning |
|---|---|---|
| `connection_state{state}` | gauge | 1 for the current source connection state |
| `quotes_received_total` | counter | Quotes received from the source |
| `quotes_forwarded_total` | counter | Quote messages written to clients |
| `quotes_coalesced_total` | counter | Quotes replaced by a newer one before being sent |
| `ws_clients` | gauge | Connected WebSocket clients |
| `active_symbols` / `stale_symbols` | gauge | Symbols with an upstream subscription / currently stale |
| `reconnect_total` | counter | Transitions into `RECONNECTING` |
| `message_processing_seconds` | histogram | From receiving a quote to writing it to a client |
| `slow_consumer_evictions_total{reason}` | counter | `write_timeout`, `pong_timeout`, `control_queue_full`, `lagging_flushes` |
| `control_queue_depth` | histogram | Control-queue depth at enqueue |
| `redis_errors_total{op}`, `cache_hits_total{cache}`, `cache_misses_total{cache}`, `quote_cache_writes_total`, `bars_rate_limited_total` | counter | Redis and cache behavior |

With the observability profile running, Grafana provisions a **Realtime Gateway** dashboard from these metrics.

## Configuration

Settings come from environment variables. Invalid values stop the service at startup, and every invalid value is reported.

| Variable | Default | Meaning |
|---|---|---|
| `GATEWAY_HTTP_ADDR` / `GATEWAY_HEALTH_ADDR` | `:8090` / `:8091` | Listen addresses |
| `GATEWAY_ALLOWED_ORIGINS` | `localhost:3000,127.0.0.1:3000` | Browser origins (host patterns) allowed to connect. Wildcards are rejected. Non-browser clients without an `Origin` header are allowed. |
| `GATEWAY_SEED` | `20260927` | Simulator seed |
| `GATEWAY_TICK_INTERVAL` | `1s` | Quote interval per symbol |
| `GATEWAY_SIM_SYNTHETIC_SYMBOLS` | `0` | Synthetic load-test instruments (0 to 999) |
| `GATEWAY_MAX_SYMBOLS_PER_SUBSCRIBE` | `50` | Symbols per subscribe message |
| `GATEWAY_MAX_SUBSCRIBED_SYMBOLS` | `100` | Symbols per connection |
| `GATEWAY_MAX_ACTIVE_SYMBOLS` | `100` | Symbols with an upstream subscription, across all clients |
| `GATEWAY_UNSUBSCRIBE_GRACE` | `10s` | Delay before an unused upstream subscription is closed |
| `GATEWAY_STALE_AFTER` | `5s` | No update for this long marks a symbol stale (at least twice the tick interval) |
| `GATEWAY_MAX_INBOUND_MESSAGE_BYTES` | `4096` | Maximum client message size |
| `GATEWAY_HEARTBEAT_INTERVAL` | `15s` | Heartbeat and ping interval |
| `GATEWAY_WRITE_TIMEOUT` | `5s` | Per-frame write timeout and pong timeout |
| `GATEWAY_CONTROL_QUEUE_SIZE` | `256` | Per-connection control queue capacity |
| `GATEWAY_FLUSH_INTERVAL` | `50ms` | Minimum time between two quotes of the same symbol to one client |
| `GATEWAY_SLOW_CONSUMER_LAG` | `2s` | A flush delivering data older than this counts as lagging |
| `GATEWAY_SLOW_CONSUMER_MAX_LAGGING_FLUSHES` | `5` | Consecutive lagging flushes tolerated before eviction |
| `GATEWAY_MAX_CONNECTIONS` | `1000` | Concurrent WebSocket connections |
| `GATEWAY_MAX_BAR_COMPUTATIONS` | `2` | Concurrent uncached bar computations |
| `GATEWAY_BARS_TIMEOUT` | `8s` | Bound on one bar computation |
| `GATEWAY_BARS_CACHE_MAX_TTL` | `5m` | Upper bound of the bars cache TTL |
| `GATEWAY_REDIS_ADDR` | *(empty)* | Redis address; empty disables the hot state and the bars cache |
| `GATEWAY_REDIS_USERNAME` | `app` | Redis ACL user |
| `GATEWAY_REDIS_PASSWORD_FILE` | *(empty)* | File containing the Redis password (required when Redis is enabled) |
| `GATEWAY_REDIS_TIMEOUT` | `100ms` | Timeout of every Redis call |
| `GATEWAY_REDIS_COOLDOWN` | `5s` | Pause after a Redis failure |
| `GATEWAY_QUOTE_CACHE_INTERVAL` / `GATEWAY_QUOTE_CACHE_TTL` | `1s` / `30s` | Latest-quote write interval and TTL |
| `GATEWAY_SHUTDOWN_TIMEOUT` | `10s` | Graceful shutdown deadline |
| `GATEWAY_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` (JSON logs) |

## Running and testing

From the repository root:

```bash
make up-mock      # build and start the gateway (plus core infrastructure)
make probe        # connect a test client: connection, snapshots, ordered quotes
make load         # load/soak clients with leak checks (LOAD_CLIENTS, LOAD_DURATION, LOAD_ARGS)
make test-unit    # unit, integration and contract-conformance tests with the race detector
make lint-go      # gofmt, go vet, golangci-lint
```

- **Integration tests** start the gateway on a local test server and connect real WebSocket clients. They cover shared subscriptions, slow-client eviction, source outage and recovery, and Redis failures. Every message is validated against the JSON Schemas in `contracts/schemas/`, and after each test they verify that no goroutines are left running.
- **`stream-load`** (`make load`) runs many clients with subscription churn and optional slow readers against the running feed.
  - It reports delivery statistics.
  - It samples `/metrics` during the run, then checks that sessions, upstream subscriptions and goroutines return to their baseline, and that heap usage levels off.
  - Its numbers are measurements of one run on one machine, not performance claims.

**Graceful shutdown order:**
1. readiness fails;
2. WebSocket clients are closed (1001);
3. upstream subscriptions and the stale monitor stop;
4. the Redis writer flushes and stops;
5. the HTTP servers stop.

All of this happens within `GATEWAY_SHUTDOWN_TIMEOUT`.

The container image is built on a distroless base, runs as a non-root user, and has a read-only filesystem. The `realtime-gateway healthcheck` subcommand provides the container health check.
