# Realtime Gateway

This Go service streams market data to browsers over WebSocket and serves historical bars over HTTP. In `MOCK` mode it is backed by a deterministic market simulator, so the feed runs 24/7 without any brokerage credentials.

## How it works

- **`MarketDataSource`** is the only way the gateway obtains market data. `SimulatorMarketDataSource` is the `MOCK` implementation; a broker-backed source can replace it without changing any consumer.
- **The simulator is deterministic and stateless.** Every price is a pure function of `(seed, symbol, time)`, computed with integer fixed-point arithmetic only. That means:
  - the same seed produces the same prices on every machine and after every restart;
  - historical bars and live quotes agree at the same timestamps;
  - long histories cost nothing to store.
- **Live quotes** are emitted on a fixed time grid (every `GATEWAY_TICK_INTERVAL`). Quote *k* is taken at time *k* × interval and carries `sequence` *k*, so sequences increase monotonically and any quote can be reproduced from its timestamp.
- **Bars** are built from the same price function:
  - `1m` bars sample each second, so a completed minute equals the aggregate of the 1-second live quotes of that minute;
  - `5m`, `15m` and `1h` bars aggregate their 1m bars exactly;
  - `1d` bars sample each minute of the UTC day.
- **WebSocket protocol:** see [`contracts/asyncapi/market-stream.yaml`](../../contracts/asyncapi/market-stream.yaml).
  - On connect the server sends a `connection` message with the effective limits.
  - `subscribe` gets a `snapshot` for each symbol, followed by `quote` messages; `unsubscribe` stops them.
  - `heartbeat` messages keep the connection alive; `error` messages report problems.
- **Connection handling:**
  - each connection has one bounded send queue;
  - browser origins must be on an allowlist;
  - inbound messages are size-limited, with close code 1009 when the limit is exceeded;
  - writes have timeouts, and a ping/pong liveness check detects dead peers;
  - a full send queue closes the connection with 1013 (try again later);
  - on shutdown, clients are closed with 1001.

## Endpoints

| Port | Path | Purpose |
|---|---|---|
| 8090 | `GET /ws` | Market stream (WebSocket) |
| 8090 | `GET /api/v1/market/bars?symbol=&interval=&range=` | Historical OHLCV bars ([OpenAPI](../../contracts/openapi/realtime-gateway-api.yaml)) |
| 8091 | `GET /liveness`, `/readiness`, `/ready`, `/health` | Health (internal port) |

Supported interval/range combinations:

| Interval | Ranges |
|---|---|
| `1m` | `1d`, `5d` |
| `5m` | `1d`, `5d`, `1mo` |
| `15m` | `5d`, `1mo` |
| `1h` | `5d`, `1mo` |
| `1d` | `1mo`, `3mo`, `1y` |

## Configuration

Settings come from environment variables. Invalid values stop the service at startup, and every invalid value is reported.

| Variable | Default | Meaning |
|---|---|---|
| `GATEWAY_HTTP_ADDR` / `GATEWAY_HEALTH_ADDR` | `:8090` / `:8091` | Listen addresses |
| `GATEWAY_ALLOWED_ORIGINS` | `localhost:3000,127.0.0.1:3000` | Browser origins (host patterns) allowed to connect. Wildcards are rejected. Non-browser clients without an `Origin` header are allowed. |
| `GATEWAY_SEED` | `20260927` | Simulator seed |
| `GATEWAY_TICK_INTERVAL` | `1s` | Quote interval per symbol |
| `GATEWAY_MAX_SYMBOLS_PER_SUBSCRIBE` | `50` | Symbols per subscribe message |
| `GATEWAY_MAX_SUBSCRIBED_SYMBOLS` | `100` | Symbols per connection |
| `GATEWAY_MAX_INBOUND_MESSAGE_BYTES` | `4096` | Maximum client message size |
| `GATEWAY_HEARTBEAT_INTERVAL` | `15s` | Heartbeat and ping interval |
| `GATEWAY_WRITE_TIMEOUT` | `5s` | Per-frame write timeout and pong timeout |
| `GATEWAY_SEND_QUEUE_SIZE` | `256` | Per-connection send queue capacity |
| `GATEWAY_MAX_CONNECTIONS` | `1000` | Concurrent WebSocket connections |
| `GATEWAY_SHUTDOWN_TIMEOUT` | `10s` | Graceful shutdown deadline |
| `GATEWAY_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` (JSON logs) |

## Running and testing

From the repository root:

```bash
make up-mock      # build and start the gateway (plus core infrastructure)
make probe        # connect a test client: connection, snapshots, ordered quotes
make test-unit    # unit, integration and contract-conformance tests with the race detector
make lint-go      # gofmt, go vet, golangci-lint
```

The integration tests start the gateway on a local test server, connect a real WebSocket client, and validate every message against the JSON Schemas in `contracts/schemas/`. After each test they verify that no goroutines are left running.

The container image is built on a distroless base, runs as a non-root user, and has a read-only filesystem. The `realtime-gateway healthcheck` subcommand provides the container health check.
