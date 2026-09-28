# Trading Core

This Spring Boot service is the system of record for trading. It owns order validation and the order lifecycle, idempotent order submission, executions, positions and P&L, the portfolio summary, the watchlist and instrument resolution. It is the only service that writes to PostgreSQL.

The broker sits behind a port (`BrokerTradingPort`). In `MOCK` mode a simulated broker implements it, so the complete trading workflow runs without any brokerage account. In `IBKR_PAPER` mode an adapter trades an Interactive Brokers **paper** account through the Client Portal Gateway running on the developer's machine.

## Runtime modes

`APP_RUNTIME_MODE` is read once, at startup, and only selects adapters. No business code checks the mode.

| Mode | Broker | Quotes used for fills and valuation |
|---|---|---|
| `MOCK` (default) | The simulated broker described below | `quote:MOCK:{SYMBOL}` in Redis, written by the realtime gateway's simulator |
| `IBKR_PAPER` | The IBKR paper account, described below. Trusted, private environments only | `quote:IBKR:{SYMBOL}` in Redis, written by the realtime gateway from IBKR market data |

There is no live-money mode.

A database belongs to one runtime mode and account. Trading Core records both on first start and refuses to start when they differ, so simulated instruments, orders and positions can never be traded against the paper account. Switching modes needs a separate or clean database.

## API

The REST API follows [`contracts/openapi/trading-api.yaml`](../../contracts/openapi/trading-api.yaml). Prices, quantities and money are decimal strings; timestamps are UTC with a trailing `Z`. Every error is a problem body with a stable `category` and the request's correlation ID, and never includes stack traces or internals.

| Method and path | Result |
|---|---|
| `POST /api/v1/orders` | Places an order. Requires `Idempotency-Key` (UUID). `201` when the broker accepted (or definitively rejected) it, `202` while it waits for a confirmation (`PENDING_CONFIRMATION`) or its outcome is being verified (`UNKNOWN`). |
| `POST /api/v1/orders/{id}/confirmation` | Confirms or declines a broker confirmation request (`{"confirm": true}`) |
| `DELETE /api/v1/orders/{id}` | Requests cancellation (`202`, `CANCEL_PENDING` until the broker confirms) |
| `GET /api/v1/orders?status=&limit=` | Orders, newest first |
| `GET /api/v1/executions?orderId=&limit=` | Fills, newest first |
| `GET /api/v1/positions` | Open positions with average cost, market value and P&L |
| `GET /api/v1/portfolio` | Account metrics |
| `GET /api/v1/watchlist`, `POST /api/v1/watchlist/items`, `DELETE /api/v1/watchlist/items/{symbol}` | The persisted watchlist |
| `GET /api/v1/instruments/search?q=&limit=` | Instruments with their shortability |

Health (`/actuator/health/liveness`, `/actuator/health/readiness`) and Prometheus metrics (`/actuator/prometheus`) are served on the separate management port `8081` only.

## Orders

### Lifecycle

`CREATED → SUBMISSION_PENDING → SUBMITTED → (PARTIALLY_FILLED) → FILLED`, with `PENDING_CONFIRMATION`, `CANCEL_PENDING`, `CANCELLED`, `REJECTED`, `FAILED` and `UNKNOWN`. Only the transitions of the lifecycle table are allowed; anything else is refused and counted. Filled quantity only grows, duplicate or out-of-date broker updates are ignored, and fills that arrive after a cancel request are still applied.

### Placement

1. The `Idempotency-Key` is claimed (the key's primary key arbitrates concurrent requests).
2. The order is validated: known instrument, positive whole-share quantity, limit price within the instrument's tick scale (validated, never rounded), configured maximum quantity and order value, broker availability, no other order waiting for confirmation, and the position and shortability rules below. A market order's value uses the fresh quote side it would trade against; without a fresh quote it is refused with `STALE_MARKET_DATA`.
3. The order is recorded (`SUBMISSION_PENDING`) in one transaction.
4. The broker is called **once**, with no database transaction open. A submission is never retried; an unexpected failure makes the order `UNKNOWN`.
5. The outcome is recorded in a second transaction, together with the idempotency record.

Retrying with the same key and the same request returns the stored outcome (including a stored rejection) and never creates a second order. Reusing a key for a different request returns `409 CONFLICT`.

### Intent, side and position rules

| Intent | Broker side | Rule |
|---|---|---|
| `BUY` | BUY | Always allowed (it covers a short) |
| `SELL` | SELL | Only up to the long quantity not already reserved by open sell orders |
| `SHORT` | SELL | Only without a long position. Blocked when the broker reports `NOT_SHORTABLE`; allowed when shortability is `UNAVAILABLE` (unknown or stale), leaving the decision to the broker |

### Fills

Fills, confirmed cancellations and rejections of working orders all go through one use case. In one transaction it records the execution (each broker execution ID once), moves the order and updates the position.

## Simulated broker (`MOCK`)

- **Market orders fill immediately and in full** at the fresh simulated quote: a buy at the ask, a sell or short at the bid. This includes an order that needed confirmation, which fills at the quote current at the final confirmation. If no fresh quote exists at that moment, the order is rejected rather than filled at an old price.
- **Limit orders rest** in a small, bounded book. A scheduled matcher re-evaluates them against the fresh simulated quotes (every second by default): a buy fills when the ask is at or below the limit, a sell when the bid is at or above it, in full, at the quote. Requested cancellations are confirmed on the next evaluation unless the order filled first. The book is rebuilt from PostgreSQL on start.
- **Confirmations:** orders worth more than 50,000 need one confirmation, and more than 250,000 a second, chained one (configurable).
- **Commission:** a flat 0.005 per share with a 1.00 minimum (configurable).
- **Instruments:** NVDA, AAPL, META, AMD, IONQ, MSFT, TSLA and SPY, matching the market simulator. For demonstration, IONQ is configured as not shortable and TSLA's shortability as unavailable. Borrow availability and fees are never invented (reported as null).
- **Quotes are demand-driven.** The realtime gateway streams, and caches in Redis, only the symbols that a client is subscribed to. A market order for a symbol nobody is watching is refused with `STALE_MARKET_DATA`, and resting limit orders fill only while their symbol is streaming.

## IBKR paper broker (`IBKR_PAPER`)

- **Deployment guard:** startup is refused unless `APP_TRUSTED_ENVIRONMENT=true` is set and every CORS origin is loopback or on a private network. The API has no login, so it must never be reachable publicly in this mode.
- **Session:** Trading Core never logs in and never keeps the IBKR session alive (the realtime gateway does). Before order commands it checks the session status and `/iserver/accounts`: the session must be established and not competing, report a paper account, and include the configured account. The check is reused for a few seconds.
- **TLS:** the gateway certificate must chain to the local CA in `secrets/ibkr/ca.pem` (`make ibkr-certs`); the host name is verified.
- **Orders:** a market order is sent as `MKT` and a limit order as `LMT`, with the client order ID as the order reference. `SHORT` is sent as a `SELL`. Order commands are serialized, and a new order is refused while a broker confirmation request is outstanding.
- **Confirmations:** IBKR may answer with a confirmation request (for example a price or size warning). It must be confirmed explicitly through the API, can chain (bounded depth), and expires after `app.orders.confirmation-ttl` (30 s by default): an expired or declined request is rejected locally and nothing is sent. The timeout is an application default; IBKR expects replies to be answered promptly.
- **Uncertain outcomes:** a submission that may have reached IBKR but got no readable answer (a timeout, a reset or a server error) becomes `UNKNOWN` and is never sent again.
- **Cancellation:** IBKR only acknowledges that the request was received; the order stays `CANCEL_PENDING` until IBKR reports it cancelled. Fills received in between are applied.
- **Order progress:** while this application has working IBKR orders it polls IBKR at most every 5 seconds (the documented limit of the live-orders and trades endpoints). Executions are applied once each, by their IBKR execution ID; a cancellation is applied only after every reported fill. An order missing from the broker's list is never assumed cancelled, and orders placed elsewhere are ignored.
- **Instruments:** a symbol resolves to exactly one US stock in USD (ambiguous matches are refused), with the price increment from the contract rules. It is stored and never resolved again.
- **Shortability:** from the shortable-shares market data field. 0 is `NOT_SHORTABLE` (the order is blocked locally). A positive count is `SHORTABLE`, with the fee rate when IBKR provides it. Anything else is `UNAVAILABLE`: the order is allowed and IBKR decides.
- **Pacing:** Trading Core uses its configured share of the IBKR session budget (`APP_IBKR_ALLOCATION`, 4 requests per second by default), with the documented per-endpoint limits, a bounded wait queue and a timeout. After a `429`, all requests pause for 15 minutes.

## Positions and portfolio

Positions use the average-cost method for longs and shorts. Reductions realize P&L against the average cost, and crossing zero opens the new side at the fill price. Commissions are part of cash, not of the average cost.

In `MOCK` the portfolio is a simulated cash account:

| Metric | Value |
|---|---|
| `cash`, `buyingPower` | Starting cash (100,000 by default) plus sales, minus purchases and commissions |
| `realizedPnl` | From executions |
| `unrealizedPnl`, `netLiquidation`, position `marketValue` | From fresh quotes (last price). Unavailable while any open position has no fresh quote. |
| `excessLiquidity`, `dayPnl` | Always unavailable: they can't be derived honestly from the simulation |

In `IBKR_PAPER`, `cash` (the ledger cash balance), `netLiquidation`, `excessLiquidity` and `dayPnl` come from the paper account when its base currency matches, refreshed at most every 10 seconds. `buyingPower` is unavailable. Positions and P&L are computed from the executions recorded here.

## Domain events (transactional outbox)

Every persisted order status change emits one event on `trading.order-events.v1`, and every recorded fill emits `EXECUTION_RECORDED` on `trading.execution-events.v1`. Events use the standard envelope from the event contracts and are keyed `accountId:orderId`.

- **Atomic:** the event row (`outbox_events`) is inserted in the same PostgreSQL transaction as the state change, so there is never a committed change without its event, or an event without its change. Placing an order never waits for Kafka.
- **Relay:** a background relay publishes the rows with an idempotent producer (`acks=all`). Each round:
  1. claims a bounded batch in one short transaction, as a lease;
  2. sends it with no transaction open;
  3. records the results in a second short transaction, fenced by the claim.

  An abandoned claim becomes claimable again when its lease expires.
- **Ordering:** an event is published only after every earlier event of the same key, so the events of one order arrive in order.
- **At least once:** a crash between Kafka's acknowledgement and the result update publishes the event again, with the same `eventId`. Consumers deduplicate by `eventId`.
- **Kafka unavailable or misconfigured** (unreachable, timeouts, authentication or authorization, missing metadata or topics):
  - events stay pending and their attempts are not counted;
  - the relay pauses with capped exponential backoff (1 s → 60 s), then probes once per interval;
  - everything drains automatically once Kafka is back, including after a restart.
- **An event Kafka refuses for itself** (serialization, too large) is retried with backoff and becomes `FAILED` after 10 attempts. That holds back only the later events of the same order, and the event stays visible in metrics until an operator requeues it.
- **Retention:** published events are deleted after 7 days by a bounded hourly job. Pending and failed events are never deleted.
- **Metrics:**
  - `outbox_pending`, `outbox_failed` and `outbox_oldest_pending_age_seconds`;
  - `outbox_published_total` and `outbox_publish_failure_total{kind=infrastructure|bad_record}`;
  - `outbox_publisher_backoff_seconds`, `outbox_claims_expired_total` and `outbox_publish_latency_seconds`.
- Kafka never affects startup or readiness.

## Persistence

Flyway migrations (`src/main/resources/db/migration`) create the schema: `NUMERIC` for money, prices and quantities; `timestamptz` for time; CHECK constraints for every enumerated column. Migrations run as the schema owner role. The application connects as a role with data access only, and Hibernate only validates the schema. Orders use optimistic locking plus a row lock while broker updates are applied.

## Running and testing

From the repository root:

```bash
make up-mock        # PostgreSQL, Redis, Kafka, the realtime gateway and Trading Core
make up-ibkr-paper  # the same on the IBKR paper account (trusted private environments; clean database)
make test-core      # domain, application, architecture and integration tests
make lint-java      # compile with warnings as errors, Maven enforcer rules, architecture rules
```

The integration suite runs against real PostgreSQL (initialized with the same role setup as the local stack), Redis (with an ACL user) and Kafka through Testcontainers. It drives the complete workflow over HTTP, validates every response and published event against the contract schemas, and pauses Kafka to show that committed events are published once it is back.

## Configuration

Settings are in `src/main/resources/application.yml`. The main environment variables are:

| Variable | Default | Purpose |
|---|---|---|
| `APP_RUNTIME_MODE` | `MOCK` | Selects the broker adapter |
| `APP_TRUSTED_ENVIRONMENT` | `false` | Must be `true` for `IBKR_PAPER` |
| `APP_ACCOUNT_ID` | `MOCK-ACCOUNT` | The account the database is bound to; in `IBKR_PAPER`, the paper account ID |
| `APP_IBKR_BASE_URL`, `APP_IBKR_CA_FILE` | `https://host.docker.internal:5000/v1/api`, `/run/secrets/ibkr_ca` | The Client Portal Gateway and the CA that signed its certificate (`IBKR_PAPER`) |
| `APP_IBKR_ALLOCATION` | `4` | This service's share (requests per second) of the IBKR session limit |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | Kafka brokers for the outbox relay |
| `APP_OUTBOX_PUBLISHER_ENABLED` | `true` | Whether this instance relays outbox events (events are always recorded) |
| `DB_HOST`, `DB_PORT`, `DB_NAME` | `localhost`, `15432`, `trading` | PostgreSQL |
| `DB_APP_USER`, `DB_OWNER_USER` | `trading_app`, `trading_owner` | Runtime role and migration role |
| `REDIS_HOST`, `REDIS_PORT`, `REDIS_USER` | `localhost`, `16379`, `app` | Redis |
| `APP_CORS_ALLOWED_ORIGINS` | `http://localhost:3000,http://127.0.0.1:3000` | Exact browser origins allowed to call the API |
| `SECRETS_DIR` | `/run/secrets/` | Directory of the secret files `trading_app_password`, `trading_owner_password` and `redis_app_password` |

Passwords are read only from those secret files, never from defaults.
