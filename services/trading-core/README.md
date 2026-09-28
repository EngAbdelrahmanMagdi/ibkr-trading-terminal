# Trading Core

This Spring Boot service is the system of record for trading. It owns order validation and the order lifecycle, idempotent order submission, executions, positions and P&L, the portfolio summary, the watchlist and instrument resolution. It is the only service that writes to PostgreSQL.

The broker sits behind a port (`BrokerTradingPort`). In `MOCK` mode a simulated broker implements it, so the complete trading workflow runs without any brokerage account.

## Runtime modes

`APP_RUNTIME_MODE` is read once, at startup, and only selects adapters. No business code checks the mode.

| Mode | Broker | Quotes used for fills and valuation |
|---|---|---|
| `MOCK` (default) | The simulated broker described below | `quote:MOCK:{SYMBOL}` in Redis, written by the realtime gateway's simulator |
| `IBKR_PAPER` | Not available in this version: startup **fails closed** with a clear message | |

There is no live-money mode.

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

## Positions and portfolio

Positions use the average-cost method for longs and shorts. Reductions realize P&L against the average cost, and crossing zero opens the new side at the fill price. Commissions are part of cash, not of the average cost.

In `MOCK` the portfolio is a simulated cash account:

| Metric | Value |
|---|---|
| `cash`, `buyingPower` | Starting cash (100,000 by default) plus sales, minus purchases and commissions |
| `realizedPnl` | From executions |
| `unrealizedPnl`, `netLiquidation`, position `marketValue` | From fresh quotes (last price). Unavailable while any open position has no fresh quote. |
| `excessLiquidity`, `dayPnl` | Always unavailable: they can't be derived honestly from the simulation |

## Persistence

Flyway migrations (`src/main/resources/db/migration`) create the schema: `NUMERIC` for money, prices and quantities; `timestamptz` for time; CHECK constraints for every enumerated column. Migrations run as the schema owner role. The application connects as a role with data access only, and Hibernate only validates the schema. Orders use optimistic locking plus a row lock while broker updates are applied.

## Running and testing

From the repository root:

```bash
make up-mock     # PostgreSQL, Redis, Kafka, the realtime gateway and Trading Core
make test-core   # domain, application, architecture and integration tests
make lint-java   # compile with warnings as errors, Maven enforcer rules, architecture rules
```

The integration suite runs against real PostgreSQL (initialized with the same role setup as the local stack) and Redis (with an ACL user) through Testcontainers. It drives the complete workflow over HTTP and validates every response against the contract schemas.

## Configuration

Settings are in `src/main/resources/application.yml`. The main environment variables are:

| Variable | Default | Purpose |
|---|---|---|
| `APP_RUNTIME_MODE` | `MOCK` | Selects the broker adapter |
| `DB_HOST`, `DB_PORT`, `DB_NAME` | `localhost`, `15432`, `trading` | PostgreSQL |
| `DB_APP_USER`, `DB_OWNER_USER` | `trading_app`, `trading_owner` | Runtime role and migration role |
| `REDIS_HOST`, `REDIS_PORT`, `REDIS_USER` | `localhost`, `16379`, `app` | Redis |
| `APP_CORS_ALLOWED_ORIGINS` | `http://localhost:3000,http://127.0.0.1:3000` | Exact browser origins allowed to call the API |
| `SECRETS_DIR` | `/run/secrets/` | Directory of the secret files `trading_app_password`, `trading_owner_password` and `redis_app_password` |

Passwords are read only from those secret files, never from defaults.
