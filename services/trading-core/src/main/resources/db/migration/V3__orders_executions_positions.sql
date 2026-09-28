-- Orders follow the lifecycle state machine; money and quantities are NUMERIC, timestamps are UTC.
CREATE TABLE orders (
    id                 uuid          PRIMARY KEY,
    client_order_id    varchar(64)   NOT NULL UNIQUE,
    broker_order_id    varchar(64)   UNIQUE,
    account_id         varchar(64)   NOT NULL,
    conid              bigint        NOT NULL,
    symbol             varchar(12)   NOT NULL REFERENCES instruments (symbol),
    intent             varchar(8)    NOT NULL CHECK (intent IN ('BUY', 'SELL', 'SHORT')),
    broker_side        varchar(4)    NOT NULL CHECK (broker_side IN ('BUY', 'SELL')),
    order_type         varchar(8)    NOT NULL CHECK (order_type IN ('MARKET', 'LIMIT')),
    quantity           numeric(19,4) NOT NULL CHECK (quantity > 0),
    filled_quantity    numeric(19,4) NOT NULL CHECK (filled_quantity >= 0 AND filled_quantity <= quantity),
    limit_price        numeric(19,6) CHECK (limit_price > 0),
    average_fill_price numeric(19,6) CHECK (average_fill_price > 0),
    time_in_force      varchar(4)    NOT NULL CHECK (time_in_force IN ('DAY', 'GTC')),
    status             varchar(24)   NOT NULL CHECK (status IN ('CREATED', 'SUBMISSION_PENDING', 'PENDING_CONFIRMATION',
                                         'SUBMITTED', 'PARTIALLY_FILLED', 'FILLED', 'CANCEL_PENDING', 'CANCELLED',
                                         'REJECTED', 'FAILED', 'UNKNOWN')),
    reply_id           varchar(128),
    reply_message      varchar(2000),
    reply_depth        integer       NOT NULL CHECK (reply_depth >= 0),
    rejection_reason   varchar(500),
    created_at         timestamptz   NOT NULL,
    submitted_at       timestamptz,
    updated_at         timestamptz   NOT NULL,
    version            bigint        NOT NULL,
    CONSTRAINT orders_limit_price_matches_type CHECK ((order_type = 'LIMIT') = (limit_price IS NOT NULL))
);

CREATE INDEX orders_created_idx ON orders (created_at DESC);
CREATE INDEX orders_status_created_idx ON orders (status, created_at DESC);

-- Each broker execution is recorded once (unique broker_execution_id makes duplicate updates harmless).
CREATE TABLE executions (
    id                  uuid          PRIMARY KEY,
    order_id            uuid          NOT NULL REFERENCES orders (id),
    broker_execution_id varchar(64)   NOT NULL UNIQUE,
    symbol              varchar(12)   NOT NULL,
    side                varchar(4)    NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity            numeric(19,4) NOT NULL CHECK (quantity > 0),
    price               numeric(19,6) NOT NULL CHECK (price > 0),
    commission          numeric(19,4) CHECK (commission >= 0),
    currency            varchar(3)    NOT NULL,
    executed_at         timestamptz   NOT NULL
);

CREATE INDEX executions_order_idx ON executions (order_id);
CREATE INDEX executions_executed_idx ON executions (executed_at DESC);

-- Position read model, updated in the same transaction as each execution (average-cost method).
CREATE TABLE positions (
    symbol       varchar(12)   PRIMARY KEY REFERENCES instruments (symbol),
    quantity     numeric(19,4) NOT NULL,
    average_cost numeric(19,6) NOT NULL CHECK (average_cost >= 0),
    realized_pnl numeric(19,4) NOT NULL,
    currency     varchar(3)    NOT NULL,
    updated_at   timestamptz   NOT NULL,
    version      bigint        NOT NULL
);
