-- Resolved instrument metadata: a symbol is resolved through the broker catalog once and persisted.
CREATE TABLE instruments (
    symbol      varchar(12)  PRIMARY KEY CHECK (symbol ~ '^[A-Z][A-Z0-9.-]{0,11}$'),
    conid       bigint       NOT NULL UNIQUE CHECK (conid > 0),
    name        varchar(200),
    exchange    varchar(32)  NOT NULL,
    currency    varchar(3)   NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    asset_type  varchar(8)   NOT NULL CHECK (asset_type IN ('STK')),
    price_scale integer      NOT NULL CHECK (price_scale BETWEEN 0 AND 6),
    resolved_at timestamptz  NOT NULL
);
