-- One durable opening valuation per symbol; replaced only on the next New York day.
CREATE TABLE position_day_openings (
    symbol varchar(12) PRIMARY KEY REFERENCES instruments (symbol),
    trading_day date NOT NULL,
    quantity numeric(19,4) NOT NULL,
    market_value numeric(19,4) NOT NULL,
    mark_at timestamptz NOT NULL
);
