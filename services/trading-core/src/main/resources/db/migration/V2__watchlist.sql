CREATE TABLE watchlists (
    id         uuid        PRIMARY KEY,
    name       varchar(64) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL
);

CREATE TABLE watchlist_items (
    watchlist_id uuid        NOT NULL REFERENCES watchlists (id) ON DELETE CASCADE,
    symbol       varchar(12) NOT NULL REFERENCES instruments (symbol),
    position     integer     NOT NULL CHECK (position >= 0),
    added_at     timestamptz NOT NULL,
    PRIMARY KEY (watchlist_id, symbol)
);

CREATE INDEX watchlist_items_position_idx ON watchlist_items (watchlist_id, position);
