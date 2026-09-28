-- The runtime mode and account this database belongs to. A database holds the data of exactly one mode and
-- account (simulated and broker data must never mix); the application refuses to start on a mismatch.
CREATE TABLE runtime_binding (
    singleton    boolean      PRIMARY KEY DEFAULT true CHECK (singleton),
    runtime_mode varchar(16)  NOT NULL CHECK (runtime_mode IN ('MOCK', 'IBKR_PAPER')),
    account_id   varchar(64)  NOT NULL,
    bound_at     timestamptz  NOT NULL
);
