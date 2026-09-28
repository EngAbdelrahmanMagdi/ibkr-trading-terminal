-- Transactional outbox: every domain event is inserted in the transaction that changes the state it describes, and
-- published to Kafka afterwards by a relay. Unpublished rows are never deleted.
CREATE TABLE outbox_events (
    id               uuid          PRIMARY KEY,
    -- Insertion order; per aggregate it equals commit order (order updates are serialized by a row lock).
    seq              bigint        GENERATED ALWAYS AS IDENTITY UNIQUE,
    aggregate_type   varchar(32)   NOT NULL CHECK (aggregate_type IN ('ORDER')),
    aggregate_id     uuid          NOT NULL,
    topic            varchar(128)  NOT NULL,
    record_key       varchar(200)  NOT NULL,
    event_type       varchar(64)   NOT NULL,
    event_version    integer       NOT NULL CHECK (event_version > 0),
    payload          jsonb         NOT NULL,
    status           varchar(16)   NOT NULL CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    created_at       timestamptz   NOT NULL,
    published_at     timestamptz,
    attempt_count    integer       NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at  timestamptz   NOT NULL,
    last_error       varchar(500),
    -- Publisher lease: a claim that is not completed becomes claimable again once it expires.
    claimed_by       uuid,
    claim_expires_at timestamptz,
    CHECK ((claimed_by IS NULL) = (claim_expires_at IS NULL)),
    CHECK ((status = 'PUBLISHED') = (published_at IS NOT NULL))
);

CREATE INDEX outbox_events_status_next_attempt_idx ON outbox_events (status, next_attempt_at);
-- Head-of-line check: the earliest unpublished event of each key.
CREATE INDEX outbox_events_unpublished_key_idx ON outbox_events (topic, record_key, seq) WHERE status <> 'PUBLISHED';
-- Retention purge of published events.
CREATE INDEX outbox_events_published_at_idx ON outbox_events (published_at) WHERE status = 'PUBLISHED';
