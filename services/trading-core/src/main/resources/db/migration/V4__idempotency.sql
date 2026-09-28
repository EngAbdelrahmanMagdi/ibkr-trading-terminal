-- One row per Idempotency-Key: the fingerprint detects reuse with a different body; the stored outcome is
-- replayed to retries (including 4xx outcomes).
CREATE TABLE idempotency_records (
    idempotency_key     uuid        PRIMARY KEY,
    request_fingerprint varchar(64) NOT NULL,
    order_id            uuid        REFERENCES orders (id),
    response_status     integer     CHECK (response_status BETWEEN 200 AND 499),
    problem_body        text,
    created_at          timestamptz NOT NULL,
    completed_at        timestamptz
);
