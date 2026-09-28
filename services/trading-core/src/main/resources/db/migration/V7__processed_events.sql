-- Idempotent Kafka consumers: an event is marked processed in the same transaction as its effects, so a redelivered
-- event changes nothing. Rows are kept longer than the topics' retention, then purged in bounded batches.
CREATE TABLE processed_events (
    consumer_name varchar(64) NOT NULL,
    event_id      uuid        NOT NULL,
    processed_at  timestamptz NOT NULL,
    PRIMARY KEY (consumer_name, event_id)
);

CREATE INDEX processed_events_processed_at_idx ON processed_events (processed_at);
