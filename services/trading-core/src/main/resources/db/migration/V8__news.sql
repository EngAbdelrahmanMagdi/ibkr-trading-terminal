ALTER TABLE outbox_events DROP CONSTRAINT outbox_events_aggregate_type_check;
ALTER TABLE outbox_events ADD CONSTRAINT outbox_events_aggregate_type_check
    CHECK (aggregate_type IN ('ORDER', 'NEWS_ARTICLE'));

CREATE TABLE news_articles (
    id uuid PRIMARY KEY,
    provider varchar(32) NOT NULL,
    provider_id varchar(200) NOT NULL,
    primary_symbol varchar(12) NOT NULL,
    headline varchar(500) NOT NULL,
    source varchar(200) NOT NULL,
    canonical_url varchar(2048) NOT NULL,
    canonical_url_hash char(64) NOT NULL,
    published_at timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL,
    raw_summary varchar(8000),
    content_hash char(64) NOT NULL,
    UNIQUE (provider, provider_id),
    -- SHA-256 is the practical URL identity; a theoretical collision is treated as a duplicate.
    UNIQUE (provider, canonical_url_hash)
);
CREATE INDEX news_articles_published_idx ON news_articles (published_at DESC, id DESC);
CREATE INDEX news_articles_content_hash_idx ON news_articles (content_hash);
CREATE TABLE news_article_symbols (
    article_id uuid NOT NULL REFERENCES news_articles(id) ON DELETE CASCADE,
    symbol varchar(12) NOT NULL,
    PRIMARY KEY (article_id, symbol)
);
CREATE INDEX news_article_symbols_symbol_idx ON news_article_symbols (symbol, article_id);
CREATE TABLE news_fetch_state (
    provider varchar(32) NOT NULL,
    symbol varchar(12) NOT NULL,
    last_attempt_at timestamptz NOT NULL,
    last_success_at timestamptz,
    failure varchar(32),
    PRIMARY KEY (provider, symbol)
);
