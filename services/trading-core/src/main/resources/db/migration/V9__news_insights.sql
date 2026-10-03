CREATE TABLE news_insights (
    article_id UUID PRIMARY KEY REFERENCES news_articles(id) ON DELETE CASCADE,
    accepted_processing_id UUID NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    prompt_version VARCHAR(64) NOT NULL,
    model VARCHAR(128) NOT NULL,
    model_version VARCHAR(128),
    enriched_at TIMESTAMPTZ NOT NULL,
    insight JSONB NOT NULL
);
COMMENT ON TABLE news_insights IS 'First accepted informational interpretation per article; no automatic replacement';
