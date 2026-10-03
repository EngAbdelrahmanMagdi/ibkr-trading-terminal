from prometheus_client import Counter, Gauge, Histogram

PROCESSED = Counter("ai_articles_processed_total", "Handled articles", ["outcome"])
FAILURES = Counter("ai_processing_failure_total", "Classified failures", ["category"])
SECONDS = Histogram("ai_processing_seconds", "Article processing time")
CACHE = Counter("ai_cache_hit_total", "Validated content-cache reuse")
TOKENS = Counter("ai_token_usage_total", "Provider-reported tokens", ["direction"])
COST = Gauge("ai_estimated_cost", "Conservative daily reserved dollars")
READY = Gauge("ai_ready", "Configuration, Kafka and state ready")
UNCERTAIN = Counter("ai_uncertain_attempts_total", "Interrupted requests not automatically retried")
