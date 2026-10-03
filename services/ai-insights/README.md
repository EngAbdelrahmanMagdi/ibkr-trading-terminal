# News insights worker

Consumes normalized news and publishes validated informational interpretations. It cannot access trading APIs,
broker sessions or application databases. Public runs use deterministic synthetic fixtures with model identity
`synthetic-news.v1`. Disabling this service does not disable raw news.

One active instance restores committed operational Kafka state before processing. Results are saved before
publication; publication, completion and raw offsets share a Kafka transaction. External inference is outside
that transaction. Interrupted private attempts without saved output remain uncertain and are not reinferred.

Private OpenAI configuration is explicit, disabled by default and requires verified content rights, model policy,
a server credential file, an approved budget and an operator-provided restricted proxy. The isolated worker must
have no direct Internet or trading-service routes. Do not attach another network to bypass this boundary.

Health and metrics are internal only. No prompts, provider responses or credentials are logged.
