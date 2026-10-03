# API and Event Contracts

Every message the platform exchanges is defined here, once, as a **JSON Schema 2020-12** file. The API descriptions reference these schemas instead of repeating them.

| Path | Contents |
|---|---|
| `schemas/` | The canonical message schemas (the single source of truth) |
| `openapi/trading-api.yaml` | Trading Core REST API (OpenAPI 3.1) |
| `openapi/realtime-gateway-api.yaml` | Historical bars and health endpoints of the realtime gateway (OpenAPI 3.1) |
| `asyncapi/events.yaml` | Kafka topics, keys, headers and event payloads (AsyncAPI 3.1) |
| `asyncapi/market-stream.yaml` | Browser WebSocket protocol (AsyncAPI 3.1) |

## Schema layout

| Directory | Schemas |
|---|---|
| `common/` | Primitive types (`primitives`) and the error body (`problem`) |
| `trading/` | Order enums, order placement and confirmation requests, the order and execution resources |
| `portfolio/` | Portfolio summary, positions, and metrics that may be unavailable |
| `instruments/` | Instrument metadata and shortability |
| `watchlist/` | Watchlist resource and requests |
| `news/` | News articles, AI enrichment, structured insights |
| `market/` | Historical bars |
| `ops/` | Detailed health |
| `events/` | Kafka event envelope, record headers, and one schema per event stream |
| `stream/` | WebSocket client and server messages |

## Conventions

- **Money and prices** are decimal strings, never JSON numbers:
  - `Price`: up to 13 integer and 6 fractional digits;
  - `Money` and `Quantity`: up to 15 integer and 4 fractional digits.
- **Integers** never exceed 2^53-1, so every language, including JavaScript, represents them exactly.
- **Timestamps** are RFC 3339 in UTC with a trailing `Z`.
- **Identifiers:** every schema has an `$id` under `https://contracts.trading-terminal.invalid/schemas/`, mirroring its file path. The `.invalid` domain is reserved and never resolves, so tools must load the schemas from this directory and can never fetch them remotely. Cross-file references are relative paths.
- **Portable keywords only:** schemas use keywords whose meaning is identical in draft-07 (the AsyncAPI default schema format) and 2020-12. Regular expressions stay within the subset that ECMA-262, RE2, Java and Python treat identically: `[0-9]` instead of `\d`, and no lookarounds.
- **Strictness:** producers validate strictly (`additionalProperties: false`). Consumers must tolerate unknown fields and unknown message or event types, so that backward-compatible additions don't break them.
- **Compatibility:** within a major version, changes are additive only. Breaking changes get a new version: a new `.vN` Kafka topic, or a new major API version.

## Verification

- `make lint-contracts` lints the OpenAPI documents with Redocly and validates the AsyncAPI documents.
- `make test-contract` checks every schema against golden fixtures in Java, Go, Python and TypeScript (see `tests/contract/`).
## Worker operational state and dead letters

`news.ai-state.v1` is a single-partition compacted operational topic owned by the news worker.
It holds result-cache, completion and conservative budget state, not application news truth.
Null values are deletion tombstones. State expiry is enforced by the worker; compaction alone is not expiry.

`news.enriched.v1.dlq` preserves permanently invalid original payload bytes, including malformed JSON.
Its diagnostic headers have a separate schema. Database/Kafka outages are not poison records.
