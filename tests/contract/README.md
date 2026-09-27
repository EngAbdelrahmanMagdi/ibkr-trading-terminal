# Contract Tests

These tests prove that the four implementation languages agree on every message shape defined in `contracts/schemas/`. They are deliberately minimal, isolated test harnesses, not application code.

| Harness | Validator | Also checks |
|---|---|---|
| `java/` | networknt json-schema-validator, Jackson | Decimal strings convert to `BigDecimal` and back unchanged |
| `go/` | santhosh-tekuri/jsonschema | Numbers decode as `json.Number` (never `float64`); `big.Rat` decimal exactness; typed struct round trip |
| `python/` | jsonschema | That the OpenAPI and AsyncAPI documents reference the canonical schemas instead of duplicating them |
| `typescript/` | Ajv (2020-12) with ajv-formats | Strict TypeScript; decimal strings convert to scaled `BigInt` and back exactly |

Every harness checks the same things against the shared golden fixtures in `fixtures/`:
1. Every schema is registered by its `$id`, and nothing is fetched remotely.
2. Every message schema has at least one valid and one invalid fixture.
3. Valid fixtures pass, and invalid fixtures are rejected. `format` assertions (`date-time`, `uuid`) are enabled in every language.
4. Valid fixtures survive a parse, serialize, parse round trip unchanged and still validate.
5. Integers up to 2^53-1 stay exact.

Fixtures live in `fixtures/<schema path without .schema.json>/{valid,invalid}/*.json`. Each invalid fixture is named after the rule it breaks, for example `price-as-number.json` or `unavailable-with-invented-quantity.json`.

## Running

```bash
make test-contract                                            # all four languages
make contract-java                                            # one language
make contract-go
make contract-python
make contract-typescript
```

Each target runs in a pinned container on a copy of the sources, so no local JDK, Go, Python or Node installation is needed.
