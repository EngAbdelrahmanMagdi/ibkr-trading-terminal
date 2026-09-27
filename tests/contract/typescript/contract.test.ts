// Contract tests (TypeScript): canonical JSON Schemas vs. golden fixtures.
//
// For every message schema under contracts/schemas: valid fixtures validate, invalid fixtures are
// rejected (format assertions enabled), every schema has both kinds of fixtures, and valid fixtures
// survive a JSON round trip. Decimal strings are converted to scaled integers (BigInt) and back
// without loss, and integers stay within Number.MAX_SAFE_INTEGER.
import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import Ajv2020Module from "ajv/dist/2020.js";
import addFormatsModule from "ajv-formats";

// Both packages are CommonJS and also expose their export as `.default`.
const Ajv2020 = Ajv2020Module.default;
const addFormats = addFormatsModule.default;

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

const HERE = dirname(fileURLToPath(import.meta.url));
const CONTRACTS = resolve(process.env["CONTRACTS_DIR"] ?? join(HERE, "../../../contracts"));
const FIXTURES = resolve(process.env["FIXTURES_DIR"] ?? join(HERE, "../fixtures"));
const SCHEMAS_DIR = join(CONTRACTS, "schemas");
const BASE_ID = "https://contracts.trading-terminal.invalid/schemas/";
const ROOT_KEYWORDS = ["type", "allOf", "oneOf", "anyOf", "$ref"];
const DECIMAL_STRING = /^-?[0-9]+\.[0-9]+$/;

function readJson(path: string): Json {
  return JSON.parse(readFileSync(path, "utf8")) as Json;
}

function listFiles(dir: string, suffix: string): string[] {
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .flatMap((name) => {
      const path = join(dir, name);
      return statSync(path).isDirectory() ? listFiles(path, suffix) : path.endsWith(suffix) ? [path] : [];
    })
    .sort();
}

function toRel(path: string): string {
  return relative(SCHEMAS_DIR, path).split(sep).join("/");
}

const schemas = new Map<string, Record<string, Json>>();
for (const path of listFiles(SCHEMAS_DIR, ".schema.json")) {
  schemas.set(toRel(path), readJson(path) as Record<string, Json>);
}

// Strict mode stays on (unknown keywords, invalid schemas fail). Two checks are relaxed because the schemas
// compose by design: allOf/if-then branches constrain or require properties that the shared base schema
// (e.g. the event envelope) defines, without repeating "type" or the property definitions.
const ajv = new Ajv2020({ strict: true, strictTypes: false, strictRequired: false, allErrors: true });
addFormats(ajv);
for (const doc of schemas.values()) ajv.addSchema(doc);

const messageSchemas = [...schemas.entries()]
  .filter(([, doc]) => ROOT_KEYWORDS.some((k) => k in doc))
  .map(([rel]) => rel);

function fixtures(rel: string, kind: "valid" | "invalid"): string[] {
  return listFiles(join(FIXTURES, rel.slice(0, -".schema.json".length), kind), ".json");
}

function validator(rel: string) {
  const validate = ajv.getSchema(BASE_ID + rel);
  assert.ok(validate, `schema ${rel} is not registered`);
  return validate;
}

function* strings(value: Json): Generator<string> {
  if (typeof value === "string") yield value;
  else if (Array.isArray(value)) for (const v of value) yield* strings(v);
  else if (value !== null && typeof value === "object") for (const v of Object.values(value)) yield* strings(v);
}

// Exact decimal handling without floating point: "184.25" -> 18425n (scale 2) -> "184.25".
function decimalRoundTrip(text: string): string {
  const negative = text.startsWith("-");
  const [intPart = "", fracPart = ""] = (negative ? text.slice(1) : text).split(".");
  const scaled = BigInt(intPart + fracPart);
  const digits = scaled.toString().padStart(fracPart.length + 1, "0");
  const whole = digits.slice(0, digits.length - fracPart.length);
  const frac = digits.slice(digits.length - fracPart.length);
  return `${negative ? "-" : ""}${whole}${fracPart.length > 0 ? "." + frac : ""}`;
}

test("schemas are present and their $id matches their location", () => {
  assert.ok(schemas.size > 0, `no schemas under ${SCHEMAS_DIR}`);
  for (const [rel, doc] of schemas) {
    assert.equal(doc["$schema"], "https://json-schema.org/draft/2020-12/schema", rel);
    assert.equal(doc["$id"], BASE_ID + rel, rel);
  }
});

test("format assertions are active", () => {
  assert.equal(validator("stream/heartbeat.schema.json")({ type: "heartbeat", timestamp: "2026-13-45T25:61:61Z" }), false);
  assert.equal(validator("watchlist/watchlist.schema.json")({ id: "not-a-uuid", items: [] }), false);
});

test("every message schema has valid and invalid fixtures", () => {
  for (const rel of messageSchemas) {
    assert.ok(fixtures(rel, "valid").length > 0, `${rel}: no valid fixtures`);
    assert.ok(fixtures(rel, "invalid").length > 0, `${rel}: no invalid fixtures`);
  }
});

test("valid fixtures validate", () => {
  for (const rel of messageSchemas) {
    const validate = validator(rel);
    for (const path of fixtures(rel, "valid")) {
      const ok = validate(readJson(path));
      assert.ok(ok, `${rel}: ${path} should be valid: ${JSON.stringify(validate.errors)}`);
    }
  }
});

test("invalid fixtures are rejected", () => {
  for (const rel of messageSchemas) {
    const validate = validator(rel);
    for (const path of fixtures(rel, "invalid")) {
      assert.equal(validate(readJson(path)), false, `${rel}: ${path} should be rejected`);
    }
  }
});

test("round trip preserves valid fixtures", () => {
  for (const rel of messageSchemas) {
    const validate = validator(rel);
    for (const path of fixtures(rel, "valid")) {
      const original = readJson(path);
      const again = JSON.parse(JSON.stringify(original)) as Json;
      assert.deepStrictEqual(again, original, path);
      assert.ok(validate(again), `${path}: re-serialized document is invalid`);
    }
  }
});

test("decimal strings convert to scaled integers and back exactly", () => {
  let count = 0;
  for (const rel of messageSchemas) {
    for (const path of fixtures(rel, "valid")) {
      for (const s of strings(readJson(path))) {
        if (!DECIMAL_STRING.test(s)) continue;
        count++;
        assert.equal(decimalRoundTrip(s), s, `${path}: ${s}`);
      }
    }
  }
  assert.ok(count > 0, "no decimal strings found in fixtures");
});

test("integers stay within the safe range", () => {
  const quote = readJson(join(FIXTURES, "stream/quote/valid/max-safe-volume.json")) as Record<string, Json>;
  assert.equal(quote["volume"], Number.MAX_SAFE_INTEGER);
  assert.ok(Number.isSafeInteger(quote["volume"]));
});
