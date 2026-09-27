"""Contract tests (Python): canonical JSON Schemas vs. golden fixtures.

Checks, for every message schema under contracts/schemas:
  * the schema is valid JSON Schema 2020-12 and its $id matches its file location;
  * it has at least one valid and one invalid fixture (and no fixture set is orphaned);
  * valid fixtures validate, invalid fixtures are rejected (format assertions enabled);
  * valid fixtures survive a JSON parse -> serialize -> parse round trip unchanged;
  * decimal strings convert to Decimal and back without any change, and safe integers stay exact.
"""
import json
import os
import pathlib
import re
import unittest
from decimal import Decimal

from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

HERE = pathlib.Path(__file__).resolve().parent
CONTRACTS = pathlib.Path(os.environ.get("CONTRACTS_DIR", HERE / "../../../contracts")).resolve()
FIXTURES = pathlib.Path(os.environ.get("FIXTURES_DIR", HERE / "../fixtures")).resolve()
SCHEMAS_DIR = CONTRACTS / "schemas"
BASE_ID = "https://contracts.trading-terminal.invalid/schemas/"
ROOT_KEYWORDS = ("type", "allOf", "oneOf", "anyOf", "$ref")
DECIMAL_STRING = re.compile(r"^-?[0-9]+\.[0-9]+$")
MAX_SAFE_INTEGER = 9007199254740991


def read_json(path: pathlib.Path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_schemas() -> dict:
    return {p.relative_to(SCHEMAS_DIR).as_posix(): read_json(p) for p in sorted(SCHEMAS_DIR.rglob("*.schema.json"))}


SCHEMAS = load_schemas()
REGISTRY = Registry().with_resources(
    (doc["$id"], Resource.from_contents(doc, default_specification=DRAFT202012)) for doc in SCHEMAS.values()
)
FORMAT_CHECKER = Draft202012Validator.FORMAT_CHECKER


def message_schemas() -> dict:
    return {rel: doc for rel, doc in SCHEMAS.items() if any(k in doc for k in ROOT_KEYWORDS)}


def fixture_dir(rel: str) -> pathlib.Path:
    return FIXTURES / rel[: -len(".schema.json")]


def fixtures(rel: str, kind: str) -> list:
    return sorted((fixture_dir(rel) / kind).glob("*.json"))


def validator(rel: str) -> Draft202012Validator:
    return Draft202012Validator(SCHEMAS[rel], registry=REGISTRY, format_checker=FORMAT_CHECKER)


def strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from strings(v)
    elif isinstance(value, list):
        for v in value:
            yield from strings(v)


class SchemaTests(unittest.TestCase):
    def test_schemas_found(self):
        self.assertGreater(len(SCHEMAS), 0, f"no schemas under {SCHEMAS_DIR}")

    def test_format_assertions_are_active(self):
        # Without these checkers the 'format' keyword would silently pass.
        for fmt in ("date-time", "uuid"):
            self.assertIn(fmt, FORMAT_CHECKER.checkers, f"format checker for {fmt!r} is not installed")

    def test_schemas_are_valid_and_ids_match_locations(self):
        for rel, doc in SCHEMAS.items():
            with self.subTest(schema=rel):
                Draft202012Validator.check_schema(doc)
                self.assertEqual(doc.get("$schema"), "https://json-schema.org/draft/2020-12/schema")
                self.assertEqual(doc.get("$id"), BASE_ID + rel)


class FixtureTests(unittest.TestCase):
    def test_every_message_schema_has_valid_and_invalid_fixtures(self):
        for rel in message_schemas():
            with self.subTest(schema=rel):
                self.assertTrue(fixtures(rel, "valid"), f"no valid fixtures in {fixture_dir(rel)}")
                self.assertTrue(fixtures(rel, "invalid"), f"no invalid fixtures in {fixture_dir(rel)}")

    def test_no_orphaned_fixture_sets(self):
        expected = {fixture_dir(rel).resolve() for rel in message_schemas()}
        for kind_dir in FIXTURES.rglob("*"):
            if kind_dir.is_dir() and kind_dir.name in ("valid", "invalid"):
                with self.subTest(fixtures=str(kind_dir)):
                    self.assertIn(kind_dir.parent.resolve(), expected)

    def test_valid_fixtures_validate(self):
        for rel in message_schemas():
            v = validator(rel)
            for path in fixtures(rel, "valid"):
                with self.subTest(schema=rel, fixture=path.name):
                    errors = [e.message for e in v.iter_errors(read_json(path))]
                    self.assertEqual(errors, [])

    def test_invalid_fixtures_are_rejected(self):
        for rel in message_schemas():
            v = validator(rel)
            for path in fixtures(rel, "invalid"):
                with self.subTest(schema=rel, fixture=path.name):
                    self.assertFalse(v.is_valid(read_json(path)), "invalid fixture was accepted")


class SerializationTests(unittest.TestCase):
    def test_round_trip_preserves_valid_fixtures(self):
        for rel in message_schemas():
            v = validator(rel)
            for path in fixtures(rel, "valid"):
                with self.subTest(schema=rel, fixture=path.name):
                    original = read_json(path)
                    again = json.loads(json.dumps(original))
                    self.assertEqual(again, original)
                    self.assertTrue(v.is_valid(again))

    def test_decimal_strings_are_exact(self):
        count = 0
        for rel in message_schemas():
            for path in fixtures(rel, "valid"):
                for s in strings(read_json(path)):
                    if DECIMAL_STRING.match(s):
                        count += 1
                        with self.subTest(fixture=str(path), value=s):
                            self.assertEqual(format(Decimal(s), "f"), s)
        self.assertGreater(count, 0)

    def test_max_safe_integer_is_exact(self):
        quote = read_json(FIXTURES / "stream/quote/valid/max-safe-volume.json")
        self.assertEqual(quote["volume"], MAX_SAFE_INTEGER)
        self.assertEqual(json.loads(json.dumps(quote))["volume"], MAX_SAFE_INTEGER)


if __name__ == "__main__":
    unittest.main()
