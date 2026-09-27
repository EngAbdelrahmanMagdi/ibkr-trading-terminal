"""Checks that OpenAPI and AsyncAPI documents reference the canonical JSON Schemas instead of duplicating them.

  * no inline object schemas ('properties') anywhere in the API documents;
  * every external $ref points to an existing file under contracts/schemas and, if present, to an existing JSON pointer;
  * OpenAPI component schemas and AsyncAPI message payloads/headers are pure references.
"""
import json
import pathlib
import unittest
from urllib.parse import unquote

import yaml

from test_contract import CONTRACTS, SCHEMAS_DIR

SPECS = sorted((CONTRACTS / "openapi").glob("*.yaml")) + sorted((CONTRACTS / "asyncapi").glob("*.yaml"))


def load_yaml(path: pathlib.Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def walk(node, path=()):
    yield path, node
    if isinstance(node, dict):
        for k, v in node.items():
            yield from walk(v, path + (str(k),))
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from walk(v, path + (str(i),))


def resolve_pointer(doc, pointer: str):
    node = doc
    for token in pointer.lstrip("/").split("/"):
        if not token:
            continue
        token = unquote(token).replace("~1", "/").replace("~0", "~")
        node = node[token]
    return node


class SpecReferenceTests(unittest.TestCase):
    def test_specs_found(self):
        self.assertEqual(len(SPECS), 4, [p.name for p in SPECS])

    def test_no_inline_object_schemas(self):
        for spec in SPECS:
            for path, node in walk(load_yaml(spec)):
                if isinstance(node, dict) and "properties" in node:
                    with self.subTest(spec=spec.name, at="/".join(path)):
                        self.fail("inline object schema; define it under contracts/schemas and $ref it")

    def test_external_refs_resolve_to_canonical_schemas(self):
        for spec in SPECS:
            for path, node in walk(load_yaml(spec)):
                if not (isinstance(node, dict) and isinstance(node.get("$ref"), str)):
                    continue
                ref = node["$ref"]
                if ref.startswith("#"):
                    continue
                with self.subTest(spec=spec.name, ref=ref):
                    file_part, _, pointer = ref.partition("#")
                    target = (spec.parent / file_part).resolve()
                    self.assertTrue(target.is_file(), f"missing {target}")
                    self.assertTrue(target.is_relative_to(SCHEMAS_DIR.resolve()), f"{target} is outside contracts/schemas")
                    if pointer:
                        resolve_pointer(json.loads(target.read_text(encoding="utf-8")), pointer)

    def test_openapi_component_schemas_are_pure_references(self):
        for spec in sorted((CONTRACTS / "openapi").glob("*.yaml")):
            for name, schema in load_yaml(spec).get("components", {}).get("schemas", {}).items():
                with self.subTest(spec=spec.name, schema=name):
                    self.assertEqual(list(schema), ["$ref"])
                    self.assertTrue(schema["$ref"].startswith("../schemas/"))

    def test_asyncapi_payloads_and_headers_are_pure_references(self):
        for spec in sorted((CONTRACTS / "asyncapi").glob("*.yaml")):
            for name, message in load_yaml(spec).get("components", {}).get("messages", {}).items():
                for part in ("payload", "headers"):
                    if part in message:
                        with self.subTest(spec=spec.name, message=name, part=part):
                            self.assertEqual(list(message[part]), ["$ref"])
                            self.assertTrue(message[part]["$ref"].startswith("../schemas/"))


if __name__ == "__main__":
    unittest.main()
