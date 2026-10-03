"""Offline canonical schema registry, including UTC format assertions."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource


class Contracts:
    def __init__(self, root: Path) -> None:
        self.schemas = {
            p.relative_to(root).as_posix(): json.loads(p.read_text())
            for p in root.rglob("*.schema.json")
        }
        registry: Registry[Any] = Registry().with_resources(
            (s["$id"], Resource.from_contents(s)) for s in self.schemas.values()
        )
        checker = FormatChecker()

        def timestamp(value: Any) -> bool:
            return isinstance(value, str) and datetime.fromisoformat(value).tzinfo is not None

        checker.checks("date-time", raises=ValueError)(timestamp)

        self.validators = {
            name: Draft202012Validator(schema, registry=registry, format_checker=checker)
            for name, schema in self.schemas.items()
        }

    def validate(self, name: str, value: Any) -> None:
        self.validators[name].validate(value)

    def provider_schema(self) -> dict[str, Any]:
        def resolve(value: Any, location: str) -> Any:
            if isinstance(value, list):
                return [resolve(item, location) for item in value]
            if not isinstance(value, dict):
                return value
            if "$ref" in value:
                file, _, fragment = value["$ref"].partition("#")
                target = (Path(location).parent / file).as_posix() if file else location
                # Local references only; normalize news/../common without network retrieval.
                target = str(Path(target)).replace("\\", "/")
                import posixpath

                target = posixpath.normpath(target)
                doc = self.schemas[target]
                for part in fragment.strip("/").split("/") if fragment else []:
                    doc = doc[part]
                return resolve(doc, target)
            return {
                k: resolve(v, location)
                for k, v in value.items()
                if k not in {"$schema", "$id", "$defs", "uniqueItems"}
            }

        result: dict[str, Any] = resolve(
            self.schemas["news/news-insight.schema.json"], "news/news-insight.schema.json"
        )
        return result
