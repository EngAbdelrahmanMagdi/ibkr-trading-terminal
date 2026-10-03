"""Bounded restored operational state. Never evict a live paid identity to admit work."""

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from .config import Settings
from .contracts import Contracts


def utc() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class State:
    def __init__(self, settings: Settings, contracts: Contracts) -> None:
        self.settings = settings
        self.contracts = contracts
        self.values: dict[str, dict[str, Any]] = {}

    def record(self, kind: str, identity: str, data: dict[str, Any]) -> dict[str, Any]:
        result = {
            "version": 1,
            "kind": kind,
            "identity": identity,
            "expiresAt": (datetime.now(UTC) + timedelta(seconds=self.settings.state_ttl))
            .isoformat()
            .replace("+00:00", "Z"),
            "data": data,
        }
        self.contracts.validate("events/news-ai-state.schema.json", result)
        return result

    def check(self, changes: Mapping[str, dict[str, Any] | None]) -> None:
        future = self.values | {k: v for k, v in changes.items() if v is not None}
        for k, v in changes.items():
            if v is None:
                future.pop(k, None)
        if (
            len(future) > self.settings.state_keys
            or sum(len(k.encode()) + len(json.dumps(v).encode()) for k, v in future.items())
            > self.settings.state_bytes
        ):
            raise ValueError("state_capacity")

    def apply(self, changes: Mapping[str, dict[str, Any] | None]) -> None:
        self.check(changes)
        for key, value in changes.items():
            if value is None:
                self.values.pop(key, None)
            else:
                self.contracts.validate("events/news-ai-state.schema.json", value)
                if key != value["kind"] + ":" + value["identity"]:
                    raise ValueError("state_key")
                self.values[key] = value

    def get(self, key: str) -> dict[str, Any] | None:
        record = self.values.get(key)
        if record is None or datetime.fromisoformat(record["expiresAt"]) <= datetime.now(UTC):
            return None
        result: dict[str, Any] = record["data"]
        return result

    def expired(self) -> dict[str, None]:
        return {
            key: None
            for key, value in list(self.values.items())[: self.settings.state_keys]
            if datetime.fromisoformat(value["expiresAt"]) <= datetime.now(UTC)
        }
