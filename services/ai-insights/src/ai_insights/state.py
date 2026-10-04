"""Bounded restored operational state. Never evict a live paid identity to admit work."""

import json
import re
from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from .config import Settings
from .contracts import Contracts


def utc() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class State:
    def __init__(self, settings: Settings, contracts: Contracts) -> None:
        self.settings = settings
        self.contracts = contracts
        self.values: dict[str, dict[str, Any]] = {}
        self._sizes: dict[str, int] = {}
        self._bytes = 0

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
        self.validate(changes)
        count, size = len(self.values), self._bytes
        for key, value in changes.items():
            count -= int(key in self.values)
            size -= self._sizes.get(key, 0)
            if value is not None:
                count += 1
                size += len(key.encode()) + len(json.dumps(value).encode())
        if count > self.settings.state_keys or size > self.settings.state_bytes:
            raise ValueError("state_capacity")

    def apply(self, changes: Mapping[str, dict[str, Any] | None]) -> None:
        self.check(changes)
        for key, value in changes.items():
            self._bytes -= self._sizes.pop(key, 0)
            if value is None:
                self.values.pop(key, None)
            else:
                self.values[key] = deepcopy(value)
                size = len(key.encode()) + len(json.dumps(value).encode())
                self._sizes[key] = size
                self._bytes += size

    def validate(self, changes: Mapping[str, dict[str, Any] | None]) -> None:
        # Validate the whole change set before mutation or durable publication.
        for key, value in changes.items():
            if len(key.encode()) > 256:
                raise ValueError("state_record_bounds")
            if value is None:
                continue
            if len(json.dumps(value).encode()) > 131072:
                raise ValueError("state_record_bounds")
            self.contracts.validate("events/news-ai-state.schema.json", value)
            if key != value["kind"] + ":" + value["identity"]:
                raise ValueError("state_key")
            data = value["data"]
            if value["kind"] == "CONTENT" and not re.fullmatch(r"[a-f0-9]{64}", value["identity"]):
                raise ValueError("state_content_identity")
            if value["kind"] == "BUDGET":
                if date.fromisoformat(value["identity"]).isoformat() != value["identity"]:
                    raise ValueError("state_budget_identity")
            if value["kind"] == "ARTICLE":
                if value["identity"] != str(
                    uuid5(NAMESPACE_URL, data["articleId"] + ":" + data["processingIdentity"])
                ):
                    raise ValueError("state_article_identity")
                event = data.get("event")
                if event is not None and (
                    event["eventId"] != value["identity"]
                    or event["payload"]["articleId"] != data["articleId"]
                ):
                    raise ValueError("state_event_identity")

    def get(self, key: str) -> dict[str, Any] | None:
        record = self.values.get(key)
        if record is None or datetime.fromisoformat(record["expiresAt"]) <= datetime.now(UTC):
            return None
        result: dict[str, Any] = deepcopy(record["data"])
        return result

    def expired(self) -> dict[str, None]:
        return {
            key: None
            for key, value in list(self.values.items())[: self.settings.state_keys]
            if datetime.fromisoformat(value["expiresAt"]) <= datetime.now(UTC)
        }
