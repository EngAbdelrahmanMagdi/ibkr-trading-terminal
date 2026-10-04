"""Reproduce state-admission profiling with identical validation and bounded data.

Run from an installed worker environment, with its src directory on PYTHONPATH.
The full-scan reference changes only accounting; neither path uses Kafka or an external provider.
"""

import argparse
import cProfile
import json
import pstats
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from ai_insights.config import Settings
from ai_insights.contracts import Contracts
from ai_insights.providers import FixtureProvider
from ai_insights.state import State, utc
from ai_insights.worker import Worker


class FixtureBus:
    """In-memory publication boundary; deliberately excludes Kafka network timing."""

    def __init__(self, state: State) -> None:
        self.state = state
        self.completed = 0
        self.touched: set[str] = set()

    def commit(
        self, changes: Mapping[str, dict[str, Any] | None], message: Any = None, event: Any = None
    ) -> None:
        self.state.apply(changes)
        self.touched.update(changes)
        if event is not None:
            self.completed += 1


class FixtureMessage:
    def __init__(self, event: dict[str, Any]) -> None:
        self.event = event

    def value(self) -> bytes:
        return json.dumps(self.event).encode()

    def key(self) -> bytes:
        return str(self.event["symbol"]).encode()


class FullScanState(State):
    """Reference accounting algorithm with the same current security validation."""

    def check(self, changes: Mapping[str, dict[str, Any] | None]) -> None:
        self.validate(changes)
        future = self.values | {key: value for key, value in changes.items() if value is not None}
        for key, value in changes.items():
            if value is None:
                future.pop(key, None)
        size = sum(
            len(key.encode()) + len(json.dumps(value).encode()) for key, value in future.items()
        )
        if len(future) > self.settings.state_keys or size > self.settings.state_bytes:
            raise ValueError("state_capacity")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--algorithm", choices=("incremental", "full-scan"), default="incremental")
    parser.add_argument("--operation", choices=("check", "restore", "fixture"), default="check")
    args = parser.parse_args()
    settings = Settings(contracts_dir=Path(__file__).resolve().parents[3] / "contracts/schemas")
    contracts = Contracts(settings.contracts_dir)
    raw = json.loads(
        (
            settings.contracts_dir.parents[1]
            / "tests/contract/fixtures/events/news-raw/valid/ingested.json"
        ).read_text()
    )
    state_type = State if args.algorithm == "incremental" else FullScanState
    for count in (100, 1000, 10000):
        state = state_type(settings, contracts)
        records = {}
        for index in range(count):
            article = str(uuid5(NAMESPACE_URL, str(index)))
            identity = str(uuid5(NAMESPACE_URL, article + ":" + "a" * 64))
            records["ARTICLE:" + identity] = state.record(
                "ARTICLE",
                identity,
                {
                    "status": "STARTED",
                    "articleId": article,
                    "processingIdentity": "a" * 64,
                    "reservedNanodollars": 0,
                    "budgetDay": "2026-10-04",
                },
            )
        state.apply(records)
        change = {next(iter(records)): next(iter(records.values()))}
        state.check(change)
        serialized = [(key, json.dumps(value)) for key, value in records.items()]

        def operation(
            state: State = state,
            change: dict[str, dict[str, Any]] = change,
            serialized: list[tuple[str, str]] = serialized,
            count: int = count,
        ) -> None:
            if args.operation == "check":
                state.check(change)
            elif args.operation == "restore":
                restored = state_type(settings, contracts)
                for key, value in serialized:
                    restored.apply({key: json.loads(value)})
                assert len(restored.values) == count
            else:
                bus = FixtureBus(state)
                worker = Worker(settings, contracts, state, bus, FixtureProvider())  # type: ignore[arg-type]
                article = str(uuid5(NAMESPACE_URL, f"fixture-benchmark:{len(state.values)}"))
                event = {
                    **raw,
                    "occurredAt": utc(),
                    "payload": {
                        **raw["payload"],
                        "articleId": article,
                        "contentHash": article.replace("-", "") * 2,
                    },
                }
                worker.handle(FixtureMessage(event))
                assert bus.completed == 1
                # Keep the initial key count identical between fixture samples.
                # This is an in-memory benchmark only, never a paid/live state store.
                state.apply(dict.fromkeys(bus.touched))

        iterations = 20 if args.operation in {"check", "fixture"} else 1
        operation()  # Warm up the same operation.
        for run in range(1, 4):
            started = time.perf_counter()
            for _ in range(iterations):
                operation()
            print(
                json.dumps(
                    {
                        "algorithm": args.algorithm,
                        "operation": args.operation,
                        "keys": count,
                        "run": run,
                        "iterations": iterations,
                        "millisecondsPerOperation": (time.perf_counter() - started)
                        * 1000
                        / iterations,
                    }
                ),
                flush=True,
            )
        if count == 10000:
            profile = cProfile.Profile()
            profile.runcall(operation)
            pstats.Stats(profile).sort_stats("cumulative").print_stats(8)


if __name__ == "__main__":
    main()
