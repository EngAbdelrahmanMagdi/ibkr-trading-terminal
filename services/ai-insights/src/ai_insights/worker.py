import json
import random
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from jsonschema import ValidationError

from . import metrics
from .config import Settings
from .contracts import Contracts
from .kafka import Kafka
from .policy import Policy
from .prompt import VERSION, article_input, identity
from .providers import Provider, ProviderError
from .state import State, utc
from .validation import validate_output


class Worker:
    def __init__(
        self,
        settings: Settings,
        contracts: Contracts,
        state: State,
        kafka: Kafka,
        provider: Provider,
    ) -> None:
        self.settings, self.contracts, self.state = settings, contracts, state
        self.kafka, self.provider = kafka, provider
        self.policy = Policy(settings)

    def handle(self, message: Any) -> None:
        with metrics.SECONDS.time():
            self.process(message)

    def process(self, message: Any) -> None:
        try:
            value = message.value()
            if not value or len(value) > 1048576:
                raise ValueError("record_size")
            event = json.loads(value)
            if event.get("eventType") != "NEWS_ARTICLE_INGESTED" or event.get("eventVersion") != 1:
                self.skip(message, "unknown_event")
                return
            # Consumer compatibility: ignore future fields, then validate the known contract.
            event = {
                k: v
                for k, v in event.items()
                if k
                in {
                    "eventId",
                    "eventType",
                    "eventVersion",
                    "occurredAt",
                    "source",
                    "correlationId",
                    "symbol",
                    "payload",
                }
            }
            event["payload"] = {
                k: v
                for k, v in event["payload"].items()
                if k
                in {
                    "articleId",
                    "providerId",
                    "symbols",
                    "headline",
                    "source",
                    "url",
                    "publishedAt",
                    "rawSummary",
                    "contentHash",
                }
            }
            self.contracts.validate("events/news-raw.schema.json", event)
            if (
                message.key().decode() != event["symbol"]
                or event["symbol"] not in event["payload"]["symbols"]
            ):
                raise ValueError("key")
            if datetime.fromisoformat(event["occurredAt"]) < datetime.now(UTC) - timedelta(days=7):
                self.skip(message, "expired")
                return
            data = article_input(event, self.settings.input_bytes)
        except ValueError, KeyError, TypeError, AttributeError, UnicodeError, ValidationError:
            self.skip(message, "input_invalid")
            return
        digest = identity(event, self.provider.model, self.provider.revision)
        content_key = "CONTENT:" + digest
        article_id = event["payload"]["articleId"]
        article_key = "ARTICLE:" + str(uuid5(NAMESPACE_URL, article_id + ":" + digest))
        previous = self.state.get(article_key)
        if previous and previous["status"] in {"DONE", "SKIPPED", "UNCERTAIN"}:
            self.skip(message, "duplicate")
            return
        if previous and previous["status"] == "STARTED":
            uncertain = {**previous, "status": "UNCERTAIN"}
            self.kafka.commit(
                {
                    article_key: self.state.record("ARTICLE", article_key[8:], uncertain),
                    content_key: self.state.record("CONTENT", digest, {"status": "UNCERTAIN"}),
                },
                message,
            )
            metrics.UNCERTAIN.inc()
            return
        if previous and previous["status"] == "RESULT":
            self.finish(message, article_key, previous)
            return
        cached = self.state.get(content_key)
        if cached and cached["status"] != "RESULT":
            self.skip(message, "uncertain_content")
            return
        today = datetime.now(UTC).date().isoformat()
        record: dict[str, Any] = {
            "status": "STARTED",
            "articleId": article_id,
            "processingIdentity": digest,
            "reservedNanodollars": 0,
            "budgetDay": today,
        }
        deadline = time.monotonic() + self.settings.deadline
        if cached:
            output = {**cached["insight"], "evidence": [article_id]}
            metrics.CACHE.inc()
        else:
            try:
                if self.settings.provider != "FIXTURE":
                    self.policy.admit()
            except ValueError:
                self.skip(message, "admission")
                return
            output = None
            for attempt in range(self.settings.attempts):
                reservation = self.policy.reservation()
                budget_key = "BUDGET:" + today
                spent = (self.state.get(budget_key) or {}).get("reservedNanodollars", 0)
                if spent + reservation > int(self.settings.daily_budget * DecimalNanodollars):
                    self.skip(message, "budget")
                    return
                record["reservedNanodollars"] += reservation
                changes = {article_key: self.state.record("ARTICLE", article_key[8:], record)}
                changes[content_key] = self.state.record("CONTENT", digest, {"status": "STARTED"})
                if reservation:
                    changes[budget_key] = self.state.record(
                        "BUDGET", today, {"reservedNanodollars": spent + reservation}
                    )
                try:
                    self.state.check(changes)
                except ValueError:
                    self.skip(message, "state_capacity")
                    return
                # No raw message/offset here. commit() must return success before provider HTTP.
                self.kafka.commit(changes)
                metrics.COST.set((spent + reservation) / 1000000000)
                try:
                    output = self.provider.generate(data, deadline)
                    self.policy.failures = 0
                    break
                except ProviderError as error:
                    self.policy.failed(error.category)
                    wait = (
                        error.retry_after
                        if error.category == "rate"
                        else random.uniform(0, min(2, 0.25 * 2**attempt))
                    )
                    if (
                        error.category not in {"transient", "timeout", "rate"}
                        or wait is None
                        or attempt + 1 == self.settings.attempts
                        or time.monotonic() + wait >= deadline
                    ):
                        break
                    # Retry rate must also fit the configured token bucket.
                    wait = max(wait, self.policy.next_request - time.monotonic())
                    if time.monotonic() + wait >= deadline:
                        break
                    time.sleep(wait)
                    self.policy.admit()
                except ValueError, TypeError, KeyError:
                    break
            if output is None:
                record["status"] = "SKIPPED"
                self.kafka.commit(
                    {article_key: self.state.record("ARTICLE", article_key[8:], record)}, message
                )
                metrics.FAILURES.labels("provider").inc()
                return
        try:
            insight = validate_output(
                output, event, self.contracts, self.settings.confidence_threshold
            )
        except ValueError, TypeError, ValidationError:
            record["status"] = "SKIPPED"
            self.kafka.commit(
                {article_key: self.state.record("ARTICLE", article_key[8:], record)}, message
            )
            metrics.FAILURES.labels("validation").inc()
            return
        now = utc()
        enriched = {
            "eventId": article_key[8:],
            "eventType": "NEWS_ARTICLE_ENRICHED",
            "eventVersion": 1,
            "occurredAt": now,
            "source": "ai-insights",
            "correlationId": event["correlationId"],
            "symbol": event["symbol"],
            "payload": {
                "articleId": article_id,
                "contentHash": event["payload"]["contentHash"],
                "enrichment": {
                    "promptVersion": VERSION,
                    "model": self.provider.model,
                    "modelVersion": self.provider.revision,
                    "enrichedAt": now,
                    "insight": insight,
                },
            },
        }
        self.contracts.validate("events/news-enriched.schema.json", enriched)
        record = {**record, "status": "RESULT", "event": enriched}
        self.kafka.commit(
            {
                article_key: self.state.record("ARTICLE", article_key[8:], record),
                content_key: self.state.record(
                    "CONTENT", digest, {"status": "RESULT", "insight": insight}
                ),
            }
        )
        self.finish(message, article_key, record)

    def finish(self, message: Any, key: str, record: dict[str, Any]) -> None:
        self.kafka.commit(
            {key: self.state.record("ARTICLE", key[8:], {**record, "status": "DONE"})},
            message,
            record["event"],
        )
        metrics.PROCESSED.labels("enriched").inc()

    def skip(self, message: Any, category: str) -> None:
        self.kafka.commit({}, message)
        metrics.PROCESSED.labels(category).inc()


DecimalNanodollars = Decimal(1000000000)
