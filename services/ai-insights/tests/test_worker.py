import json

import pytest

from ai_insights.providers import FixtureProvider
from ai_insights.state import State
from ai_insights.worker import Worker


class Message:
    def __init__(self, event):
        self.event = event

    def value(self):
        return json.dumps(self.event).encode()

    def key(self):
        return self.event["symbol"].encode()


class MemoryKafka:
    def __init__(self, state):
        self.state = state
        self.commits = []
        self.fail = None

    def commit(self, changes, message=None, event=None):
        if self.fail == "reservation" or (self.fail == "publish" and event is not None):
            raise RuntimeError("transaction")
        self.state.apply(changes)
        self.commits.append((changes, message, event))


def test_reservation_confirmation_offset_and_restart(settings, contracts, raw):
    state = State(settings, contracts)
    kafka = MemoryKafka(state)
    calls = []

    class Provider(FixtureProvider):
        def generate(self, data, deadline):
            assert kafka.commits[-1][1] is None
            assert any(v["data"].get("status") == "STARTED" for v in state.values.values())
            calls.append(1)
            raise RuntimeError("crash_before_or_after_http")

    worker = Worker(settings, contracts, state, kafka, Provider())
    message = Message(raw)
    kafka.fail = "reservation"
    with pytest.raises(RuntimeError):
        worker.handle(message)
    assert calls == [] and not state.values
    kafka.fail = None
    with pytest.raises(RuntimeError):
        worker.handle(message)
    assert calls == [1]
    worker.handle(message)
    assert calls == [1]
    assert any(v["data"].get("status") == "UNCERTAIN" for v in state.values.values())
    assert kafka.commits[-1][1] is message


def test_saved_result_republish_duplicate_and_context_cache(settings, contracts, raw):
    state = State(settings, contracts)
    kafka = MemoryKafka(state)
    calls = []

    class Provider(FixtureProvider):
        def generate(self, data, deadline):
            calls.append(1)
            return super().generate(data, deadline)

    worker = Worker(settings, contracts, state, kafka, Provider())
    message = Message(raw)
    kafka.fail = "publish"
    with pytest.raises(RuntimeError):
        worker.handle(message)
    saved = next(
        v["data"]["event"] for v in state.values.values() if v["data"].get("status") == "RESULT"
    )
    kafka.fail = None
    worker.handle(message)
    assert kafka.commits[-1][2] == saved
    worker.handle(message)
    assert calls == [1]
    raw["payload"]["articleId"] = "11111111-1111-4111-8111-111111111111"
    worker.handle(Message(raw))
    assert calls == [1]
    assert kafka.commits[-1][2]["payload"]["enrichment"]["insight"]["evidence"] == [
        raw["payload"]["articleId"]
    ]


def test_capacity_skips_before_provider_without_evicting_live_state(settings, contracts, raw):
    settings = settings.model_copy(update={"state_keys": 10})
    state = State(settings, contracts)
    for index in range(10):
        digest = f"{index:064x}"
        state.apply({"CONTENT:" + digest: state.record("CONTENT", digest, {"status": "UNCERTAIN"})})
    kafka = MemoryKafka(state)

    class NeverCall(FixtureProvider):
        def generate(self, data, deadline):
            raise AssertionError("provider must not run without state capacity")

    message = Message(raw)
    Worker(settings, contracts, state, kafka, NeverCall()).handle(message)
    assert len(state.values) == 10
    assert kafka.commits == [({}, message, None)]
