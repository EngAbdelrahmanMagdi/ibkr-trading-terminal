"""Shutdown stops readiness/admission while allowing the current article to finish."""

import signal
from types import SimpleNamespace

from ai_insights import __main__ as entry
from ai_insights.config import Settings


def test_shutdown_does_not_admit_another_article(monkeypatch):
    handlers = {}
    observations = []
    health_instances = []

    class Health:
        def __init__(self, port):
            self.ready = False
            health_instances.append(self)

        def close(self):
            observations.append("health_closed")

    class Kafka:
        def __init__(self, settings, state):
            self.consumer = self
            self.state = state

        def restore(self):
            observations.append("restored")

        def healthy(self):
            return True

        def poll(self, timeout):
            observations.append("polled")
            return type("Message", (), {"error": lambda self: None})()

        def close(self):
            observations.append("kafka_closed")

    class Worker:
        def __init__(self, *args):
            pass

        def handle(self, message):
            observations.append("started")
            handlers[signal.SIGTERM](signal.SIGTERM, None)
            assert not health_instances[0].ready
            observations.append("finished")

    settings = Settings()
    monkeypatch.setattr(entry.Settings, "environment", lambda: settings)
    monkeypatch.setattr(entry, "Contracts", lambda root: object())
    monkeypatch.setattr(entry, "State", lambda *args: SimpleNamespace(expired=lambda: {}))
    monkeypatch.setattr(entry, "Health", Health)
    monkeypatch.setattr(entry, "Kafka", Kafka)
    monkeypatch.setattr(entry, "Worker", Worker)
    monkeypatch.setattr(
        entry.signal, "signal", lambda name, handler: handlers.__setitem__(name, handler)
    )
    entry.main()
    assert observations == [
        "restored",
        "polled",
        "started",
        "finished",
        "kafka_closed",
        "health_closed",
    ]
