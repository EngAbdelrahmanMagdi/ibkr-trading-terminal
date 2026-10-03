import json
import signal
import threading
import time

from . import metrics
from .config import Settings
from .contracts import Contracts
from .health import Health
from .kafka import Kafka
from .providers import FixtureProvider, OpenAIProvider, Provider
from .state import State
from .worker import Worker


def main() -> None:
    settings = Settings.environment()
    contracts = Contracts(settings.contracts_dir)
    provider: Provider = (
        FixtureProvider() if settings.provider == "FIXTURE" else OpenAIProvider(settings, contracts)
    )
    health = Health(settings.health_port)
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    kafka = Kafka(settings, State(settings, contracts))
    try:
        kafka.restore()
        worker = Worker(settings, contracts, kafka.state, kafka, provider)
        health.ready = True
        metrics.READY.set(1)
        cleanup_at = time.monotonic()
        while not stopped.is_set():
            if time.monotonic() >= cleanup_at:
                expired = dict(list(kafka.state.expired().items())[:100])
                if expired:
                    kafka.commit(expired)
                cleanup_at = time.monotonic() + 3600
            message = kafka.consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("consumer")
            worker.handle(message)
    except Exception as error:
        # Transaction uncertainty is fatal: restart restores committed state before any new request.
        print(json.dumps({"service": "ai-insights", "category": type(error).__name__}), flush=True)
        raise SystemExit(1) from None
    finally:
        health.ready = False
        metrics.READY.set(0)
        kafka.close()
        health.close()
        if isinstance(provider, OpenAIProvider):
            provider.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(json.dumps({"service": "ai-insights", "category": type(error).__name__}), flush=True)
        raise SystemExit(1) from None
