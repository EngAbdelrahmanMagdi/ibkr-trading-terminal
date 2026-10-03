"""Real Kafka transaction/restart proof; provider remains deterministic and local."""

import json
import os
import time
from copy import deepcopy
from uuid import uuid4

import docker
import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer

from ai_insights.config import Settings
from ai_insights.kafka import ENRICHED, RAW, STATE, Kafka
from ai_insights.providers import FixtureProvider
from ai_insights.state import State
from ai_insights.worker import Worker


def receive(consumer, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is not None and not message.error():
            return message
    raise AssertionError("Kafka record not received before deadline")


def test_transactions_restart_saved_result_and_duplicate(contracts, raw):
    client = docker.from_env()
    network = client.networks.create("ai-test-" + uuid4().hex)
    runner = client.containers.get(os.environ["HOSTNAME"])
    alias = "kafka-" + uuid4().hex
    container = (
        DockerContainer("apache/kafka:4.3.1")
        .with_name(alias)
        .with_kwargs(network=network.id, hostname=alias)
    )
    for key, value in {
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": f"1@{alias}:29093",
        "KAFKA_LISTENERS": "PLAINTEXT://:29092,CONTROLLER://:29093",
        "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{alias}:29092",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
    }.items():
        container.with_env(key, value)
    network.connect(runner)
    bus = None
    try:
        container.start()
        brokers = f"{alias}:29092"
        admin = AdminClient({"bootstrap.servers": brokers})
        deadline = time.monotonic() + 60
        while True:
            try:
                admin.list_topics(timeout=2)
                break
            except Exception:
                if time.monotonic() > deadline:
                    raise
        for future in admin.create_topics(
            [
                NewTopic(RAW, 1, 1),
                NewTopic(ENRICHED, 1, 1),
                NewTopic(STATE, 1, 1, config={"cleanup.policy": "compact"}),
            ]
        ).values():
            future.result(20)
        settings = Settings(
            brokers=brokers,
            contracts_dir=contracts_root(contracts),
            group="restart-test",
            transactional_id="restart-test",
        )
        state = State(settings, contracts)
        bus = Kafka(settings, state)
        bus.restore()
        producer = Producer({"bootstrap.servers": brokers})
        producer.produce(RAW, key=raw["symbol"], value=json.dumps(raw))
        assert producer.flush(10) == 0
        message = receive(bus.consumer)
        calls = []

        class Provider(FixtureProvider):
            def generate(self, data, deadline):
                calls.append(1)
                assert bus.consumer.committed([TopicPartition(RAW, 0)], timeout=5)[0].offset < 1
                observer = Kafka(
                    settings.model_copy(update={"transactional_id": "observer"}),
                    State(settings, contracts),
                )
                try:
                    observer.restore()
                    assert any(
                        v["data"].get("status") == "STARTED" for v in observer.state.values.values()
                    )
                finally:
                    observer.close()
                return super().generate(data, deadline)

        Worker(settings, contracts, state, bus, Provider()).handle(message)
        assert calls == [1]
        assert bus.consumer.committed([TopicPartition(RAW, 0)], timeout=5)[0].offset == 1
        bus.close()
        state = State(settings, contracts)
        bus = Kafka(settings, state)
        bus.restore()
        producer.produce(RAW, key=raw["symbol"], value=json.dumps(raw))
        producer.flush(10)
        Worker(settings, contracts, state, bus, Provider()).handle(receive(bus.consumer))
        assert calls == [1]
        reader = Consumer(
            {
                "bootstrap.servers": brokers,
                "group.id": "result-test",
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
            }
        )
        try:
            reader.subscribe([ENRICHED])
            event = json.loads(receive(reader).value())
            contracts.validate("events/news-enriched.schema.json", event)
            assert event["payload"]["enrichment"]["model"] == "synthetic-news.v1"
        finally:
            reader.close()

        # Crash after the durable reservation but before a result: restoration is conservative.
        interrupted = deepcopy(raw)
        interrupted["payload"]["articleId"] = str(uuid4())
        interrupted["payload"]["contentHash"] = "1" * 64
        producer.produce(RAW, key=raw["symbol"], value=json.dumps(interrupted))
        producer.flush(10)
        pending = receive(bus.consumer)

        class Interrupted(FixtureProvider):
            def generate(self, data, deadline):
                calls.append(2)
                raise RuntimeError("interrupted")

        with pytest.raises(RuntimeError):
            Worker(settings, contracts, state, bus, Interrupted()).handle(pending)
        assert bus.consumer.committed([TopicPartition(RAW, 0)], timeout=5)[0].offset == 2
        bus.close()
        state = State(settings, contracts)
        bus = Kafka(settings, state)
        bus.restore()
        Worker(settings, contracts, state, bus, Interrupted()).handle(receive(bus.consumer))
        assert calls == [1, 2]
        assert any(v["data"].get("status") == "UNCERTAIN" for v in state.values.values())
        assert bus.consumer.committed([TopicPartition(RAW, 0)], timeout=5)[0].offset == 3

        # A final-publication interruption reuses the saved, frozen result after restoration.
        saved_article = deepcopy(raw)
        saved_article["payload"]["articleId"] = str(uuid4())
        saved_article["payload"]["contentHash"] = "2" * 64
        producer.produce(RAW, key=raw["symbol"], value=json.dumps(saved_article))
        producer.flush(10)
        pending = receive(bus.consumer)
        original_commit = bus.commit

        def fail_final(changes, message=None, event=None):
            if event is not None:
                raise RuntimeError("publication interrupted")
            original_commit(changes, message, event)

        bus.commit = fail_final
        with pytest.raises(RuntimeError):
            Worker(settings, contracts, state, bus, FixtureProvider()).handle(pending)
        frozen = next(
            v["data"]["event"]
            for v in state.values.values()
            if v["kind"] == "ARTICLE" and v["data"].get("status") == "RESULT"
        )
        bus.close()
        state = State(settings, contracts)
        bus = Kafka(settings, state)
        bus.restore()
        Worker(settings, contracts, state, bus, Interrupted()).handle(receive(bus.consumer))
        assert calls == [1, 2]
        assert any(
            v["data"].get("event") == frozen and v["data"].get("status") == "DONE"
            for v in state.values.values()
        )
        assert bus.consumer.committed([TopicPartition(RAW, 0)], timeout=5)[0].offset == 4

        # A second singleton producer fences the old producer before it can reserve/infer.
        fenced = Kafka(settings, State(settings, contracts))
        try:
            fenced.restore()
            with pytest.raises((KafkaException, RuntimeError, SystemError)) as failure:
                key, value = next(iter(state.values.items()))
                bus.commit({key: value})
            cause = failure.value.__cause__ or failure.value
            assert isinstance(cause, KafkaException)
            assert cause.args[0].code() == KafkaError._FENCED
        finally:
            fenced.close()
    finally:
        if bus is not None:
            bus.close()
        container.stop()
        network.disconnect(runner)
        network.remove()
        client.close()


def contracts_root(contracts):
    # Settings only require a path here; Contracts is already constructed by the shared fixture.
    from pathlib import Path

    return Path("/work/contracts/schemas")
