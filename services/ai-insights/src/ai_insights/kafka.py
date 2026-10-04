"""Kafka transactions cover operational state, publication and raw offsets only."""

import json
import time
from collections.abc import Mapping
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

from .config import Settings
from .state import State

RAW = "news.raw.v1"
ENRICHED = "news.enriched.v1"
STATE = "news.ai-state.v1"


class Kafka:
    def __init__(self, settings: Settings, state: State) -> None:
        self.settings = settings
        self.state = state
        self.producer: Any = Producer(
            {
                "bootstrap.servers": settings.brokers,
                **self.tls_config(),
                "transactional.id": settings.transactional_id,
                "enable.idempotence": True,
                "transaction.timeout.ms": 60000,
                "delivery.timeout.ms": 15000,
                "request.timeout.ms": 5000,
                "socket.timeout.ms": 10000,
                "queue.buffering.max.kbytes": 16384,
                "queue.buffering.max.messages": 100,
            }
        )
        self.consumer: Any = Consumer(
            {
                "bootstrap.servers": settings.brokers,
                **self.tls_config(),
                "group.id": settings.group,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "earliest",
                "isolation.level": "read_committed",
                "max.poll.interval.ms": 300000,
                "socket.timeout.ms": 10000,
                "queued.max.messages.kbytes": 16384,
                "fetch.message.max.bytes": 1048576,
            }
        )

    def tls_config(self) -> dict[str, str]:
        files = [self.settings.tls_ca_file, self.settings.tls_cert_file, self.settings.tls_key_file]
        if not any(files):
            return {}
        if not all(path and path.is_file() for path in files):
            raise ValueError("kafka_tls_identity")
        return {
            "security.protocol": "SSL",
            "ssl.ca.location": str(files[0]),
            "ssl.certificate.location": str(files[1]),
            "ssl.key.location": str(files[2]),
            "ssl.endpoint.identification.algorithm": "https",
            "enable.ssl.certificate.verification": "true",
        }

    def restore(self) -> None:
        self.producer.init_transactions(30)
        reader: Any = Consumer(
            {
                "bootstrap.servers": self.settings.brokers,
                **self.tls_config(),
                "group.id": self.settings.group + "-restore",
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
                "enable.partition.eof": True,
                "queued.max.messages.kbytes": 16384,
            }
        )
        deadline = time.monotonic() + self.settings.restore_seconds
        try:
            metadata = reader.list_topics(STATE, timeout=10)
            topic = metadata.topics.get(STATE)
            if topic is None or topic.error or len(topic.partitions) != 1:
                raise ValueError("state_topic")
            partition = TopicPartition(STATE, 0)
            low, high = reader.get_watermark_offsets(partition, timeout=10)
            reader.assign([TopicPartition(STATE, 0, low)])
            scanned = 0
            position = low
            while position < high:
                if time.monotonic() >= deadline:
                    raise TimeoutError("state_restore")
                message = reader.poll(0.5)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        position = message.offset()
                        continue
                    raise RuntimeError("state_restore")
                position = message.offset() + 1
                value = message.value()
                scanned += len(value or b"")
                if scanned > self.settings.restore_bytes:
                    raise ValueError("restore_capacity")
                raw_key = message.key()
                if not raw_key or len(raw_key) > 256 or len(value or b"") > 131072:
                    raise ValueError("state_record_bounds")
                key = raw_key.decode()
                self.state.apply({key: json.loads(value) if value is not None else None})
        finally:
            reader.close()
        self.consumer.subscribe([RAW])

    def commit(
        self,
        changes: Mapping[str, dict[str, Any] | None],
        message: Any = None,
        event: dict[str, Any] | None = None,
    ) -> None:
        self.state.check(changes)
        self.producer.begin_transaction()
        errors: list[Any] = []

        def delivered(error: Any, _: Any) -> None:
            if error is not None:
                errors.append(error)

        try:
            for key, value in changes.items():
                if value is not None:
                    self.state.contracts.validate("events/news-ai-state.schema.json", value)
                self.producer.produce(
                    STATE,
                    key=key,
                    value=json.dumps(value).encode() if value is not None else None,
                    on_delivery=delivered,
                )
            if event is not None:
                self.state.contracts.validate("events/news-enriched.schema.json", event)
                self.producer.produce(
                    ENRICHED,
                    key=event["symbol"],
                    value=json.dumps(event).encode(),
                    headers=[
                        (key, str(event[key]).encode())
                        for key in ("correlationId", "eventType", "eventVersion")
                    ],
                    on_delivery=delivered,
                )
            if self.producer.flush(15) or errors:
                raise RuntimeError("publication")
            if message is not None:
                self.producer.send_offsets_to_transaction(
                    [TopicPartition(message.topic(), message.partition(), message.offset() + 1)],
                    self.consumer.consumer_group_metadata(),
                    10,
                )
            self.producer.commit_transaction(15)
        except BaseException:
            try:
                self.producer.abort_transaction(10)
            except Exception:
                pass  # Original transaction outcome remains fatal; never infer after uncertainty.
            raise
        self.state.apply(changes)

    def close(self) -> None:
        self.consumer.close()
        self.producer = None  # Release librdkafka threads after completed/aborted transactions.

    def healthy(self) -> bool:
        try:
            metadata = self.producer.list_topics(STATE, timeout=2)
            return STATE in metadata.topics and not metadata.topics[STATE].error
        except Exception:
            return False
