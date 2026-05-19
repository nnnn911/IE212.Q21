from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from typing import Any

LOGGER = logging.getLogger(__name__)


class JsonKafkaProducer:
    def __init__(self, bootstrap_servers: str, retries: int = 5, retry_delay: float = 1.0):
        from kafka import KafkaProducer

        self._retry_delay = retry_delay
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            retries=retries,
            linger_ms=10,
        )

    def send(self, topic: str, payload: dict[str, Any]) -> None:
        while True:
            try:
                self._producer.send(topic, payload).get(timeout=10)
                return
            except Exception as exc:
                LOGGER.warning("Kafka send failed; retrying: %s", exc)
                time.sleep(self._retry_delay)

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()


class JsonKafkaConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        retry_delay: float = 2.0,
    ):
        from kafka import KafkaConsumer

        self._retry_delay = retry_delay
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )

    def messages(self) -> Iterator[dict[str, Any]]:
        while True:
            try:
                for message in self._consumer:
                    if isinstance(message.value, dict):
                        yield message.value
                    else:
                        LOGGER.warning("Skipping non-object Kafka payload: %r", message.value)
            except Exception as exc:
                LOGGER.warning("Kafka consume failed; reconnecting: %s", exc)
                time.sleep(self._retry_delay)

    def close(self) -> None:
        self._consumer.close()
