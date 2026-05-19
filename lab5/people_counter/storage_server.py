from __future__ import annotations

import argparse
import logging
import sys

from people_counter.broker.kafka_client import JsonKafkaConsumer
from people_counter.broker.tcp_json import serve_json_lines
from people_counter.config import load_settings
from people_counter.logging_config import configure_logging
from people_counter.models import DetectionResult
from people_counter.storage.database import DetectionRepository

LOGGER = logging.getLogger(__name__)


def store_payload(repo: DetectionRepository, payload: dict) -> None:
    result = DetectionResult.from_dict(payload)
    repo.insert_detection(result)
    LOGGER.info("Stored detection for frame %s: people_count=%s", result.frame_id, result.people_count)


def run_kafka() -> None:
    settings = load_settings()
    repo = DetectionRepository(settings.database_url)
    repo.init_schema()
    consumer = JsonKafkaConsumer(
        settings.broker_url,
        settings.detections_topic,
        settings.consumer_group_storage,
        retry_delay=settings.reconnect_delay_seconds,
    )
    LOGGER.info("Storage Server ready; waiting for detection results on topic %s", settings.detections_topic)
    try:
        for payload in consumer.messages():
            try:
                store_payload(repo, payload)
            except Exception as exc:
                LOGGER.exception("Failed to store Kafka payload: %s", exc)
    finally:
        consumer.close()
        repo.close()


def run_tcp(args) -> None:
    settings = load_settings()
    repo = DetectionRepository(settings.database_url)
    repo.init_schema()
    LOGGER.info("Storage Server ready; waiting for detection results on TCP %s:%s", args.listen_host, args.listen_port)

    def handle(payload: dict) -> None:
        try:
            store_payload(repo, payload)
        except Exception as exc:
            LOGGER.exception("Failed to store TCP payload: %s", exc)

    serve_json_lines(args.listen_host, args.listen_port, handle)


def build_parser() -> argparse.ArgumentParser:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Storage Server")
    parser.add_argument("--mode", choices=["kafka", "tcp"], default="kafka")
    parser.add_argument("--listen-host", default=settings.storage_tcp_host)
    parser.add_argument("--listen-port", type=int, default=settings.storage_tcp_port)
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args(argv)
    LOGGER.info("Starting Storage Server in %s mode", args.mode)
    try:
        if args.mode == "tcp":
            run_tcp(args)
        else:
            run_kafka()
    except KeyboardInterrupt:
        LOGGER.info("Storage Server stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
