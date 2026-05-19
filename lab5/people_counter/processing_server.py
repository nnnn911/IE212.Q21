from __future__ import annotations

import argparse
import logging
import sys

from people_counter.broker.kafka_client import JsonKafkaConsumer, JsonKafkaProducer
from people_counter.broker.tcp_json import TcpJsonSender, serve_json_lines
from people_counter.config import load_settings
from people_counter.logging_config import configure_logging
from people_counter.models import FramePayload
from people_counter.processing.detector import PersonDetector

LOGGER = logging.getLogger(__name__)


def make_detector() -> PersonDetector:
    settings = load_settings()
    return PersonDetector(
        model_name=settings.detector_model_name,
        model_path=settings.opencv_model_path,
        config_path=settings.opencv_config_path,
        confidence_threshold=settings.confidence_threshold,
    )


def process_payload(detector: PersonDetector, payload: dict) -> dict:
    frame = FramePayload.from_dict(payload)
    result = detector.detect(frame)
    LOGGER.info("Processed frame %s: people_count=%s", frame.frame_id, result.people_count)
    return result.to_dict()


def run_kafka() -> None:
    settings = load_settings()
    detector = make_detector()
    consumer = JsonKafkaConsumer(
        settings.broker_url,
        settings.frames_topic,
        settings.consumer_group_processing,
        retry_delay=settings.reconnect_delay_seconds,
    )
    producer = JsonKafkaProducer(settings.broker_url, retry_delay=settings.reconnect_delay_seconds)
    LOGGER.info("Processing Server ready; waiting for frames on topic %s", settings.frames_topic)
    try:
        for payload in consumer.messages():
            try:
                producer.send(settings.detections_topic, process_payload(detector, payload))
            except Exception as exc:
                LOGGER.exception("Failed to process frame payload: %s", exc)
    finally:
        consumer.close()
        producer.close()


def run_tcp(args) -> None:
    settings = load_settings()
    detector = make_detector()
    sender = TcpJsonSender(args.storage_host, args.storage_port, reconnect_delay=settings.reconnect_delay_seconds)

    def handle(payload: dict) -> None:
        try:
            sender.send(process_payload(detector, payload))
        except Exception as exc:
            LOGGER.exception("Failed to process TCP payload: %s", exc)

    LOGGER.info("Processing Server ready; waiting for frames on TCP %s:%s", args.listen_host, args.listen_port)
    serve_json_lines(args.listen_host, args.listen_port, handle)


def build_parser() -> argparse.ArgumentParser:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Processing Server")
    parser.add_argument("--mode", choices=["kafka", "tcp"], default="kafka")
    parser.add_argument("--listen-host", default=settings.processing_tcp_host)
    parser.add_argument("--listen-port", type=int, default=settings.processing_tcp_port)
    parser.add_argument("--storage-host", default=settings.storage_tcp_host)
    parser.add_argument("--storage-port", type=int, default=settings.storage_tcp_port)
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args(argv)
    LOGGER.info("Starting Processing Server in %s mode", args.mode)
    try:
        if args.mode == "tcp":
            run_tcp(args)
        else:
            run_kafka()
    except KeyboardInterrupt:
        LOGGER.info("Processing Server stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
