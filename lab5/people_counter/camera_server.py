from __future__ import annotations

import argparse
import logging
import sys

from people_counter.broker.kafka_client import JsonKafkaProducer
from people_counter.broker.tcp_json import TcpJsonSender
from people_counter.camera.frame_sampler import sample_frames
from people_counter.camera.sources import make_frame_source
from people_counter.config import load_settings
from people_counter.logging_config import configure_logging

LOGGER = logging.getLogger(__name__)


def run_kafka(args) -> None:
    settings = load_settings()
    producer = JsonKafkaProducer(settings.broker_url, retry_delay=settings.reconnect_delay_seconds)
    frames = sample_frames(
        make_frame_source(args.source_type, args.source_path, target_fps=args.fps),
        camera_id=args.camera_id,
        fps=args.fps,
        max_frames=args.max_frames,
    )
    try:
        for frame in frames:
            producer.send(settings.frames_topic, frame.to_dict())
            LOGGER.info("Published frame %s to topic %s", frame.frame_id, settings.frames_topic)
        LOGGER.info("Camera Ingestion Server finished sending frames")
    finally:
        producer.close()


def run_tcp(args) -> None:
    settings = load_settings()
    sender = TcpJsonSender(args.host, args.port, reconnect_delay=settings.reconnect_delay_seconds)
    frames = sample_frames(
        make_frame_source(args.source_type, args.source_path, target_fps=args.fps),
        camera_id=args.camera_id,
        fps=args.fps,
        max_frames=args.max_frames,
    )
    try:
        for frame in frames:
            sender.send(frame.to_dict())
            LOGGER.info("Sent frame %s to processing TCP server", frame.frame_id)
        LOGGER.info("Camera Ingestion Server finished sending frames")
    finally:
        sender.close()


def build_parser() -> argparse.ArgumentParser:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Camera Ingestion Server")
    parser.add_argument("--mode", choices=["kafka", "tcp"], default="kafka")
    parser.add_argument("--camera-id", default=settings.camera_id)
    parser.add_argument("--fps", type=float, default=settings.fps)
    parser.add_argument("--source-type", choices=["simulated", "webcam", "video", "directory"], default=settings.source_type)
    parser.add_argument("--source-path", default=settings.source_path)
    parser.add_argument(
        "--max-frames",
        type=int,
        default=settings.max_frames,
        help="0 means unlimited; video sources run until end of file",
    )
    parser.add_argument("--host", default=settings.processing_tcp_host)
    parser.add_argument("--port", type=int, default=settings.processing_tcp_port)
    return parser


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args(argv)
    LOGGER.info("Starting Camera Ingestion Server in %s mode", args.mode)
    try:
        if args.mode == "tcp":
            run_tcp(args)
        else:
            run_kafka(args)
    except KeyboardInterrupt:
        LOGGER.info("Camera Ingestion Server stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
