from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


@dataclass
class Settings:
    broker_url: str
    frames_topic: str
    detections_topic: str
    consumer_group_processing: str
    consumer_group_storage: str
    database_url: str
    processing_tcp_host: str
    processing_tcp_port: int
    storage_tcp_host: str
    storage_tcp_port: int
    camera_id: str
    fps: float
    source_type: str
    source_path: str
    max_frames: int
    detector_model_name: str
    confidence_threshold: float
    reconnect_delay_seconds: float


def settings_from_env() -> Settings:
    return Settings(
        broker_url=_env("BROKER_URL", "localhost:9092"),
        frames_topic=_env("FRAMES_TOPIC", "camera.frames"),
        detections_topic=_env("DETECTIONS_TOPIC", "people.detections"),
        consumer_group_processing=_env("PROCESSING_GROUP_ID", "people-counter-processing"),
        consumer_group_storage=_env("STORAGE_GROUP_ID", "people-counter-storage"),
        database_url=_env(
            "DATABASE_URL",
            "postgresql://people_counter:people_counter@localhost:5432/people_counter",
        ),
        processing_tcp_host=_env("PROCESSING_TCP_HOST", "127.0.0.1"),
        processing_tcp_port=_env_int("PROCESSING_TCP_PORT", 6100),
        storage_tcp_host=_env("STORAGE_TCP_HOST", "127.0.0.1"),
        storage_tcp_port=_env_int("STORAGE_TCP_PORT", 6200),
        camera_id=_env("CAMERA_ID", "camera-001"),
        fps=_env_float("FPS", 1.0),
        source_type=_env("SOURCE_TYPE", "simulated"),
        source_path=_env("SOURCE_PATH", ""),
        max_frames=_env_int("MAX_FRAMES", 0),
        detector_model_name=_env("DETECTOR_MODEL_NAME", "yolo11n.pt"),
        confidence_threshold=_env_float("CONFIDENCE_THRESHOLD", 0.25),
        reconnect_delay_seconds=_env_float("RECONNECT_DELAY_SECONDS", 2.0),
    )


def load_settings() -> Settings:
    try:
        from dotenv import load_dotenv
    except ImportError:
        pass
    else:
        load_dotenv()
    return settings_from_env()
