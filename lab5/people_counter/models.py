from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _parse_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class BoundingBox:
    x1: int
    y1: int
    x2: int
    y2: int
    confidence: float
    class_name: str = "person"

    def __post_init__(self) -> None:
        if self.class_name != "person":
            raise ValueError("Only class_name='person' is supported")
        if self.x2 <= self.x1 or self.y2 <= self.y1:
            raise ValueError("Bounding box must have x2 > x1 and y2 > y1")
        if not 0.0 <= float(self.confidence) <= 1.0:
            raise ValueError("confidence must be between 0 and 1")

    def to_dict(self) -> dict[str, Any]:
        return {
            "x1": int(self.x1),
            "y1": int(self.y1),
            "x2": int(self.x2),
            "y2": int(self.y2),
            "confidence": float(self.confidence),
            "class_name": self.class_name,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BoundingBox":
        return cls(
            x1=int(data["x1"]),
            y1=int(data["y1"]),
            x2=int(data["x2"]),
            y2=int(data["y2"]),
            confidence=float(data["confidence"]),
            class_name=str(data.get("class_name", "person")),
        )


@dataclass
class FramePayload:
    camera_id: str
    frame_id: str
    timestamp: datetime | str
    source_type: str
    image_encoding: str
    image_data: str
    width: int
    height: int

    def __post_init__(self) -> None:
        self.timestamp = _parse_datetime(self.timestamp)
        if not self.camera_id:
            raise ValueError("camera_id is required")
        if not self.frame_id:
            raise ValueError("frame_id is required")
        if self.image_encoding != "jpg_base64":
            raise ValueError("image_encoding must be 'jpg_base64'")
        if not self.image_data:
            raise ValueError("image_data is required")
        if int(self.width) <= 0 or int(self.height) <= 0:
            raise ValueError("width and height must be positive")

    def to_dict(self) -> dict[str, Any]:
        return {
            "camera_id": self.camera_id,
            "frame_id": self.frame_id,
            "timestamp": self.timestamp.isoformat(),
            "source_type": self.source_type,
            "image_encoding": self.image_encoding,
            "image_data": self.image_data,
            "width": int(self.width),
            "height": int(self.height),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FramePayload":
        return cls(**data)


@dataclass
class DetectionResult:
    camera_id: str
    frame_id: str
    timestamp: datetime | str
    people_count: int
    bounding_boxes: list[BoundingBox | dict[str, Any]]
    processing_time_ms: float
    model_name: str
    created_at: datetime | str = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        self.timestamp = _parse_datetime(self.timestamp)
        self.created_at = _parse_datetime(self.created_at)
        self.bounding_boxes = [
            box if isinstance(box, BoundingBox) else BoundingBox.from_dict(box)
            for box in self.bounding_boxes
        ]
        if int(self.people_count) != len(self.bounding_boxes):
            raise ValueError("people_count must match number of person bounding boxes")
        if float(self.processing_time_ms) < 0:
            raise ValueError("processing_time_ms must be non-negative")
        if not self.model_name:
            raise ValueError("model_name is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "camera_id": self.camera_id,
            "frame_id": self.frame_id,
            "timestamp": self.timestamp.isoformat(),
            "people_count": int(self.people_count),
            "bounding_boxes": [box.to_dict() for box in self.bounding_boxes],
            "processing_time_ms": float(self.processing_time_ms),
            "model_name": self.model_name,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DetectionResult":
        return cls(**data)
