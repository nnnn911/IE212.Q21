from __future__ import annotations

import logging
import os
import tempfile
import time

from people_counter.models import BoundingBox, DetectionResult, FramePayload
from people_counter.processing.image_codec import decode_jpg_base64

LOGGER = logging.getLogger(__name__)


class PersonDetector:
    def __init__(
        self,
        model_name: str,
        model_path: str = "",
        config_path: str = "",
        confidence_threshold: float = 0.25,
    ):
        del config_path
        model_ref = model_path or model_name
        if not model_ref:
            raise ValueError("YOLO model name/path is required")

        os.environ.setdefault("YOLO_CONFIG_DIR", os.path.join(tempfile.gettempdir(), "people-counter-ultralytics"))
        try:
            from ultralytics import YOLO
        except ImportError as exc:
            raise RuntimeError("ultralytics is required. Run: pip install -r requirements.txt") from exc

        self.model_name = model_ref
        self.confidence_threshold = confidence_threshold
        self._yolo = YOLO(model_ref)
        self.active_model_name = model_ref
        LOGGER.info("Using Ultralytics YOLO detector: %s", model_ref)

    def detect(self, frame: FramePayload) -> DetectionResult:
        started = time.perf_counter()
        boxes = self._detect_with_yolo(frame)
        elapsed_ms = (time.perf_counter() - started) * 1000
        return DetectionResult(
            camera_id=frame.camera_id,
            frame_id=frame.frame_id,
            timestamp=frame.timestamp,
            people_count=len(boxes),
            bounding_boxes=boxes,
            processing_time_ms=elapsed_ms,
            model_name=self.active_model_name,
        )

    def _detect_with_yolo(self, frame: FramePayload) -> list[BoundingBox]:
        image = decode_jpg_base64(frame.image_data)
        results = self._yolo.predict(
            source=image,
            conf=self.confidence_threshold,
            classes=[0],
            verbose=False,
        )
        boxes: list[BoundingBox] = []
        for result in results:
            result_boxes = getattr(result, "boxes", None)
            if result_boxes is None:
                continue
            for yolo_box in result_boxes:
                class_id = int(yolo_box.cls[0])
                class_name = self._yolo.names.get(class_id, str(class_id))
                if class_name != "person":
                    continue
                x1, y1, x2, y2 = [int(value) for value in yolo_box.xyxy[0].tolist()]
                confidence = float(yolo_box.conf[0])
                boxes.append(
                    BoundingBox(
                        x1=max(0, x1),
                        y1=max(0, y1),
                        x2=max(x1 + 1, x2),
                        y2=max(y1 + 1, y2),
                        confidence=max(0.0, min(1.0, confidence)),
                        class_name="person",
                    )
                )
        return boxes
