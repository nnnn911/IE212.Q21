from __future__ import annotations

import logging
import os
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
        confidence_threshold: float = 0.5,
    ):
        self.model_name = model_name
        self.confidence_threshold = confidence_threshold
        self._net = None
        self._cv2 = None
        self._hog = None
        self._yolo = None
        self.active_model_name = "fallback-zero-detector"
        if self._should_try_yolo(model_name, model_path):
            self._load_yolo_detector(model_path or model_name)
        if self._yolo is None and model_path and os.path.exists(model_path):
            try:
                import cv2

                self._cv2 = cv2
                self._net = cv2.dnn.readNet(model_path, config_path) if config_path else cv2.dnn.readNet(model_path)
                self.active_model_name = model_name
                LOGGER.info("Loaded OpenCV DNN model from %s", model_path)
            except Exception as exc:
                LOGGER.warning("Could not load OpenCV DNN model; trying HOG detector: %s", exc)
                self._net = None
        if self._yolo is None and self._net is None:
            self._load_hog_detector()

    def _should_try_yolo(self, model_name: str, model_path: str) -> bool:
        candidate = model_path or model_name
        return candidate.endswith(".pt") or candidate.startswith("yolo")

    def _load_yolo_detector(self, model_ref: str) -> None:
        try:
            os.environ.setdefault("YOLO_CONFIG_DIR", "/private/tmp/ultralytics")
            from ultralytics import YOLO

            self._yolo = YOLO(model_ref)
            self.active_model_name = model_ref
            LOGGER.info("Using Ultralytics YOLO detector: %s", model_ref)
        except Exception as exc:
            LOGGER.warning("Ultralytics YOLO unavailable; trying OpenCV fallback: %s", exc)
            self._yolo = None

    def _load_hog_detector(self) -> None:
        try:
            import cv2

            self._cv2 = cv2
            self._hog = cv2.HOGDescriptor()
            self._hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())
            self.active_model_name = "opencv-hog-person-detector"
            LOGGER.info("Using OpenCV HOG person detector")
        except Exception as exc:
            LOGGER.warning("OpenCV HOG detector unavailable; using zero fallback: %s", exc)
            self._hog = None
            self.active_model_name = "fallback-zero-detector"

    def detect(self, frame: FramePayload) -> DetectionResult:
        started = time.perf_counter()
        boxes: list[BoundingBox] = []
        if self._yolo is not None:
            boxes = self._detect_with_yolo(frame)
        elif self._net is not None and self._cv2 is not None:
            boxes = self._detect_with_opencv_dnn(frame)
        elif self._hog is not None and self._cv2 is not None:
            boxes = self._detect_with_hog(frame)
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

    def _detect_with_opencv_dnn(self, frame: FramePayload) -> list[BoundingBox]:
        image = decode_jpg_base64(frame.image_data)
        height, width = image.shape[:2]
        blob = self._cv2.dnn.blobFromImage(image, scalefactor=1.0 / 127.5, size=(300, 300), mean=(127.5, 127.5, 127.5))
        self._net.setInput(blob)
        detections = self._net.forward()
        boxes: list[BoundingBox] = []
        for i in range(detections.shape[2]):
            confidence = float(detections[0, 0, i, 2])
            class_id = int(detections[0, 0, i, 1])
            if confidence < self.confidence_threshold or class_id != 15:
                continue
            x1, y1, x2, y2 = detections[0, 0, i, 3:7]
            boxes.append(
                BoundingBox(
                    x1=max(0, int(x1 * width)),
                    y1=max(0, int(y1 * height)),
                    x2=min(width, int(x2 * width)),
                    y2=min(height, int(y2 * height)),
                    confidence=confidence,
                    class_name="person",
                )
            )
        return boxes

    def _detect_with_hog(self, frame: FramePayload) -> list[BoundingBox]:
        image = decode_jpg_base64(frame.image_data)
        rectangles, weights = self._hog.detectMultiScale(
            image,
            winStride=(8, 8),
            padding=(16, 16),
            scale=1.05,
        )
        boxes: list[BoundingBox] = []
        for rect, weight in zip(rectangles, weights):
            confidence = max(0.0, min(1.0, float(weight)))
            if confidence < self.confidence_threshold:
                continue
            x, y, width, height = [int(value) for value in rect]
            boxes.append(
                BoundingBox(
                    x1=x,
                    y1=y,
                    x2=x + width,
                    y2=y + height,
                    confidence=confidence,
                    class_name="person",
                )
            )
        return boxes
