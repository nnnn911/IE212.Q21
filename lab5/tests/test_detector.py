from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from people_counter.models import FramePayload
from people_counter.processing.detector import PersonDetector


class FakeHog:
    def setSVMDetector(self, detector):
        self.detector = detector


class FakeCv2:
    def HOGDescriptor(self):
        return FakeHog()

    def HOGDescriptor_getDefaultPeopleDetector(self):
        return "default-people-detector"


class FakeYoloModel:
    names = {0: "person", 1: "bicycle"}


class FakeUltralytics:
    def YOLO(self, model_name):
        self.model_name = model_name
        return FakeYoloModel()


class DetectorTests(unittest.TestCase):
    def test_detector_falls_back_when_opencv_is_missing(self):
        frame = FramePayload(
            camera_id="cam-1",
            frame_id="cam-1-000001",
            timestamp=datetime.now(timezone.utc),
            source_type="simulated",
            image_encoding="jpg_base64",
            image_data="abc",
            width=1,
            height=1,
        )

        original_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "cv2":
                raise ImportError("cv2 unavailable")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            detector = PersonDetector(model_name="auto")
            result = detector.detect(frame)

        self.assertEqual(result.people_count, 0)
        self.assertEqual(result.bounding_boxes, [])
        self.assertEqual(result.model_name, "fallback-zero-detector")

    def test_detector_uses_hog_when_no_dnn_model_is_configured(self):
        original_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "cv2":
                return FakeCv2()
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            detector = PersonDetector(model_name="auto")

        self.assertEqual(detector.active_model_name, "opencv-hog-person-detector")

    def test_detector_uses_ultralytics_yolo_when_model_name_starts_with_yolo(self):
        original_import = __import__
        fake_ultralytics = FakeUltralytics()

        def fake_import(name, *args, **kwargs):
            if name == "ultralytics":
                return fake_ultralytics
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            detector = PersonDetector(model_name="yolo11n.pt")

        self.assertEqual(detector.active_model_name, "yolo11n.pt")


if __name__ == "__main__":
    unittest.main()
