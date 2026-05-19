import unittest
from unittest.mock import patch

from people_counter.processing.detector import PersonDetector


class FakeYoloModel:
    names = {0: "person", 1: "bicycle"}


class FakeUltralytics:
    def YOLO(self, model_name):
        self.model_name = model_name
        return FakeYoloModel()


class DetectorTests(unittest.TestCase):
    def test_detector_uses_only_ultralytics_yolo(self):
        original_import = __import__
        fake_ultralytics = FakeUltralytics()

        def fake_import(name, *args, **kwargs):
            if name == "ultralytics":
                return fake_ultralytics
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            detector = PersonDetector(model_name="yolo11n.pt")

        self.assertEqual(detector.active_model_name, "yolo11n.pt")

    def test_detector_fails_when_ultralytics_is_missing(self):
        original_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "ultralytics":
                raise ImportError("missing ultralytics")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaises(RuntimeError):
                PersonDetector(model_name="yolo11n.pt")


if __name__ == "__main__":
    unittest.main()
