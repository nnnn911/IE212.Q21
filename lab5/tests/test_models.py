from datetime import datetime, timezone
import unittest

from people_counter.models import BoundingBox, DetectionResult, FramePayload
from people_counter.camera.frame_sampler import RawFrame, sample_frames


class ModelTests(unittest.TestCase):
    def test_frame_payload_requires_jpg_base64_encoding(self):
        payload = FramePayload(
            camera_id="cam-1",
            frame_id="cam-1-000001",
            timestamp=datetime.now(timezone.utc),
            source_type="simulated",
            image_encoding="jpg_base64",
            image_data="abc",
            width=640,
            height=480,
        )

        self.assertEqual(payload.camera_id, "cam-1")
        self.assertEqual(payload.image_encoding, "jpg_base64")

    def test_frame_payload_rejects_invalid_dimensions(self):
        with self.assertRaises(ValueError):
            FramePayload(
                camera_id="cam-1",
                frame_id="cam-1-000001",
                timestamp=datetime.now(timezone.utc),
                source_type="simulated",
                image_encoding="jpg_base64",
                image_data="abc",
                width=0,
                height=480,
            )

    def test_detection_result_people_count_matches_person_boxes(self):
        result = DetectionResult(
            camera_id="cam-1",
            frame_id="cam-1-000001",
            timestamp=datetime.now(timezone.utc),
            people_count=1,
            bounding_boxes=[
                BoundingBox(
                    x1=1,
                    y1=2,
                    x2=100,
                    y2=200,
                    confidence=0.9,
                    class_name="person",
                )
            ],
            processing_time_ms=12.5,
            model_name="fallback-zero-detector",
        )

        self.assertEqual(result.people_count, 1)

    def test_detection_result_rejects_count_mismatch(self):
        with self.assertRaises(ValueError):
            DetectionResult(
                camera_id="cam-1",
                frame_id="cam-1-000001",
                timestamp=datetime.now(timezone.utc),
                people_count=2,
                bounding_boxes=[
                    BoundingBox(
                        x1=1,
                        y1=2,
                        x2=100,
                        y2=200,
                        confidence=0.9,
                        class_name="person",
                    )
                ],
                processing_time_ms=12.5,
                model_name="fallback-zero-detector",
            )

    def test_sample_frames_zero_max_frames_means_all_input_frames(self):
        raw_frames = [
            RawFrame("abc", 1, 1, "simulated"),
            RawFrame("abc", 1, 1, "simulated"),
            RawFrame("abc", 1, 1, "simulated"),
        ]

        frames = list(sample_frames(raw_frames, camera_id="cam-1", fps=0, max_frames=0))

        self.assertEqual(len(frames), 3)
        self.assertEqual(frames[-1].frame_id, "cam-1-000003")


if __name__ == "__main__":
    unittest.main()
