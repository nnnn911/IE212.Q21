from __future__ import annotations

import itertools
import os
from collections.abc import Iterator

from people_counter.camera.frame_sampler import RawFrame
from people_counter.processing.image_codec import encode_image_bytes_base64, encode_jpg_base64

TINY_JPEG_BASE64 = (
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAP//////////////////////////////////////////////////////////////////////////////////////"
    "2wBDAf//////////////////////////////////////////////////////////////////////////////////////wAARCAABAAEDASIAAhEBAxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAX/"
    "xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIQAxAAAAH/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oACAEBAAEFAqf/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oACAEDAQE/ASP/"
    "xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oACAECAQE/ASP/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oACAEBAAY/Al//xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oACAEBAAE/IV//2gAMAwEAAgADAAAAEP/"
    "xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oACAEDAQE/ESP/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oACAECAQE/ESP/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/9oACAEBAAE/EF//2Q=="
)


def simulated_frames() -> Iterator[RawFrame]:
    image_data = TINY_JPEG_BASE64
    width = 1
    height = 1
    try:
        import cv2
        import numpy as np

        image = np.full((240, 320, 3), 245, dtype=np.uint8)
        cv2.rectangle(image, (130, 50), (190, 210), (40, 40, 40), 2)
        cv2.circle(image, (160, 35), 18, (40, 40, 40), 2)
        image_data = encode_jpg_base64(image)
        height, width = image.shape[:2]
    except Exception:
        pass

    for _ in itertools.count():
        yield RawFrame(
            image_data=image_data,
            width=width,
            height=height,
            source_type="simulated",
        )


def directory_frames(path: str) -> Iterator[RawFrame]:
    for name in sorted(os.listdir(path)):
        if not name.lower().endswith((".jpg", ".jpeg", ".png")):
            continue
        full_path = os.path.join(path, name)
        with open(full_path, "rb") as file:
            yield RawFrame(
                image_data=encode_image_bytes_base64(file.read()),
                width=1,
                height=1,
                source_type="directory",
            )


def opencv_capture_frames(source, source_type: str, target_fps: float = 0) -> Iterator[RawFrame]:
    try:
        import cv2
    except ImportError as exc:
        raise RuntimeError("OpenCV is required for webcam and video sources") from exc

    capture = cv2.VideoCapture(source)
    if not capture.isOpened():
        raise RuntimeError(f"Could not open source: {source}")
    native_fps = capture.get(cv2.CAP_PROP_FPS) or 0
    frame_interval = 1
    if source_type == "video" and target_fps > 0 and native_fps > target_fps:
        frame_interval = max(1, round(native_fps / target_fps))
    try:
        frame_number = 0
        while True:
            ok, frame = capture.read()
            if not ok:
                break
            frame_number += 1
            if (frame_number - 1) % frame_interval != 0:
                continue
            height, width = frame.shape[:2]
            yield RawFrame(
                image_data=encode_jpg_base64(frame),
                width=width,
                height=height,
                source_type=source_type,
            )
    finally:
        capture.release()


def make_frame_source(source_type: str, source_path: str = "", target_fps: float = 0) -> Iterator[RawFrame]:
    if source_type == "simulated":
        return simulated_frames()
    if source_type == "directory":
        return directory_frames(source_path)
    if source_type == "webcam":
        return opencv_capture_frames(int(source_path or "0"), "webcam", target_fps=target_fps)
    if source_type == "video":
        return opencv_capture_frames(source_path, "video", target_fps=target_fps)
    raise ValueError(f"Unsupported source_type: {source_type}")
