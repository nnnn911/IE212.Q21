from __future__ import annotations

import base64


def encode_image_bytes_base64(image_bytes: bytes) -> str:
    return base64.b64encode(image_bytes).decode("ascii")


def decode_image_bytes_base64(image_data: str) -> bytes:
    return base64.b64decode(image_data.encode("ascii"))


def encode_jpg_base64(image) -> str:
    try:
        import cv2
    except ImportError as exc:
        raise RuntimeError("OpenCV is required to encode image arrays") from exc

    success, buffer = cv2.imencode(".jpg", image)
    if not success:
        raise ValueError("Could not encode frame as JPEG")
    return encode_image_bytes_base64(buffer.tobytes())


def decode_jpg_base64(image_data: str):
    try:
        import cv2
        import numpy as np
    except ImportError as exc:
        raise RuntimeError("OpenCV and NumPy are required to decode JPEG frames") from exc

    raw = decode_image_bytes_base64(image_data)
    array = np.frombuffer(raw, dtype=np.uint8)
    image = cv2.imdecode(array, cv2.IMREAD_COLOR)
    if image is None:
        raise ValueError("Could not decode JPEG frame")
    return image
