from __future__ import annotations

import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone

from people_counter.models import FramePayload


@dataclass
class RawFrame:
    image_data: str
    width: int
    height: int
    source_type: str


def sample_frames(
    frames: Iterable[RawFrame],
    camera_id: str,
    fps: float,
    max_frames: int = 0,
) -> Iterator[FramePayload]:
    delay = 1.0 / fps if fps > 0 else 0
    for index, frame in enumerate(frames, start=1):
        if max_frames and index > max_frames:
            break
        started = time.monotonic()
        yield FramePayload(
            camera_id=camera_id,
            frame_id=f"{camera_id}-{index:06d}",
            timestamp=datetime.now(timezone.utc),
            source_type=frame.source_type,
            image_encoding="jpg_base64",
            image_data=frame.image_data,
            width=frame.width,
            height=frame.height,
        )
        elapsed = time.monotonic() - started
        if delay > elapsed:
            time.sleep(delay - elapsed)
