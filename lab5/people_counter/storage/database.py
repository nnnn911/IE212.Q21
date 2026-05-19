from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Iterable

from people_counter.models import DetectionResult

LOGGER = logging.getLogger(__name__)
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


class DetectionRepository:
    def __init__(self, database_url: str, retries: int = 10, retry_delay: float = 2.0):
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("psycopg is required for PostgreSQL storage") from exc

        self._psycopg = psycopg
        self._conn = None
        for attempt in range(1, retries + 1):
            try:
                self._conn = psycopg.connect(database_url)
                self._conn.autocommit = True
                return
            except Exception as exc:
                LOGGER.warning("Database connection attempt %s failed: %s", attempt, exc)
                time.sleep(retry_delay)
        raise RuntimeError("Could not connect to PostgreSQL")

    def init_schema(self) -> None:
        with open(SCHEMA_PATH, "r", encoding="utf-8") as file:
            schema = file.read()
        with self._conn.cursor() as cursor:
            cursor.execute(schema)

    def insert_detection(self, result: DetectionResult) -> None:
        payload = result.to_dict()
        with self._conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO people_detections (
                    camera_id, frame_id, timestamp, people_count, bounding_boxes,
                    processing_time_ms, model_name, created_at
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s)
                ON CONFLICT (camera_id, frame_id) DO NOTHING
                """,
                (
                    payload["camera_id"],
                    payload["frame_id"],
                    payload["timestamp"],
                    payload["people_count"],
                    json.dumps(payload["bounding_boxes"]),
                    payload["processing_time_ms"],
                    payload["model_name"],
                    payload["created_at"],
                ),
            )

    def query(self, sql: str) -> Iterable[tuple]:
        with self._conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
