from __future__ import annotations

import argparse
import csv
import sys

from people_counter.config import load_settings
from people_counter.logging_config import configure_logging
from people_counter.storage.database import DetectionRepository


def _camera_where(camera_id: str = "") -> str:
    if not camera_id:
        return ""
    escaped = camera_id.replace("'", "''")
    return f"WHERE camera_id = '{escaped}'"


def build_stats_query(group_by: str, camera_id: str = "") -> str:
    where_clause = _camera_where(camera_id)
    if group_by == "minute":
        bucket = "date_trunc('minute', timestamp)"
        return f"""
            SELECT camera_id, {bucket} AS bucket, AVG(people_count) AS avg_people,
                   MAX(people_count) AS max_people, COUNT(*) AS frames
            FROM people_detections
            {where_clause}
            GROUP BY camera_id, bucket
            ORDER BY bucket, camera_id
        """
    if group_by == "hour":
        bucket = "date_trunc('hour', timestamp)"
        return f"""
            SELECT camera_id, {bucket} AS bucket, AVG(people_count) AS avg_people,
                   MAX(people_count) AS max_people, COUNT(*) AS frames
            FROM people_detections
            {where_clause}
            GROUP BY camera_id, bucket
            ORDER BY bucket, camera_id
        """
    if group_by == "camera":
        return """
            SELECT camera_id, AVG(people_count) AS avg_people,
                   MAX(people_count) AS max_people, COUNT(*) AS frames
            FROM people_detections
            {where_clause}
            GROUP BY camera_id
            ORDER BY camera_id
        """.format(where_clause=where_clause)
    raise ValueError("group_by must be one of: minute, hour, camera")


def print_rows(rows, output_csv: str = "") -> None:
    if output_csv:
        with open(output_csv, "w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerows(rows)
        return
    for row in rows:
        print("\t".join(str(value) for value in row))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Query people-count statistics.")
    parser.add_argument("--group-by", choices=["minute", "hour", "camera"], default="minute")
    parser.add_argument("--camera-id", default="", help="Optional camera_id filter")
    parser.add_argument("--csv", default="", help="Optional CSV output path")
    args = parser.parse_args(argv)

    configure_logging()
    settings = load_settings()
    repo = DetectionRepository(settings.database_url)
    rows = repo.query(build_stats_query(args.group_by, camera_id=args.camera_id))
    print_rows(rows, args.csv)
    repo.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
