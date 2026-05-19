# People Counter Distributed System Design

## Goal

Build a local, free/open-source distributed people-counting system for a lab assignment. The system has three independent Python servers: Camera Ingestion, Processing, and Storage. It demonstrates stream processing and big-data architecture through a broker-backed pipeline while keeping a TCP fallback that relates directly to `tcp_example.py`.

## Recommended Approach

Use a hybrid architecture:

- Primary path: Redpanda/Kafka-compatible broker for streaming frames and detection results.
- Fallback/demo path: JSON lines over TCP sockets between servers.
- Storage: PostgreSQL with JSONB columns for bounding boxes and metadata.
- Detection: a local open-source detector using OpenCV DNN with MobileNet SSD by default, with an optional YOLO adapter for future upgrade.

This approach is best for the lab because it clearly shows distributed servers, queue-based decoupling, local free infrastructure, and simple demoability.

## Data Flow

```text
Camera/Webcam/Video/File/Simulator
    |
    v
Camera Ingestion Server
    |
    | topic: camera.frames
    v
Redpanda/Kafka Broker
    |
    v
Processing Server
    |
    | topic: people.detections
    v
Redpanda/Kafka Broker
    |
    v
Storage Server
    |
    v
PostgreSQL
    |
    v
Analytics Script
```

TCP fallback:

```text
Camera/Webcam/Video/File/Simulator
    |
    v
Camera Ingestion Server
    |
    | JSON line over TCP
    v
Processing Server
    |
    | JSON line over TCP
    v
Storage Server
```

## Components

### Camera Ingestion Server

Responsibilities:

- Read frames from webcam, video file, image directory, or simulated generated frames.
- Sample frames at a configurable FPS.
- Encode frame data as JPEG base64 for MVP.
- Attach metadata: `camera_id`, `frame_id`, `timestamp`, `source_type`, `image_data`, `image_encoding`.
- Publish payloads to Redpanda topic `camera.frames`.
- Support TCP client mode that sends newline-delimited JSON payloads to Processing Server.
- Reconnect or retry when the broker/TCP receiver is temporarily unavailable.

### Processing Server

Responsibilities:

- Consume frame payloads from topic `camera.frames` or receive TCP JSON lines.
- Decode image bytes.
- Detect only `person` objects.
- Produce bounding boxes with `x1`, `y1`, `x2`, `y2`, `confidence`, and `class_name`.
- Compute `people_count`.
- Measure `processing_time_ms`.
- Publish result payloads to topic `people.detections` or send TCP JSON lines to Storage Server.
- Allow CPU-only operation.

The MVP detector will use OpenCV DNN when model files are available. If model files are not configured, it will use a deterministic lightweight fallback detector that returns zero detections, so the distributed pipeline remains demoable without paid APIs or heavyweight downloads. The README will explain how to add local model files.

### Storage Server

Responsibilities:

- Consume detection results from topic `people.detections` or receive TCP JSON lines.
- Persist each result into PostgreSQL.
- Store bounding boxes in JSONB.
- Log successful inserts and connection failures.
- Retry database connection on startup and after transient failures.

### Analytics Script

Responsibilities:

- Query PostgreSQL for people-count statistics:
  - by minute
  - by hour
  - by camera
- Print tabular results to the terminal.
- Support optional CSV export.

## Technology Stack

- Python 3.10+
- OpenCV for image decoding, video capture, and optional DNN inference
- Redpanda as a Kafka-compatible local streaming broker
- PostgreSQL as the metadata and detection database
- Docker Compose for Redpanda and PostgreSQL
- `kafka-python` for broker communication
- `psycopg` for PostgreSQL
- `pydantic` for payload schema validation
- `python-dotenv` for configuration
- `pytest` for unit tests

MinIO is a documented extension, not part of the MVP. The MVP sends JPEG base64 in JSON to keep local setup simple. The architecture leaves room to switch to object storage by replacing `image_data` with `object_key`.

## Data Schemas

Frame payload:

```json
{
  "camera_id": "camera-001",
  "frame_id": "camera-001-000001",
  "timestamp": "2026-05-19T10:00:00.000000+07:00",
  "source_type": "webcam",
  "image_encoding": "jpg_base64",
  "image_data": "...",
  "width": 1280,
  "height": 720
}
```

Detection result payload:

```json
{
  "camera_id": "camera-001",
  "frame_id": "camera-001-000001",
  "timestamp": "2026-05-19T10:00:00.000000+07:00",
  "people_count": 2,
  "bounding_boxes": [
    {
      "x1": 10,
      "y1": 20,
      "x2": 120,
      "y2": 240,
      "confidence": 0.87,
      "class_name": "person"
    }
  ],
  "processing_time_ms": 35.5,
  "model_name": "opencv-dnn-mobilenet-ssd",
  "created_at": "2026-05-19T10:00:00.050000+07:00"
}
```

PostgreSQL table:

```sql
CREATE TABLE people_detections (
    id BIGSERIAL PRIMARY KEY,
    camera_id TEXT NOT NULL,
    frame_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    people_count INTEGER NOT NULL,
    bounding_boxes JSONB NOT NULL,
    processing_time_ms DOUBLE PRECISION NOT NULL,
    model_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (camera_id, frame_id)
);

CREATE INDEX idx_people_detections_camera_time
ON people_detections (camera_id, timestamp);
```

## Project Structure

```text
people_counter/
  __init__.py
  camera_server.py
  processing_server.py
  storage_server.py
  analytics.py
  config.py
  logging_config.py
  models.py
  broker/
    kafka_client.py
    tcp_json.py
  camera/
    sources.py
    frame_sampler.py
  processing/
    detector.py
    image_codec.py
  storage/
    database.py
    schema.sql
scripts/
  run_demo_tcp.sh
  run_demo_kafka.sh
tests/
  test_models.py
  test_image_codec.py
  test_analytics_sql.py
docker-compose.yml
.env.example
requirements.txt
README.md
```

## Error Handling

- Broker producers retry sends with small backoff.
- Kafka consumers keep running and log malformed messages instead of crashing.
- TCP sender reconnects with a bounded delay when the receiver is unavailable.
- Storage Server retries PostgreSQL connection before failing startup.
- Duplicate `(camera_id, frame_id)` inserts are ignored to support at-least-once delivery.
- Payload validation errors are logged with frame metadata when possible.

## Demo Plan

Kafka/Redpanda demo:

1. Start infrastructure with Docker Compose.
2. Start Storage Server.
3. Start Processing Server.
4. Start Camera Ingestion Server with `--source simulated` or a local video path.
5. Run analytics script to query PostgreSQL.

TCP demo:

1. Start Storage Server in TCP mode.
2. Start Processing Server in TCP mode.
3. Start Camera Ingestion Server in TCP mode.
4. Observe logs and query analytics.

## Out of Scope for MVP

- Multi-node Redpanda or Kafka cluster.
- GPU-specific optimizations.
- Web dashboard.
- MinIO frame archival.
- Exactly-once stream processing.

These are reasonable extension points after the lab MVP works.
