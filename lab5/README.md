# Distributed People Counter

Hệ thống đếm số người trong camera/video theo kiến trúc phân tán, có yếu tố stream processing và dữ liệu lớn. Project gồm 3 server Python độc lập:

- **Camera Ingestion Server**: đọc webcam, video file, thư mục ảnh hoặc nguồn giả lập; lấy frame theo FPS cấu hình; gửi frame đi.
- **Processing Server**: nhận frame; chạy YOLO để nhận diện class `person`; tạo bounding boxes và `people_count`.
- **Storage Server**: nhận kết quả; lưu metadata, bounding boxes và số người vào PostgreSQL.

Hệ thống dùng một model object detection: YOLO qua Ultralytics.

## Luồng dữ liệu

Luồng chính dùng Redpanda/Kafka:

```text
Webcam / Video / Image Directory / Simulator
    |
    v
Camera Ingestion Server
    |
    | Kafka topic: camera.frames
    v
Redpanda
    |
    v
Processing Server
    |
    | Kafka topic: people.detections
    v
Redpanda
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
## Công nghệ sử dụng

- Python 3.9+
- Redpanda, Kafka-compatible broker, chạy local bằng Docker
- PostgreSQL để lưu kết quả xử lý
- Ultralytics YOLO, mặc định `yolo11n.pt`
- OpenCV để đọc webcam/video và encode/decode frame
- Docker Compose để chạy hạ tầng miễn phí local

## Cấu trúc project

```text
people_counter/
  camera_server.py          # Camera Ingestion Server
  processing_server.py      # Processing Server chạy YOLO
  storage_server.py         # Storage Server lưu PostgreSQL
  analytics.py              # Query thống kê theo phút/giờ/camera
  config.py                 # Đọc cấu hình từ .env
  models.py                 # Schema payload frame/result/bounding box
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
tests/
docker-compose.yml
.env.example
requirements.txt
```

## Cài đặt lần đầu

Vào thư mục project:

```bash
cd path/to/lab5
```

Tạo `.env` và cài Python dependencies:

```bash
cp .env.example .env
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
pip install -r requirements.txt
```

Khởi động Redpanda và PostgreSQL:

```bash
docker compose up -d
docker compose ps
```

## Cấu hình quan trọng

File `.env` mặc định:

```env
BROKER_URL=localhost:9092
DATABASE_URL=postgresql://people_counter:people_counter@localhost:5432/people_counter

CAMERA_ID=camera-001
FPS=1
SOURCE_TYPE=simulated
SOURCE_PATH=
MAX_FRAMES=0

DETECTOR_MODEL_NAME=yolo11n.pt
CONFIDENCE_THRESHOLD=0.25
```

Ý nghĩa:

- `MAX_FRAMES=0`: chạy đến hết nguồn dữ liệu. Với video, nghĩa là chạy hết video.
- `FPS`: số frame/giây muốn lấy mẫu từ webcam/video.
- `DETECTOR_MODEL_NAME=yolo11n.pt`: model YOLO duy nhất được dùng.
- `CONFIDENCE_THRESHOLD=0.25`: ngưỡng confidence để giữ detection.

Lần đầu chạy YOLO, Ultralytics có thể tự tải `yolo11n.pt` về thư mục project. File `.pt` đã được ignore trong Git.

## Chạy pipeline Kafka/Redpanda

Mở 3 terminal riêng. Terminal nào cũng vào project và activate venv:

```bash
cd path/to/lab5
source .venv/bin/activate
```

Terminal 1, chạy Storage Server:

```bash
python3 -m people_counter.storage_server --mode kafka
```

Khi sẵn sàng, server sẽ chờ kết quả:

```text
Storage Server ready; waiting for detection results on topic people.detections
```

Terminal 2, chạy Processing Server:

```bash
python3 -m people_counter.processing_server --mode kafka
```

Khi YOLO load thành công:

```text
Using Ultralytics YOLO detector: yolo11n.pt
Processing Server ready; waiting for frames on topic camera.frames
```

Terminal 3, chạy Camera Server.

Với video:

```bash
python3 -m people_counter.camera_server \
  --mode kafka \
  --camera-id yolo-full \
  --source-type video \
  --source-path path/to/video.mp4 \
  --fps 5
```

## Chạy với webcam

Giữ Storage Server và Processing Server đang chạy, rồi ở Terminal 3:

```bash
python3 -m people_counter.camera_server \
  --mode kafka \
  --camera-id webcam-001 \
  --source-type webcam \
  --source-path 0 \
  --fps 1
```

`--source-path 0` là webcam mặc định.

## Chạy với thư mục ảnh

```bash
python3 -m people_counter.camera_server \
  --mode kafka \
  --camera-id image-dir-001 \
  --source-type directory \
  --source-path /path/to/images \
  --fps 2
```

## Xem kết quả

Thống kê theo camera:

```bash
python3 -m people_counter.analytics --group-by camera
```

Lọc riêng một camera/video:

```bash
python3 -m people_counter.analytics --group-by camera --camera-id yolo-full
```

Thống kê theo phút:

```bash
python3 -m people_counter.analytics --group-by minute --camera-id yolo-full
```

Thống kê theo giờ:

```bash
python3 -m people_counter.analytics --group-by hour --camera-id yolo-full
```

Xuất CSV:

```bash
python3 -m people_counter.analytics --group-by camera --csv output.csv
```

## Payload schema

Frame payload gửi từ Camera Server:

```json
{
  "camera_id": "camera-001",
  "frame_id": "camera-001-000001",
  "timestamp": "2026-05-19T10:00:00+07:00",
  "source_type": "video",
  "image_encoding": "jpg_base64",
  "image_data": "...",
  "width": 1280,
  "height": 720
}
```

Detection result gửi từ Processing Server:

```json
{
  "camera_id": "camera-001",
  "frame_id": "camera-001-000001",
  "timestamp": "2026-05-19T10:00:00+07:00",
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
  "model_name": "yolo11n.pt",
  "created_at": "2026-05-19T10:00:00.050000+07:00"
}
```

## Database schema

Schema nằm ở `people_counter/storage/schema.sql`.

Bảng `people_detections` lưu:

- `camera_id`
- `frame_id`
- `timestamp`
- `people_count`
- `bounding_boxes` dạng JSONB
- `processing_time_ms`
- `model_name`
- `created_at`

`UNIQUE (camera_id, frame_id)` giúp tránh lưu trùng khi message được gửi lại.