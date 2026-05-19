CREATE TABLE IF NOT EXISTS people_detections (
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

CREATE INDEX IF NOT EXISTS idx_people_detections_camera_time
ON people_detections (camera_id, timestamp);
