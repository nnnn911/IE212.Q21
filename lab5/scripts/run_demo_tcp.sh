#!/usr/bin/env bash
set -euo pipefail

echo "Terminal 1: python3 -m people_counter.storage_server --mode tcp"
echo "Terminal 2: python3 -m people_counter.processing_server --mode tcp"
echo "Terminal 3: python3 -m people_counter.camera_server --mode tcp --source-type simulated --fps 1 --max-frames 10"
echo "Then query: python3 -m people_counter.analytics --group-by minute"
