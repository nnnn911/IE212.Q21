#!/usr/bin/env bash
set -euo pipefail

echo "Start infrastructure:"
echo "  docker compose up -d"
echo
echo "Terminal 1: python3 -m people_counter.storage_server --mode kafka"
echo "Terminal 2: python3 -m people_counter.processing_server --mode kafka"
echo "Terminal 3: python3 -m people_counter.camera_server --mode kafka --source-type simulated --fps 5 --max-frames 100"
echo "Then query: python3 -m people_counter.analytics --group-by minute"
