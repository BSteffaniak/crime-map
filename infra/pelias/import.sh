#!/bin/bash
set -euo pipefail

# Pelias data import script — downloads and indexes US geocoding data.
#
# Prerequisites:
#   - Docker and Docker Compose installed
#   - ~16 GB RAM available
#   - ~150 GB free disk space
#
# On Linux, Elasticsearch requires vm.max_map_count >= 262144:
#   sudo sysctl -w vm.max_map_count=262144
#   echo "vm.max_map_count=262144" | sudo tee /etc/sysctl.d/99-elasticsearch.conf

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Pelias Data Import ==="
echo "This will download and import US geocoding data."
echo "Expected disk usage: ~80-100 GB"
echo "Expected time: 6-12 hours depending on network speed"
echo ""

# Check vm.max_map_count on Linux
if [ "$(uname)" = "Linux" ]; then
  MAP_COUNT=$(sysctl -n vm.max_map_count 2>/dev/null || echo "0")
  if [ "$MAP_COUNT" -lt 262144 ]; then
    echo "ERROR: vm.max_map_count is $MAP_COUNT (need >= 262144)"
    echo "Run: sudo sysctl -w vm.max_map_count=262144"
    exit 1
  fi
fi

# Create data directories
mkdir -p data/elasticsearch data/placeholder data/whosonfirst data/openstreetmap

# Derive the Docker Compose project name (used for network naming)
PROJECT_NAME=$(basename "$SCRIPT_DIR" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]//g')
NETWORK="${PROJECT_NAME}_default"

# Start Elasticsearch first
echo "[1/8] Starting Elasticsearch..."
docker compose up -d elasticsearch
echo "Waiting for Elasticsearch to be ready..."
until curl -s http://localhost:9200/_cluster/health | grep -qE '"status":"(yellow|green)"'; do
  sleep 5
  echo "  waiting..."
done
echo "Elasticsearch is ready."

# Create Pelias schema
echo "[2/8] Creating Pelias schema..."
docker run --rm --network "$NETWORK" \
  -v "$SCRIPT_DIR/pelias.json:/code/pelias.json:ro" \
  pelias/schema:latest node scripts/create_index.js

# Download Who's On First data
echo "[3/8] Downloading Who's On First data (US)..."
docker run --rm \
  -v "$SCRIPT_DIR/pelias.json:/code/pelias.json:ro" \
  -v "$SCRIPT_DIR/data/whosonfirst:/data/whosonfirst" \
  pelias/whosonfirst:latest ./bin/download

# Import Who's On First
echo "[4/8] Importing Who's On First..."
docker run --rm --network "$NETWORK" \
  -v "$SCRIPT_DIR/pelias.json:/code/pelias.json:ro" \
  -v "$SCRIPT_DIR/data/whosonfirst:/data/whosonfirst" \
  pelias/whosonfirst:latest ./bin/start

# Download and import OpenStreetMap
echo "[5/8] Downloading OpenStreetMap US extract..."
docker run --rm \
  -v "$SCRIPT_DIR/pelias.json:/code/pelias.json:ro" \
  -v "$SCRIPT_DIR/data/openstreetmap:/data/openstreetmap" \
  pelias/openstreetmap:latest ./bin/download

echo "[6/8] Importing OpenStreetMap..."
docker run --rm --network "$NETWORK" \
  -v "$SCRIPT_DIR/pelias.json:/code/pelias.json:ro" \
  -v "$SCRIPT_DIR/data/openstreetmap:/data/openstreetmap" \
  pelias/openstreetmap:latest ./bin/start

# Download and build placeholder
echo "[7/8] Building Placeholder..."
docker run --rm \
  -v "$SCRIPT_DIR/pelias.json:/code/pelias.json:ro" \
  -v "$SCRIPT_DIR/data/placeholder:/data/placeholder" \
  pelias/placeholder:latest ./cmd/download.sh
docker run --rm \
  -v "$SCRIPT_DIR/pelias.json:/code/pelias.json:ro" \
  -v "$SCRIPT_DIR/data/placeholder:/data/placeholder" \
  pelias/placeholder:latest ./cmd/build.sh

# Start all services
echo "[8/8] Starting all Pelias services..."
docker compose up -d

echo ""
echo "=== Import complete ==="
echo "Pelias API: http://localhost:4000"
echo "Test: curl 'http://localhost:4000/v1/search?text=1600+Pennsylvania+Ave+Washington+DC&size=1'"
