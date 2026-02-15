#!/usr/bin/env bash
set -euo pipefail

# Crime Map deployment script
#
# Usage:
#   ./scripts/deploy.sh [command]
#
# Commands:
#   app         Build and deploy the app to Fly.io
#   data        Upload generated data files to the Fly volume
#   tiles       Upload PMTiles to Cloudflare R2
#   all         Run app + data + tiles
#
# Prerequisites:
#   - flyctl authenticated (`fly auth login`)
#   - wrangler authenticated (`bunx wrangler login`)
#   - Generated data in data/generated/

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Configuration (override via environment)
R2_BUCKET="${R2_BUCKET:-crime-map-tiles}"
VITE_TILES_URL="${VITE_TILES_URL:-}"
VITE_PROTOMAPS_API_KEY="${VITE_PROTOMAPS_API_KEY:-}"
DATA_DIR="${PROJECT_DIR}/data/generated"

command_app() {
    echo "==> Building and deploying to Fly.io..."

    local build_args=()
    if [ -n "$VITE_TILES_URL" ]; then
        echo "    VITE_TILES_URL=${VITE_TILES_URL}"
        build_args+=(--build-arg "VITE_TILES_URL=${VITE_TILES_URL}")
    fi
    if [ -n "$VITE_PROTOMAPS_API_KEY" ]; then
        echo "    VITE_PROTOMAPS_API_KEY=(set)"
        build_args+=(--build-arg "VITE_PROTOMAPS_API_KEY=${VITE_PROTOMAPS_API_KEY}")
    fi

    fly deploy "${build_args[@]}"
    echo "==> Deploy complete!"
}

command_data() {
    echo "==> Uploading generated data to Fly volume..."

    # Start a background keepalive to prevent Fly auto-suspend during upload
    local fly_app
    fly_app=$(fly status --json 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin)['Name'])" 2>/dev/null || echo "crime-map")
    echo "    Starting keepalive for ${fly_app}.fly.dev..."
    (while true; do curl -sf "https://${fly_app}.fly.dev/api/health" > /dev/null 2>&1; sleep 30; done) &
    local keepalive_pid=$!
    trap "kill $keepalive_pid 2>/dev/null" EXIT

    # Ensure the machine is awake before starting uploads
    echo "    Waking machine..."
    curl -sf "https://${fly_app}.fly.dev/api/health" > /dev/null 2>&1 || true
    sleep 2

    local files=(
        "incidents.db"
        "counts.duckdb"
        "h3.duckdb"
        "metadata.json"
        "manifest.json"
    )

    # Remove existing files so SFTP put can overwrite
    echo "    Clearing existing data files on volume..."
    fly ssh console --command "rm -f /app/data/generated/incidents.db /app/data/generated/counts.duckdb /app/data/generated/h3.duckdb /app/data/generated/metadata.json /app/data/generated/manifest.json" 2>/dev/null || true

    for file in "${files[@]}"; do
        local filepath="${DATA_DIR}/${file}"
        if [ -f "$filepath" ]; then
            local size
            size=$(du -h "$filepath" | cut -f1)
            echo "    Uploading ${file} (${size})..."
            fly ssh sftp shell <<EOF
put ${filepath} /app/data/generated/${file}
EOF
        else
            echo "    Skipping ${file} (not found)"
        fi
    done

    kill $keepalive_pid 2>/dev/null
    trap - EXIT

    # Verify uploads by comparing checksums
    echo "==> Verifying uploaded files..."
    local verify_failed=false
    for file in "${files[@]}"; do
        local filepath="${DATA_DIR}/${file}"
        if [ -f "$filepath" ]; then
            local local_size remote_size
            local_size=$(wc -c < "$filepath" | tr -d ' ')
            remote_size=$(fly ssh console --command "wc -c < /app/data/generated/${file}" 2>/dev/null | tr -d '[:space:]')

            if [ "$local_size" != "$remote_size" ]; then
                echo "    MISMATCH ${file}: local=${local_size} bytes, remote=${remote_size} bytes"
                verify_failed=true
            else
                echo "    OK ${file} (${local_size} bytes)"
            fi
        fi
    done

    if [ "$verify_failed" = true ]; then
        echo ""
        echo "WARNING: Some files failed verification. Re-run './scripts/deploy.sh data' to retry."
        exit 1
    fi

    # Restart the machine to ensure clean data loading
    echo "==> Restarting machine to load new data..."
    fly machine restart --skip-health-checks
    sleep 3

    # Wait for data to be ready
    echo "==> Waiting for server to load data..."
    local attempts=0
    local max_attempts=20
    while [ $attempts -lt $max_attempts ]; do
        local health
        health=$(curl -sf "https://${fly_app}.fly.dev/api/health" 2>/dev/null || echo "{}")
        if echo "$health" | python3 -c "import sys,json; sys.exit(0 if json.load(sys.stdin).get('dataReady') else 1)" 2>/dev/null; then
            echo "    Server reports dataReady=true"
            break
        fi
        attempts=$((attempts + 1))
        echo "    Waiting for data to load... (${attempts}/${max_attempts})"
        sleep 3
    done

    if [ $attempts -ge $max_attempts ]; then
        echo "WARNING: Server did not report dataReady=true within expected time."
        echo "         Check logs with: fly logs"
    fi

    echo "==> Data upload complete!"
}

command_tiles() {
    echo "==> Uploading PMTiles to Cloudflare R2..."

    local pmtiles="${DATA_DIR}/incidents.pmtiles"
    if [ ! -f "$pmtiles" ]; then
        echo "ERROR: ${pmtiles} not found. Run 'cargo generate all' first."
        exit 1
    fi

    local size
    size=$(du -h "$pmtiles" | cut -f1)
    echo "    Uploading incidents.pmtiles (${size}) to bucket '${R2_BUCKET}'..."
    bunx wrangler r2 object put "${R2_BUCKET}/incidents.pmtiles" \
        --file "$pmtiles" \
        --content-type "application/octet-stream" \
        --remote

    echo "==> PMTiles upload complete!"
    echo ""
    echo "    After enabling R2.dev public access in the Cloudflare dashboard,"
    echo "    set VITE_TILES_URL to: pmtiles://https://pub-<hash>.r2.dev/incidents.pmtiles"
    echo "    Then redeploy: ./scripts/deploy.sh app"
}

command_all() {
    command_tiles
    command_data
    command_app
}

# ── Main ─────────────────────────────────────────────────────────

case "${1:-all}" in
    app)   command_app ;;
    data)  command_data ;;
    tiles) command_tiles ;;
    all)   command_all ;;
    *)
        echo "Usage: $0 {app|data|tiles|all}"
        exit 1
        ;;
esac
