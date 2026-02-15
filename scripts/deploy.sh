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
DATA_DIR="${PROJECT_DIR}/data/generated"

command_app() {
    echo "==> Building and deploying to Fly.io..."
    if [ -n "$VITE_TILES_URL" ]; then
        echo "    VITE_TILES_URL=${VITE_TILES_URL}"
        fly deploy --build-arg "VITE_TILES_URL=${VITE_TILES_URL}"
    else
        fly deploy
    fi
    echo "==> Deploy complete!"
}

command_data() {
    echo "==> Uploading generated data to Fly volume..."

    local files=(
        "incidents.db"
        "counts.duckdb"
        "h3.duckdb"
        "metadata.json"
        "manifest.json"
    )

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
