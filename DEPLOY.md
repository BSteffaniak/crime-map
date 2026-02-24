# Deployment Guide

This document covers how to deploy the crime map application, including
infrastructure provisioning, app deployment, and data pipeline execution.

## Architecture

```
Browser
  ├─ Map tiles ──► Cloudflare R2 (incidents.pmtiles)
  │                Free: 10 GB storage, 10M reads/mo, $0 egress
  │
  └─ App + API ──► Cloudflare DNS/Proxy ──► Fly.io
                                             shared-cpu-2x, 2 GB RAM
                                             auto_stop = "suspend"
                                             min_machines = 0

                                             Volume (5 GB):
                                             ├── incidents.db     (SQLite sidebar)
                                             ├── counts.duckdb    (count aggregates)
                                             ├── h3.duckdb        (H3 hexbin data)
                                             ├── metadata.json    (server startup context)
                                             └── manifest.json    (generation cache)
```

**Key design decisions:**

- PMTiles are served from Cloudflare R2 CDN (free egress, HTTP range
  requests). This means tile loading works even when the Fly.io machine is
  suspended.
- The Fly.io machine uses `auto_stop = "suspend"` to minimize costs. It
  suspends after idle and resumes in ~200-500ms on the next request.
- No external database is required at runtime. The server boots from
  pre-generated SQLite/DuckDB files. DuckDB source files are only needed
  during data ingestion and generation (locally or in CI).
- The server starts immediately even if data files are missing. A background
  task polls for files and initializes connections once they appear.

## Estimated Monthly Cost

| Resource | Cost |
|---|---|
| Fly.io compute (mostly suspended) | ~$1-4 |
| Fly Volume 5 GB | ~$0.75 |
| Cloudflare R2 (within free tier) | $0 |
| Cloudflare DNS/TLS/proxy | $0 |
| **Total** | **~$2-5/mo** |

## Prerequisites

### Accounts

- **Fly.io** -- [fly.io](https://fly.io) (credit card required for
  pay-as-you-go)
- **Cloudflare** -- [cloudflare.com](https://dash.cloudflare.com) (free plan)
- **GitHub** -- For CI/CD workflows

### CLI Tools

- `flyctl` -- `brew install flyctl` or [install docs](https://fly.io/docs/flyctl/install/)
- `tofu` (OpenTofu) -- `brew install opentofu` or [install docs](https://opentofu.org/docs/intro/install/)
- `bunx` (Bun) -- For running `wrangler` without global install

### Authenticate CLIs

```bash
fly auth login
bunx wrangler login    # one-time OAuth for R2 uploads
```

## One-Time Infrastructure Setup

### 1. Create Cloudflare API Token

Go to [Cloudflare API Tokens](https://dash.cloudflare.com/profile/api-tokens)
and create a custom token named `crime-map-terraform` with these permissions:

| Scope | Resource | Permission |
|---|---|---|
| Zone | DNS | Edit |
| Zone | Zone | Read |
| Account | Workers R2 Storage | Edit |

Set **Zone Resources** to "Include > Specific zone > opencrimemap.com".

### 2. Get Cloudflare Account ID

Dashboard > opencrimemap.com > right sidebar > **Account ID**.

### 3. Create Fly.io API Token

```bash
fly tokens create org personal
```

### 4. Provision Infrastructure

```bash
cd infra/deploy
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your actual values:
#   fly_api_token, cloudflare_api_token, cloudflare_account_id
tofu init
tofu apply
```

This creates:

- Fly.io app (`crime-map`) with shared IPv4, IPv6, volume (5 GB), and TLS
  cert
- Cloudflare R2 bucket (`crime-map-tiles`)
- Cloudflare DNS CNAME records (root + www -> `crime-map.fly.dev`)

### 5. Configure Cloudflare

- **SSL/TLS**: Dashboard > opencrimemap.com > SSL/TLS > Overview > set to
  **Full (strict)**
- **R2 Public Access**: Dashboard > R2 > `crime-map-tiles` > Settings >
  Public Access > enable R2.dev subdomain. Copy the
  `pub-<hash>.r2.dev` URL.

### 6. Set GitHub Actions Secrets

Go to your repo Settings > Secrets and variables > Actions > New repository
secret. Add:

| Secret | Value | Used By |
|---|---|---|
| `FLY_API_TOKEN` | From `fly tokens create org personal` | Both deploy workflows |
| `VITE_TILES_URL` | `pmtiles://https://pub-<hash>.r2.dev/incidents.pmtiles` | deploy-app |
| `VITE_PROTOMAPS_API_KEY` | Your Protomaps API key | deploy-app |
| `CLOUDFLARE_API_TOKEN` | Cloudflare token (Zone:DNS:Edit, R2:Edit) | deploy-data |
| `CLOUDFLARE_ACCOUNT_ID` | Cloudflare account ID | deploy-data |

## Deploying

### Deploy the App (code changes)

Deploys the Docker image (Rust server + frontend) to Fly.io. Does **not**
touch data files or tiles.

**Locally:**

```bash
VITE_TILES_URL="pmtiles://https://pub-<hash>.r2.dev/incidents.pmtiles" \
  ./scripts/deploy.sh app
```

**Via GitHub Actions:**

Go to Actions > "Deploy App" > Run workflow. Optionally override
`vite_tiles_url`.

### Deploy Data (new/updated crime data)

Runs the full data pipeline: ingest from source APIs, generate all output
files, upload PMTiles to R2, and upload data files to the Fly volume.

**Locally:**

```bash
# 1. Ingest crime data (DuckDB files in data/sources/)
cargo ingest sync-all

# 2. Generate all outputs
cargo generate all

# 3. Upload tiles to R2
./scripts/deploy.sh tiles

# 4. Upload data files to Fly volume
./scripts/deploy.sh data
```

**Via GitHub Actions:**

Go to Actions > "Deploy Data" > Run workflow. Inputs:

| Input | Default | Description |
|---|---|---|
| `sources` | `dc_mpd` | Comma-separated source IDs to ingest |
| `limit` | (none) | Max records per source |
| `force` | `false` | Force full re-sync and regeneration |

The CI workflow ingests data into per-source DuckDB files (pulled from /
pushed to R2), runs generation, then uploads the outputs.

**Examples:**

```
# DC data only (default)
sources: dc_mpd

# Multiple cities
sources: dc_mpd,chicago_pd,seattle_pd

# All sources (slow, ~30-60 min)
sources: (leave empty and set to all in the ingest step)
```

### Deploy Everything

```bash
./scripts/deploy.sh all    # tiles + data + app
```

## deploy.sh Reference

```
Usage: ./scripts/deploy.sh [command]

Commands:
  app     Build and deploy the Docker image to Fly.io
  data    Upload generated data files to the Fly volume
  tiles   Upload PMTiles to Cloudflare R2
  all     Run tiles + data + app (in that order)

Environment variables:
  VITE_TILES_URL           PMTiles CDN URL (baked into frontend build)
  VITE_PROTOMAPS_API_KEY   Protomaps basemap API key
  R2_BUCKET                Cloudflare R2 bucket name (default: crime-map-tiles)
```

### What `deploy.sh data` Does

1. Starts a background keepalive (pings `/api/health` every 30s to prevent
   Fly auto-suspend during upload)
2. Wakes the machine with an initial health check
3. For each data file: compares local vs remote file size
   - If sizes match, skips the file (unchanged)
   - If different, deletes the remote file and uploads via SFTP
4. Kills the keepalive
5. If any files were uploaded:
   - Verifies each uploaded file's size matches
   - Restarts the machine (`fly machine restart`)
   - Polls `/api/health` until `dataReady=true`

## Configuration Reference

### Runtime Environment Variables

Set via `fly.toml` `[env]` section or `fly secrets set`:

| Variable | Default | Description |
|---|---|---|
| `BIND_ADDR` | `127.0.0.1` | Server bind address |
| `PORT` | `8080` | Server port |
| `RUST_LOG` | `info` | Log level |
| `AI_PROVIDER` | (none) | AI provider: `anthropic`, `openai`, or `bedrock` |
| `ANTHROPIC_API_KEY` | (none) | Anthropic API key (if using Anthropic) |

### Build Arguments (Dockerfile)

| Argument | Default | Description |
|---|---|---|
| `VITE_TILES_URL` | `""` | PMTiles CDN URL baked into the frontend |
| `VITE_PROTOMAPS_API_KEY` | `""` | Protomaps basemap API key |

### OpenTofu Variables (`infra/deploy/`)

| Variable | Default | Description |
|---|---|---|
| `fly_api_token` | (required) | Fly.io API token |
| `fly_app_name` | `crime-map` | Fly.io app name (globally unique) |
| `fly_org` | `personal` | Fly.io organization |
| `fly_region` | `iad` | Fly.io region |
| `fly_volume_size_gb` | `5` | Persistent volume size |
| `cloudflare_api_token` | (required) | Cloudflare API token |
| `cloudflare_account_id` | (required) | Cloudflare account ID |
| `domain` | `opencrimemap.com` | Primary domain |
| `r2_bucket_name` | `crime-map-tiles` | R2 bucket name |

## Troubleshooting

### Machine is suspended, can't SSH/SFTP

The Fly machine auto-suspends when idle. Wake it first:

```bash
curl -sf https://crime-map.fly.dev/api/health
# Wait a few seconds, then retry your command
```

The `deploy.sh data` script handles this automatically with a keepalive.

### SFTP says "file exists on VM"

Fly's SFTP `put` doesn't overwrite existing files. The deploy script
handles this by deleting files before uploading. If you're running SFTP
manually:

```bash
fly ssh console --command "rm -f /app/data/generated/<filename>"
fly ssh sftp shell
# put <local-path> /app/data/generated/<filename>
```

### Data files uploaded but server shows dataReady=false

The server uses a `OnceLock` that is set once on first successful load. If
corrupted files were loaded before good files were uploaded, the server
needs a restart:

```bash
fly machine restart --skip-health-checks
```

The `deploy.sh data` script does this automatically after uploading.

### Deploy fails with "context deadline exceeded"

This is a non-fatal error from Fly's build system (builder cleanup timing
out). Check whether the deploy actually succeeded:

```bash
fly status
# Look for the machine in "started" or "stopped" state
```

### Volume mount path mismatch

The Dockerfile sets `WORKDIR /app`, and the server uses relative paths
(`data/generated/`). The volume must be mounted at `/app/data/generated`
(set in `fly.toml`). If you see "unable to open database file" errors,
check that the mount destination matches.

### Checking server logs

```bash
fly logs              # Live tail
fly logs --no-tail    # Recent logs
```

### Health endpoint

```bash
curl https://crime-map.fly.dev/api/health | python3 -m json.tool
```

Returns:

```json
{
    "healthy": true,
    "version": "0.1.0",
    "dataReady": true
}
```

- `dataReady`: Pre-generated data files loaded (SQLite + DuckDB)
