# Pelias Geocoder (Local)

Docker Compose setup for running a self-hosted [Pelias](https://pelias.io)
geocoder locally, exposed to CI via a Cloudflare Tunnel.

## Why Pelias?

The crime map pipeline geocodes ~170K+ addresses. The default Census Bureau
batch API handles most, but misses addresses that need fuzzy matching. Pelias
fills the gap between Census (fast, batch, but strict) and Nominatim (accurate,
but rate-limited to 1 req/1.1s):

| Provider  | Speed          | Coverage    | Cost      |
|-----------|----------------|-------------|-----------|
| Census    | ~10K/batch     | Exact match | Free      |
| **Pelias**| ~100 req/s     | Fuzzy, US   | Free (self-hosted) |
| Nominatim | ~0.9 req/s     | Global      | Free      |

## Prerequisites

- **Docker** and **Docker Compose** installed
- **~16 GB RAM** available (4 GB for Elasticsearch heap + OS/services)
- **~150 GB free disk** for geocoding data (Elasticsearch indices, OSM, WOF)
- **OpenTofu** (or Terraform) for one-time tunnel setup

On Linux, Elasticsearch requires a kernel tuning parameter:

```bash
sudo sysctl -w vm.max_map_count=262144
echo "vm.max_map_count=262144" | sudo tee /etc/sysctl.d/99-elasticsearch.conf
```

## One-Time Setup

### 1. Create the Cloudflare Tunnel

The tunnel lets CI reach your local Pelias instance through
`pelias.opencrimemap.com` without opening any inbound ports.

```bash
cd ../deploy
tofu apply   # Creates the tunnel, DNS record, and Access policy
```

### 2. Configure the tunnel token

```bash
cd ../pelias
cp .env.example .env

# Get the tunnel token from Terraform output
cd ../deploy
tofu output -raw pelias_tunnel_token
# Paste this into .env as TUNNEL_TOKEN
```

### 3. Add GitHub secrets

From the Terraform outputs, add these GitHub repository secrets:

| Secret | Source |
|--------|--------|
| `PELIAS_URL` | `https://pelias.opencrimemap.com` |
| `CF_ACCESS_CLIENT_ID` | `tofu output -raw pelias_cf_access_client_id` |
| `CF_ACCESS_CLIENT_SECRET` | `tofu output -raw pelias_cf_access_client_secret` |

### 4. Import geocoding data

This downloads and indexes US geocoding data. Only needs to be done once
(data persists in `data/` directory).

```bash
./import.sh
```

The import downloads and indexes:
- **Who's On First** -- administrative boundaries (states, counties, cities)
- **OpenStreetMap** -- US extract (venues, addresses)
- **Placeholder** -- coarse geocoding (city/state lookups)

Expected time: **6-12 hours** depending on network speed.

### 5. Verify

```bash
curl 'http://localhost:4000/v1/search?text=1600+Pennsylvania+Ave+Washington+DC&size=1' | jq .
```

## Daily Usage

### Start Pelias (before running the pipeline)

```bash
cd infra/pelias
docker compose up -d
```

This starts all 5 services:

| Service | Port | Purpose |
|---------|------|---------|
| Elasticsearch | 9200 (internal) | Full-text search index |
| Pelias API | 4000 (local) | HTTP geocoding endpoint |
| Placeholder | 4100 (internal) | Coarse geocoding |
| PIP Service | 4200 (internal) | Point-in-polygon lookups |
| cloudflared | -- | Tunnel to Cloudflare edge |

### Run the pipeline

Locally:

```bash
cargo ingest sync-all
# Pelias is used automatically at localhost:4000
```

Via CI:

```bash
# Trigger the GitHub Actions workflow -- it reaches Pelias through
# the Cloudflare Tunnel at pelias.opencrimemap.com
gh workflow run data-pipeline.yml
```

### Stop Pelias (when done)

```bash
docker compose down
# Data persists in ./data/ for next time
```

## Operations

### Check service status

```bash
docker compose ps
docker compose logs -f api
```

### Update Pelias containers

```bash
docker compose pull
docker compose up -d
```

### Re-import data

To update the geocoding data (new OSM extract, updated WOF, etc.):

```bash
docker compose down
./import.sh
```

## Architecture

```
                  crime-map pipeline
                        |
            +-----------+-----------+
            |           |           |
         Census      Pelias     Nominatim
        (priority 1) (priority 2) (priority 3)
            |           |           |
       Batch API   Self-hosted   Public API
       10K/req     ~100 req/s   ~0.9 req/s
                        |
              +---------+---------+
              |    Local Docker   |
              |    Compose Stack  |
              +---------+---------+
              |         |         |
           Elastic   Pelias    PIP
           Search     API    Service
              |
         cloudflared
              |
      Cloudflare Tunnel
              |
    pelias.opencrimemap.com
         (CI access)
```

## Environment Variables

| Variable | Used By | Description |
|----------|---------|-------------|
| `PELIAS_URL` | Rust ingest binary | Override compile-time Pelias URL (e.g., `https://pelias.opencrimemap.com`) |
| `CF_ACCESS_CLIENT_ID` | Rust ingest binary | Cloudflare Access service token client ID |
| `CF_ACCESS_CLIENT_SECRET` | Rust ingest binary | Cloudflare Access service token client secret |
| `TUNNEL_TOKEN` | cloudflared container | Cloudflare Tunnel authentication token |
