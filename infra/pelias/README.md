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

### 1. Generate a tunnel secret

This secret authenticates the Cloudflare Tunnel. Generate a random
base64-encoded value (min 32 bytes):

```bash
openssl rand -base64 32
```

Save this value -- you'll need it in the next two steps.

### 2. Update Cloudflare API token permissions

The existing Cloudflare API token (stored as `CLOUDFLARE_API_TOKEN` in
GitHub secrets) likely only has Zone and R2 permissions. The new tunnel
and Access resources require three additional **account-level** permissions.

Go to **Cloudflare Dashboard** > **My Profile** > **API Tokens**, edit the
existing token (or create a new one), and ensure it has:

| Scope | Permission | Needed for |
|-------|------------|------------|
| Zone | Zone:Edit | DNS records (already have this) |
| Zone | DNS:Edit | DNS records (already have this) |
| Account | Cloudflare R2 Storage:Edit | R2 bucket (already have this) |
| Account | **Cloudflare Tunnel:Edit** | Tunnel + tunnel config |
| Account | **Access: Service Tokens:Edit** | CI service token |
| Account | **Access: Apps and Policies:Edit** | Access application + policy |

If you created a new token, update `CLOUDFLARE_API_TOKEN` in GitHub
repository secrets and in your local `infra/deploy/terraform.tfvars`.

### 3. Add secrets and variables

The tunnel secret needs to be stored in two places:

**GitHub repository secret** (Settings > Secrets and variables > Actions):

| Secret | Value |
|--------|-------|
| `PELIAS_TUNNEL_SECRET` | The base64 string from step 1 |

**Local Terraform variables** (`infra/deploy/terraform.tfvars`):

```hcl
pelias_tunnel_secret = "<the base64 string from step 1>"
```

### 4. Deploy Cloudflare infrastructure

This creates the tunnel, DNS record (`pelias.opencrimemap.com`), Access
application, and a service token that CI uses to authenticate.

Locally:

```bash
cd infra/deploy
tofu init     # pick up the new resources
tofu plan     # review the changes
tofu apply
```

Or trigger the **Deploy Infrastructure** workflow from GitHub Actions
(it will work now that `PELIAS_TUNNEL_SECRET` is in secrets).

### 5. Add remaining GitHub secrets

After `tofu apply` succeeds, grab the outputs:

```bash
cd infra/deploy
tofu output -raw pelias_cf_access_client_id
tofu output -raw pelias_cf_access_client_secret
```

Add these as GitHub repository secrets (Settings > Secrets and variables > Actions):

| Secret | Value |
|--------|-------|
| `PELIAS_URL` | `https://pelias.opencrimemap.com` |
| `CF_ACCESS_CLIENT_ID` | Output from `tofu output -raw pelias_cf_access_client_id` |
| `CF_ACCESS_CLIENT_SECRET` | Output from `tofu output -raw pelias_cf_access_client_secret` |

### 6. Configure local tunnel token

```bash
cd infra/pelias
cp .env.example .env
```

Get the tunnel token and paste it into `.env`:

```bash
cd infra/deploy
tofu output -raw pelias_tunnel_token
```

> **Tunnel secret vs. tunnel token**: The `pelias_tunnel_secret` (from step 1)
> is the raw shared secret you generated. The `pelias_tunnel_token` is a
> base64-encoded JSON blob that Terraform computes by combining your Cloudflare
> account ID, the tunnel UUID (assigned by Cloudflare), and the secret. The
> token is what `cloudflared` actually needs to connect.

### 7. Import geocoding data

This downloads and indexes US geocoding data. Only needs to be done once
(data persists in the `data/` directory).

```bash
cd infra/pelias
./import.sh
```

The import downloads and indexes:
- **Who's On First** -- administrative boundaries (states, counties, cities)
- **OpenStreetMap** -- US extract (venues, addresses)
- **Placeholder** -- coarse geocoding (city/state lookups)

Expected time: **6-12 hours** depending on network speed.

### 8. Verify locally

```bash
cd infra/pelias
docker compose up -d

# Wait ~30 seconds for services to start, then:
curl 'http://localhost:4000/v1/search?text=1600+Pennsylvania+Ave+Washington+DC&size=1' | jq .
```

You should get a GeoJSON response with coordinates for the White House.

### 9. Verify tunnel access

With Pelias still running, test that CI can reach it through the tunnel:

```bash
curl -H "CF-Access-Client-Id: <client_id from step 5>" \
     -H "CF-Access-Client-Secret: <client_secret from step 5>" \
     'https://pelias.opencrimemap.com/v1/search?text=1600+Pennsylvania+Ave+Washington+DC&size=1' | jq .
```

Same response should come back. If it does, CI will be able to reach
your local Pelias instance.

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

**Important**: Your machine must be running Pelias + cloudflared when CI
runs. Since the data pipeline is manually triggered, start Pelias before
triggering the workflow.

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

## GitHub Secrets Reference

| Secret | Where Used | Description |
|--------|------------|-------------|
| `PELIAS_TUNNEL_SECRET` | `deploy-infra.yml` | Raw tunnel secret (input to Terraform) |
| `PELIAS_URL` | `data-pipeline.yml` | `https://pelias.opencrimemap.com` |
| `CF_ACCESS_CLIENT_ID` | `data-pipeline.yml` | Cloudflare Access service token client ID |
| `CF_ACCESS_CLIENT_SECRET` | `data-pipeline.yml` | Cloudflare Access service token client secret |

## Environment Variables

| Variable | Used By | Description |
|----------|---------|-------------|
| `PELIAS_URL` | Rust ingest binary | Override compile-time Pelias URL (e.g., `https://pelias.opencrimemap.com`) |
| `CF_ACCESS_CLIENT_ID` | Rust ingest binary | Cloudflare Access service token client ID |
| `CF_ACCESS_CLIENT_SECRET` | Rust ingest binary | Cloudflare Access service token client secret |
| `TUNNEL_TOKEN` | cloudflared container | Cloudflare Tunnel authentication token (from `tofu output -raw pelias_tunnel_token`) |
