# Pelias Geocoder Infrastructure

OpenTofu configuration for deploying a self-hosted [Pelias](https://pelias.io)
geocoder on Oracle Cloud's Always Free tier.

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

1. **Oracle Cloud Account** with Always Free tier
   - Sign up at https://cloud.oracle.com
   - Create an API signing key: Profile > API Keys > Add API Key
   - Note your tenancy OCID, user OCID, and key fingerprint

2. **OpenTofu** (or Terraform) installed
   - `brew install opentofu` or see https://opentofu.org/docs/intro/install/

3. **SSH key pair** for instance access

## Setup

### 1. Create a variables file

```bash
cp terraform.tfvars.example terraform.tfvars
# Edit with your OCI credentials
```

Create `terraform.tfvars`:

```hcl
tenancy_ocid        = "ocid1.tenancy.oc1..aaaaaa..."
user_ocid           = "ocid1.user.oc1..aaaaaa..."
api_key_fingerprint = "aa:bb:cc:dd:..."
private_key_path    = "~/.oci/oci_api_key.pem"
region              = "us-ashburn-1"
ssh_public_key_path = "~/.ssh/id_ed25519.pub"
```

### 2. Deploy the instance

```bash
tofu init
tofu plan
tofu apply
```

This creates:
- ARM VM (4 OCPU, 24 GB RAM) with 150 GB boot volume
- VCN with public subnet, internet gateway
- Security rules for SSH (22) and Pelias API (4000)
- Docker + Docker Compose pre-installed via cloud-init

### 3. Import geocoding data

SSH into the instance and run the import script:

```bash
ssh ubuntu@$(tofu output -raw instance_public_ip)

# Wait for cloud-init to finish (check with):
cloud-init status --wait

# Run the import (6-12 hours for full US data)
sudo /data/pelias/import.sh
```

The import downloads and indexes:
- **Who's On First** — administrative boundaries (states, counties, cities)
- **OpenStreetMap** — US extract (venues, addresses)
- **Placeholder** — coarse geocoding (city/state lookups)

### 4. Verify

```bash
curl 'http://<PUBLIC_IP>:4000/v1/search?text=1600+Pennsylvania+Ave+Washington+DC&size=1' | jq .
```

### 5. Update the geocoder config

Edit `packages/geocoder/services/pelias.toml` and set `base_url` to
your instance's public IP:

```toml
base_url = "http://<PUBLIC_IP>:4000"
```

## Instance Details

| Resource | Spec |
|----------|------|
| Shape    | VM.Standard.A1.Flex (ARM) |
| CPU      | 4 OCPU (Ampere A1) |
| RAM      | 24 GB |
| Storage  | 150 GB boot volume |
| OS       | Ubuntu 24.04 ARM |
| Cost     | $0/month (Always Free) |

## Operations

### Check service status

```bash
ssh ubuntu@$(tofu output -raw instance_public_ip)
cd /data/pelias
docker compose ps
docker compose logs -f api
```

### Restart services

```bash
cd /data/pelias
docker compose restart
```

### Update Pelias containers

```bash
cd /data/pelias
docker compose pull
docker compose up -d
```

### Destroy infrastructure

```bash
tofu destroy
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
                  OCI Always Free
                  ARM VM (4c/24GB)
                        |
              +---------+---------+
              |         |         |
           Elastic   Pelias    PIP
           Search     API    Service
```
