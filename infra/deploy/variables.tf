# ── Fly.io ───────────────────────────────────────────────────────

variable "fly_api_token" {
  type        = string
  sensitive   = true
  description = "Fly.io API token (from `fly tokens create`)"
}

variable "fly_app_name" {
  type        = string
  default     = "crime-map"
  description = "Name of the Fly.io application"
}

variable "fly_org" {
  type        = string
  default     = "personal"
  description = "Fly.io organization slug"
}

variable "fly_region" {
  type        = string
  default     = "iad"
  description = "Fly.io primary region (Ashburn, VA)"
}

variable "fly_volume_size_gb" {
  type        = number
  default     = 25
  description = "Size of persistent volume in GB"
}

# ── Cloudflare ───────────────────────────────────────────────────

variable "cloudflare_api_token" {
  type        = string
  sensitive   = true
  description = "Cloudflare API token with Zone:Edit and R2:Edit permissions"
}

variable "cloudflare_account_id" {
  type        = string
  description = "Cloudflare account ID (found in dashboard overview)"
}

variable "domain" {
  type        = string
  default     = "opencrimemap.com"
  description = "Primary domain name (must be added to Cloudflare)"
}

variable "r2_bucket_name" {
  type        = string
  default     = "crime-map-tiles"
  description = "Cloudflare R2 bucket name for PMTiles storage"
}

# ── Pelias Tunnel ────────────────────────────────────────────────

variable "pelias_tunnel_secret" {
  type        = string
  sensitive   = true
  description = "Base64-encoded secret for the Pelias Cloudflare Tunnel (min 32 bytes). Generate with: openssl rand -base64 32"
}

variable "r2_data_bucket_name" {
  type        = string
  default     = "crime-map-data"
  description = "Cloudflare R2 bucket name for pipeline data (source DuckDB files, boundaries, geocode cache)"
}
