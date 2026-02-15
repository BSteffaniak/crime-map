# ── Cloudflare Zone (data source) ────────────────────────────────
# The zone must already exist in Cloudflare (added via dashboard).
# We look it up by domain name to get the zone ID.

data "cloudflare_zones" "domain" {
  name = var.domain
}

locals {
  zone_id = data.cloudflare_zones.domain.result[0].id
}

# ── R2 Bucket for PMTiles ────────────────────────────────────────

resource "cloudflare_r2_bucket" "tiles" {
  account_id = var.cloudflare_account_id
  name       = var.r2_bucket_name
  location   = "enam"
}

resource "cloudflare_r2_bucket_cors" "tiles" {
  account_id  = var.cloudflare_account_id
  bucket_name = cloudflare_r2_bucket.tiles.name

  rules = [{
    id              = "Allow crime-map origins"
    max_age_seconds = 86400
    allowed = {
      methods = ["GET", "HEAD"]
      origins = [
        "https://${var.fly_app_name}.fly.dev",
        "https://${var.domain}",
        "http://localhost:5173",
      ]
      headers = ["Range", "If-Match"]
    }
    expose_headers = ["Content-Length", "Content-Range", "ETag"]
  }]
}

# ── DNS: Root domain -> Fly.io ───────────────────────────────────
# DNS-only (not proxied) — Fly.io handles TLS termination directly.
# This avoids Cloudflare SSL handshake issues and allows Fly to
# complete ACME cert validation without interference.

resource "cloudflare_dns_record" "app" {
  zone_id = local.zone_id
  name    = var.domain
  type    = "CNAME"
  content = "${var.fly_app_name}.fly.dev"
  proxied = false
  ttl     = 300
}

# ── DNS: www -> Fly.io ────────────────────────────────────────────

resource "cloudflare_dns_record" "www" {
  zone_id = local.zone_id
  name    = "www"
  type    = "CNAME"
  content = "${var.fly_app_name}.fly.dev"
  proxied = false
  ttl     = 300
}
