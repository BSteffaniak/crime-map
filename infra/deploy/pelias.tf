# ── Cloudflare Tunnel for Pelias Geocoder ────────────────────────
# Exposes a locally-run Pelias instance to CI via a secure tunnel.
# The tunnel connects outbound from the local machine to Cloudflare's
# edge — no inbound ports need to be opened.

resource "cloudflare_zero_trust_tunnel_cloudflared" "pelias" {
  account_id    = var.cloudflare_account_id
  name          = "pelias-geocoder"
  config_src    = "cloudflare"
  tunnel_secret = var.pelias_tunnel_secret
}

locals {
  # Construct the full TUNNEL_TOKEN that cloudflared expects.
  # Format: base64({"a": <account_id>, "t": <tunnel_id>, "s": <secret>})
  pelias_tunnel_token = base64encode(jsonencode({
    a = var.cloudflare_account_id
    t = cloudflare_zero_trust_tunnel_cloudflared.pelias.id
    s = var.pelias_tunnel_secret
  }))
}

resource "cloudflare_zero_trust_tunnel_cloudflared_config" "pelias" {
  account_id = var.cloudflare_account_id
  tunnel_id  = cloudflare_zero_trust_tunnel_cloudflared.pelias.id

  config = {
    ingress = [
      {
        hostname = "pelias.${var.domain}"
        service  = "http://pelias_api:4000"
      },
      {
        # Catch-all rule (required by Cloudflare)
        service = "http_status:404"
      },
    ]
  }
}

# ── DNS: pelias subdomain -> tunnel ──────────────────────────────

resource "cloudflare_dns_record" "pelias" {
  zone_id = local.zone_id
  name    = "pelias"
  type    = "CNAME"
  content = "${cloudflare_zero_trust_tunnel_cloudflared.pelias.id}.cfargotunnel.com"
  proxied = true
  ttl     = 1
}

# ── Access: restrict Pelias to CI service tokens only ────────────

resource "cloudflare_zero_trust_access_service_token" "pelias_ci" {
  account_id = var.cloudflare_account_id
  name       = "pelias-ci-token"
  duration   = "8760h"
}

resource "cloudflare_zero_trust_access_application" "pelias" {
  account_id       = var.cloudflare_account_id
  name             = "Pelias Geocoder"
  type             = "self_hosted"
  session_duration = "24h"

  destinations = [{
    type = "public"
    uri  = "pelias.${var.domain}"
  }]

  policies = [{
    name     = "Allow CI service token"
    decision = "non_identity"
    precedence = 1
    include = [{
      service_token = {
        token_id = cloudflare_zero_trust_access_service_token.pelias_ci.id
      }
    }]
  }]
}
