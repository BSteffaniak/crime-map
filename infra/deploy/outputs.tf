# ── Fly.io Outputs ───────────────────────────────────────────────

output "fly_app_name" {
  value       = fly_app.crime_map.name
  description = "Fly.io application name"
}

output "fly_app_url" {
  value       = "https://${var.fly_app_name}.fly.dev"
  description = "Default Fly.io application URL"
}

output "fly_shared_ipv4" {
  value       = fly_app.crime_map.shared_ip_address
  description = "Shared IPv4 address assigned to the app"
}

output "fly_ipv6" {
  value       = fly_ip.ipv6.address
  description = "Dedicated IPv6 anycast address"
}

output "fly_volume_id" {
  value       = fly_volume.data.id
  description = "Fly volume ID for crime map data"
}

output "fly_cert_validation" {
  value       = fly_cert.domain.dns_validation_instructions
  description = "DNS validation instructions for the TLS certificate"
}

# ── Cloudflare Outputs ───────────────────────────────────────────

output "r2_bucket_name" {
  value       = cloudflare_r2_bucket.tiles.name
  description = "R2 bucket name for PMTiles"
}

output "app_domain" {
  value       = "https://${var.domain}"
  description = "Application URL via custom domain"
}

# ── Pelias Tunnel Outputs ────────────────────────────────────────

output "pelias_tunnel_token" {
  value       = local.pelias_tunnel_token
  sensitive   = true
  description = "Full TUNNEL_TOKEN for cloudflared. Paste into infra/pelias/.env"
}

output "pelias_url" {
  value       = "https://pelias.${var.domain}"
  description = "Public URL for the Pelias geocoder via Cloudflare Tunnel"
}

output "pelias_cf_access_client_id" {
  value       = cloudflare_zero_trust_access_service_token.pelias_ci.client_id
  description = "CF-Access-Client-Id header value for CI"
}

output "pelias_cf_access_client_secret" {
  value       = cloudflare_zero_trust_access_service_token.pelias_ci.client_secret
  sensitive   = true
  description = "CF-Access-Client-Secret header value for CI"
}
