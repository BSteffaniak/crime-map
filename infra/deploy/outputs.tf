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
