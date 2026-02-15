# ── Fly.io Application ───────────────────────────────────────────

resource "fly_app" "crime_map" {
  name                     = var.fly_app_name
  org                      = var.fly_org
  assign_shared_ip_address = true
}

# ── Persistent Volume ────────────────────────────────────────────

resource "fly_volume" "data" {
  app    = fly_app.crime_map.name
  name   = "crime_map_data"
  region = var.fly_region
  size   = var.fly_volume_size_gb
}

# ── IPv6 Address ─────────────────────────────────────────────────

resource "fly_ip" "ipv6" {
  app  = fly_app.crime_map.name
  type = "v6"
}

# ── TLS Certificate ──────────────────────────────────────────────

resource "fly_cert" "domain" {
  app      = fly_app.crime_map.name
  hostname = var.domain
}
