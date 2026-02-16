terraform {
  required_version = ">= 1.6.0"

  cloud {
    hostname     = "app.terraform.io"
    organization = "opencrimemap"
    workspaces {
      name = "crime-map-deploy"
    }
  }

  required_providers {
    fly = {
      source  = "andrewbaxter/fly"
      version = "~> 0.1"
    }
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 5.0"
    }
  }
}

# ── Providers ────────────────────────────────────────────────────

provider "fly" {
  fly_api_token = var.fly_api_token
}

provider "cloudflare" {
  api_token = var.cloudflare_api_token
}
