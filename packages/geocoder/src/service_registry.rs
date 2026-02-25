//! Compile-time registry of geocoding service configurations.
//!
//! Each geocoding provider is defined in a TOML file under `services/`.
//! The registry embeds these at compile time and exposes them via
//! [`all_services`] and [`enabled_services`].

use serde::Deserialize;

/// A geocoding service configuration loaded from TOML.
#[derive(Debug, Clone, Deserialize)]
pub struct GeocodingService {
    /// Unique identifier (e.g., `"census"`, `"pelias"`, `"nominatim"`).
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Whether this service is active in the geocoding pipeline.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Execution order — lower values run first.
    pub priority: u32,
    /// Provider-specific configuration.
    pub provider: ProviderConfig,
}

/// Provider-specific configuration, tagged by `type` in TOML.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderConfig {
    /// US Census Bureau batch geocoder.
    Census {
        /// API base URL (e.g., `"https://geocoding.geo.census.gov/geocoder"`).
        base_url: String,
        /// Benchmark name (e.g., `"Public_AR_Current"`).
        benchmark: String,
        /// Maximum addresses per batch request.
        max_batch_size: usize,
    },
    /// Self-hosted Pelias geocoder.
    Pelias {
        /// API base URL (e.g., `"http://localhost:4000"`).
        base_url: String,
        /// ISO country code for boundary filtering.
        country_code: String,
        /// Number of concurrent HTTP requests to send.
        #[serde(default = "default_concurrent")]
        concurrent_requests: usize,
    },
    /// Tantivy local full-text search index.
    TantivyIndex,
    /// Nominatim / `OpenStreetMap` geocoder.
    Nominatim {
        /// API base URL (e.g., `"https://nominatim.openstreetmap.org/search"`).
        base_url: String,
        /// Minimum delay between requests in milliseconds.
        rate_limit_ms: u64,
    },
}

const fn default_true() -> bool {
    true
}

const fn default_concurrent() -> usize {
    10
}

impl GeocodingService {
    /// Returns the provider's base URL regardless of variant.
    ///
    /// Returns an empty string for providers without a base URL (e.g.,
    /// `TantivyIndex`).
    #[must_use]
    pub fn base_url(&self) -> &str {
        match &self.provider {
            ProviderConfig::Census { base_url, .. }
            | ProviderConfig::Pelias { base_url, .. }
            | ProviderConfig::Nominatim { base_url, .. } => base_url,
            ProviderConfig::TantivyIndex => "",
        }
    }
}

// ── Compile-time embedded TOML files ────────────────────────────────

const SERVICE_TOMLS: &[(&str, &str)] = &[
    ("census", include_str!("../services/census.toml")),
    ("pelias", include_str!("../services/pelias.toml")),
    ("tantivy", include_str!("../services/tantivy_index.toml")),
    ("nominatim", include_str!("../services/nominatim.toml")),
];

#[cfg(test)]
const EXPECTED_SERVICE_COUNT: usize = 4;

/// Returns all geocoding service configurations (enabled and disabled).
///
/// # Panics
///
/// Panics if any TOML config is malformed (this is a compile-time guarantee
/// since the configs are embedded).
#[must_use]
pub fn all_services() -> Vec<GeocodingService> {
    SERVICE_TOMLS
        .iter()
        .map(|(name, toml_str)| {
            toml::de::from_str(toml_str)
                .unwrap_or_else(|e| panic!("Failed to parse geocoding service '{name}': {e}"))
        })
        .collect()
}

/// Returns only enabled services, sorted by priority (ascending).
#[must_use]
pub fn enabled_services() -> Vec<GeocodingService> {
    let mut services: Vec<GeocodingService> =
        all_services().into_iter().filter(|s| s.enabled).collect();
    services.sort_by_key(|s| s.priority);
    services
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn loads_all_services() {
        let services = all_services();
        assert_eq!(services.len(), EXPECTED_SERVICE_COUNT);
    }

    #[test]
    fn service_ids_are_unique() {
        let services = all_services();
        let mut seen = BTreeSet::new();
        for svc in &services {
            assert!(seen.insert(&svc.id), "Duplicate service ID: {}", svc.id);
        }
    }

    #[test]
    fn all_services_have_required_fields() {
        for svc in &all_services() {
            assert!(!svc.id.is_empty(), "Service has empty id");
            assert!(!svc.name.is_empty(), "Service {} has empty name", svc.id);
            // TantivyIndex has no base_url (it's a local index)
            if !matches!(svc.provider, ProviderConfig::TantivyIndex) {
                assert!(
                    !svc.base_url().is_empty(),
                    "Service {} has empty base_url",
                    svc.id
                );
            }
        }
    }

    #[test]
    fn enabled_services_sorted_by_priority() {
        let services = enabled_services();
        for window in services.windows(2) {
            assert!(
                window[0].priority <= window[1].priority,
                "Services not sorted by priority: {} ({}) > {} ({})",
                window[0].id,
                window[0].priority,
                window[1].id,
                window[1].priority
            );
        }
    }
}
