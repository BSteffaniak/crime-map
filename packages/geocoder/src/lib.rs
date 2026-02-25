#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Geocoding service for crime map data.
//!
//! Converts street addresses to latitude/longitude coordinates using a
//! multi-provider strategy configured via TOML files in `services/`:
//!
//! 1. **US Census Bureau Batch Geocoder** (priority 1) — free, no API key,
//!    up to 10,000 addresses per batch request.
//! 2. **Pelias** (priority 2) — self-hosted, no rate limit, concurrent
//!    requests. Requires a running Pelias instance.
//! 3. **Nominatim / OpenStreetMap** (priority 3) — free, 1 req/sec rate
//!    limit.
//!
//! Providers are loaded from the [`service_registry`] and executed in
//! priority order.  Unreachable providers (e.g., Pelias when no instance
//! is running) are skipped automatically.
//!
//! Also provides address cleaning utilities for normalizing block-level
//! addresses from crime data sources.

pub mod address;
pub mod census;
pub mod nominatim;
pub mod pelias;
pub mod service_registry;
pub mod tantivy_index;

use thiserror::Error;

/// A geocoding result with coordinates and metadata.
#[derive(Debug, Clone)]
pub struct GeocodedAddress {
    /// Latitude (WGS84).
    pub latitude: f64,
    /// Longitude (WGS84).
    pub longitude: f64,
    /// The matched/canonical address returned by the geocoder.
    pub matched_address: Option<String>,
    /// Which provider resolved this address.
    pub provider: GeocodingProvider,
    /// Whether this was an exact or approximate match.
    pub match_quality: MatchQuality,
}

/// Which geocoding provider resolved an address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeocodingProvider {
    /// US Census Bureau Geocoder.
    Census,
    /// Self-hosted Pelias geocoder.
    Pelias,
    /// Tantivy local geocoder index.
    Tantivy,
    /// Nominatim / OpenStreetMap.
    Nominatim,
}

/// Quality of the geocoding match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchQuality {
    /// Exact address match.
    Exact,
    /// Approximate / non-exact match.
    Approximate,
}

/// An address to be geocoded, with all available context.
#[derive(Debug, Clone)]
pub struct AddressInput {
    /// Unique identifier for correlating results back to the source record.
    pub id: String,
    /// Street address (e.g., "100 N STATE ST").
    pub street: String,
    /// City name.
    pub city: String,
    /// Two-letter state abbreviation.
    pub state: String,
    /// ZIP code, if available.
    pub zip: Option<String>,
}

/// Errors from geocoding operations.
#[derive(Debug, Error)]
pub enum GeocodeError {
    /// HTTP request failed.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// Response parsing failed.
    #[error("Parse error: {message}")]
    Parse {
        /// Description of the parsing failure.
        message: String,
    },

    /// Rate limit exceeded.
    #[error("Rate limit exceeded")]
    RateLimited,
}

/// Result of a batch geocoding operation.
#[derive(Debug, Clone)]
pub struct BatchResult {
    /// Successfully geocoded addresses.
    pub matched: Vec<(String, GeocodedAddress)>,
    /// IDs of addresses that could not be matched.
    pub unmatched: Vec<String>,
}
