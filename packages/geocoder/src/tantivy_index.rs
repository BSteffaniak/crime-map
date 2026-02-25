//! Tantivy-based local geocoder using the pre-built address index.
//!
//! Searches a local Tantivy full-text index of US addresses (built from
//! `OpenAddresses` and OpenStreetMap data) to resolve street addresses
//! to coordinates.
//!
//! This provider replaces the Pelias (Elasticsearch) geocoder with an
//! in-process search that requires no external services or Docker
//! containers.

use std::sync::Arc;

use crime_map_geocoder_index::GeocoderIndex;

use crate::{GeocodeError, GeocodedAddress, GeocodingProvider, MatchQuality};

/// A lazily-initialized handle to the Tantivy geocoder index.
///
/// Wraps the index in an `Arc` so it can be shared across concurrent
/// geocoding tasks.
#[derive(Clone)]
pub struct TantivyGeocoder {
    index: Arc<GeocoderIndex>,
}

impl TantivyGeocoder {
    /// Opens the geocoder index from the given directory.
    ///
    /// # Errors
    ///
    /// Returns [`GeocodeError::Parse`] if the index cannot be opened.
    pub fn open(index_dir: &str) -> Result<Self, GeocodeError> {
        let index = GeocoderIndex::open(index_dir).map_err(|e| GeocodeError::Parse {
            message: format!("Failed to open Tantivy geocoder index: {e}"),
        })?;

        Ok(Self {
            index: Arc::new(index),
        })
    }

    /// Opens the geocoder index from the default directory.
    ///
    /// # Errors
    ///
    /// Returns [`GeocodeError::Parse`] if the index cannot be opened.
    pub fn open_default() -> Result<Self, GeocodeError> {
        let dir = crime_map_geocoder_index::default_index_dir();
        Self::open(&dir.display().to_string())
    }
}

/// Checks whether the Tantivy geocoder index is available.
///
/// Returns `true` if the default index directory exists and contains
/// a valid Tantivy index (specifically, a `meta.json` file).
#[must_use]
pub fn is_available() -> bool {
    GeocoderIndex::is_available()
}

/// Geocodes a single free-form address query against the local index.
///
/// The `query` parameter should be in the format `"street, city, state"`
/// (matching the format used by the Pelias and Nominatim providers).
///
/// # Errors
///
/// Returns [`GeocodeError::Parse`] if the search fails.
pub async fn geocode_freeform(
    geocoder: &TantivyGeocoder,
    query: &str,
) -> Result<Option<GeocodedAddress>, GeocodeError> {
    // Parse the "street, city, state" format
    let parts: Vec<&str> = query.splitn(3, ',').collect();
    let (street, city, state) = match parts.len() {
        3 => (parts[0].trim(), parts[1].trim(), parts[2].trim()),
        2 => (parts[0].trim(), parts[1].trim(), ""),
        _ => (query.trim(), "", ""),
    };

    let result = geocoder
        .index
        .search(street, city, state)
        .await
        .map_err(|e| GeocodeError::Parse {
            message: format!("Tantivy search failed: {e}"),
        })?;

    Ok(result.map(|hit| {
        let quality = if hit.score >= crime_map_geocoder_index::query::EXACT_SCORE_THRESHOLD {
            MatchQuality::Exact
        } else {
            MatchQuality::Approximate
        };

        let matched_address = format!(
            "{}, {}, {}",
            hit.matched_street, hit.matched_city, hit.matched_state
        );

        GeocodedAddress {
            latitude: hit.latitude,
            longitude: hit.longitude,
            matched_address: Some(matched_address),
            provider: GeocodingProvider::Tantivy,
            match_quality: quality,
        }
    }))
}
