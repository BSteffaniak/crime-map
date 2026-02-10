//! Fetcher dispatch and implementations for different API types.
//!
//! Each fetcher downloads raw `GeoJSON` `FeatureCollection` data from a
//! city's open data portal.

pub mod arcgis;
pub mod geojson_url;
pub mod socrata_geo;

use crime_map_neighborhood_models::{NeighborhoodFetcherConfig, NeighborhoodSource};

use crate::NeighborhoodError;

/// Fetches raw `GeoJSON` features from the source's API.
///
/// Returns the parsed `GeoJSON` `FeatureCollection` as a
/// `serde_json::Value` containing a `features` array.
///
/// # Errors
///
/// Returns [`NeighborhoodError`] if the HTTP request or response
/// parsing fails.
pub async fn fetch_features(
    client: &reqwest::Client,
    source: &NeighborhoodSource,
) -> Result<Vec<serde_json::Value>, NeighborhoodError> {
    match &source.fetcher {
        NeighborhoodFetcherConfig::Arcgis { url, max_records } => {
            arcgis::fetch(client, url, *max_records).await
        }
        NeighborhoodFetcherConfig::SocrataGeo { url, limit } => {
            socrata_geo::fetch(client, url, *limit).await
        }
        NeighborhoodFetcherConfig::GeojsonUrl { url } => geojson_url::fetch(client, url).await,
    }
}
