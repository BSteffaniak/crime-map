//! Direct `GeoJSON` URL fetcher.
//!
//! Fetches a standard `GeoJSON` `FeatureCollection` from any URL that
//! returns it directly.

use crate::NeighborhoodError;

/// Fetches all features from a direct `GeoJSON` URL.
///
/// # Errors
///
/// Returns [`NeighborhoodError`] if the request fails or the response
/// cannot be parsed.
pub async fn fetch(
    client: &reqwest::Client,
    url: &str,
) -> Result<Vec<serde_json::Value>, NeighborhoodError> {
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        return Err(NeighborhoodError::Conversion {
            message: format!("GeoJSON request failed with status {}", resp.status()),
        });
    }
    let body = resp.text().await?;

    let json: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| NeighborhoodError::Conversion {
            message: format!("Failed to parse GeoJSON response: {e}"),
        })?;

    let features = json["features"]
        .as_array()
        .ok_or_else(|| NeighborhoodError::Conversion {
            message: "No features array in GeoJSON response".to_string(),
        })?;

    Ok(features.clone())
}
