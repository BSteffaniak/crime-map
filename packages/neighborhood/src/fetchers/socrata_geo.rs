//! Socrata `GeoJSON` export fetcher.
//!
//! Fetches neighborhood boundaries from a Socrata open data portal
//! using the `resource.geojson` endpoint with a `$limit` parameter.

use crate::NeighborhoodError;

/// Fetches all features from a Socrata `GeoJSON` endpoint.
///
/// # Errors
///
/// Returns [`NeighborhoodError`] if the request fails or the response
/// cannot be parsed.
pub async fn fetch(
    client: &reqwest::Client,
    url: &str,
    limit: Option<u32>,
) -> Result<Vec<serde_json::Value>, NeighborhoodError> {
    let record_limit = limit.unwrap_or(5000);

    let full_url = if url.contains('?') {
        format!("{url}&$limit={record_limit}")
    } else {
        format!("{url}?$limit={record_limit}")
    };

    let resp = client.get(&full_url).send().await?;
    if !resp.status().is_success() {
        return Err(NeighborhoodError::Conversion {
            message: format!("Socrata request failed with status {}", resp.status()),
        });
    }
    let body = resp.text().await?;

    let json: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| NeighborhoodError::Conversion {
            message: format!("Failed to parse Socrata GeoJSON response: {e}"),
        })?;

    let features = json["features"]
        .as_array()
        .ok_or_else(|| NeighborhoodError::Conversion {
            message: "No features array in Socrata GeoJSON response".to_string(),
        })?;

    Ok(features.clone())
}
