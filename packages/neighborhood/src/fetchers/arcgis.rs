//! `ArcGIS` `FeatureServer` / `MapServer` fetcher.
//!
//! Queries an `ArcGIS` REST endpoint with `f=geojson` to get standard
//! `GeoJSON` output. Handles pagination via `resultOffset` for services
//! with transfer limits.

use crate::NeighborhoodError;

/// Fetches all features from an `ArcGIS` query endpoint.
///
/// Paginates automatically if the server indicates
/// `exceededTransferLimit`.
///
/// # Errors
///
/// Returns [`NeighborhoodError`] if the request fails or the response
/// cannot be parsed.
pub async fn fetch(
    client: &reqwest::Client,
    base_url: &str,
    max_records: Option<u32>,
) -> Result<Vec<serde_json::Value>, NeighborhoodError> {
    let record_count = max_records.unwrap_or(1000);
    let mut all_features = Vec::new();
    let mut offset = 0u32;

    loop {
        let url = format!(
            "{base_url}\
             ?where=1%3D1\
             &outFields=*\
             &f=geojson\
             &returnGeometry=true\
             &resultRecordCount={record_count}\
             &resultOffset={offset}"
        );

        let resp = client.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(NeighborhoodError::Conversion {
                message: format!("ArcGIS request failed with status {}", resp.status()),
            });
        }
        let body = resp.text().await?;

        let json: serde_json::Value =
            serde_json::from_str(&body).map_err(|e| NeighborhoodError::Conversion {
                message: format!("Failed to parse ArcGIS response: {e}"),
            })?;

        // Check for API error
        if json.get("error").is_some() {
            return Err(NeighborhoodError::Conversion {
                message: format!(
                    "ArcGIS API error: {}",
                    json["error"]["message"].as_str().unwrap_or("unknown error")
                ),
            });
        }

        let features =
            json["features"]
                .as_array()
                .ok_or_else(|| NeighborhoodError::Conversion {
                    message: "No features array in ArcGIS response".to_string(),
                })?;

        if features.is_empty() {
            break;
        }

        all_features.extend(features.iter().cloned());

        // Check if there are more pages
        let exceeded = json["exceededTransferLimit"].as_bool().unwrap_or(false);
        if !exceeded {
            break;
        }

        #[allow(clippy::cast_possible_truncation)]
        {
            offset += features.len() as u32;
        }
    }

    Ok(all_features)
}
