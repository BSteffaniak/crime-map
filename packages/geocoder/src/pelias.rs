//! Pelias geocoder client for self-hosted instances.
//!
//! Pelias exposes a `/v1/search` endpoint that accepts free-form text
//! queries and returns `GeoJSON` `FeatureCollection` responses.
//!
//! Since the instance is self-hosted there are no external rate limits;
//! the caller controls concurrency via `concurrent_requests` in the
//! service TOML configuration.
//!
//! See <https://github.com/pelias/documentation/blob/master/search.md>

use crate::{GeocodeError, GeocodedAddress, GeocodingProvider, MatchQuality};

/// Geocodes a single free-form address query against a Pelias instance.
///
/// # Errors
///
/// Returns [`GeocodeError`] if the HTTP request or response parsing fails.
pub async fn geocode_freeform(
    client: &reqwest::Client,
    base_url: &str,
    country_code: &str,
    query: &str,
) -> Result<Option<GeocodedAddress>, GeocodeError> {
    let url = format!("{base_url}/v1/search");

    let resp = client
        .get(&url)
        .query(&[
            ("text", query),
            ("boundary.country", country_code),
            ("size", "1"),
        ])
        .send()
        .await?;

    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(GeocodeError::RateLimited);
    }

    if !resp.status().is_success() {
        return Err(GeocodeError::Parse {
            message: format!("Pelias returned status {}", resp.status()),
        });
    }

    let body: serde_json::Value = resp.json().await?;
    parse_response(&body)
}

/// Checks whether a Pelias instance is reachable.
///
/// Issues a lightweight `GET /v1` request and returns `true` if the
/// server responds with any successful status code within 3 seconds.
pub async fn is_available(client: &reqwest::Client, base_url: &str) -> bool {
    let url = format!("{base_url}/v1");
    client
        .get(&url)
        .timeout(std::time::Duration::from_secs(3))
        .send()
        .await
        .is_ok_and(|r| r.status().is_success())
}

/// Parses a Pelias `GeoJSON` `FeatureCollection` response.
fn parse_response(body: &serde_json::Value) -> Result<Option<GeocodedAddress>, GeocodeError> {
    let features = body
        .get("features")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| GeocodeError::Parse {
            message: "Pelias response missing 'features' array".to_string(),
        })?;

    let Some(first) = features.first() else {
        return Ok(None);
    };

    let coords = first
        .pointer("/geometry/coordinates")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| GeocodeError::Parse {
            message: "Feature missing geometry.coordinates".to_string(),
        })?;

    if coords.len() < 2 {
        return Err(GeocodeError::Parse {
            message: "coordinates array has fewer than 2 elements".to_string(),
        });
    }

    let lng = coords[0].as_f64().ok_or_else(|| GeocodeError::Parse {
        message: "longitude is not a number".to_string(),
    })?;
    let lat = coords[1].as_f64().ok_or_else(|| GeocodeError::Parse {
        message: "latitude is not a number".to_string(),
    })?;

    let label = first
        .pointer("/properties/label")
        .and_then(serde_json::Value::as_str)
        .map(String::from);

    let confidence = first
        .pointer("/properties/confidence")
        .and_then(serde_json::Value::as_f64);

    let quality = confidence.map_or(MatchQuality::Approximate, |c| {
        if c >= 0.9 {
            MatchQuality::Exact
        } else {
            MatchQuality::Approximate
        }
    });

    Ok(Some(GeocodedAddress {
        latitude: lat,
        longitude: lng,
        matched_address: label,
        provider: GeocodingProvider::Pelias,
        match_quality: quality,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pelias_feature() {
        let body = serde_json::json!({
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [-77.0364, 38.8951]
                },
                "properties": {
                    "label": "1600 Pennsylvania Ave NW, Washington, DC, USA",
                    "confidence": 0.95
                }
            }]
        });
        let result = parse_response(&body).unwrap().unwrap();
        assert!((result.latitude - 38.8951).abs() < 1e-4);
        assert!((result.longitude - -77.0364).abs() < 1e-4);
        assert_eq!(result.provider, GeocodingProvider::Pelias);
        assert_eq!(result.match_quality, MatchQuality::Exact);
        assert_eq!(
            result.matched_address.as_deref(),
            Some("1600 Pennsylvania Ave NW, Washington, DC, USA")
        );
    }

    #[test]
    fn parses_pelias_empty() {
        let body = serde_json::json!({
            "type": "FeatureCollection",
            "features": []
        });
        assert!(parse_response(&body).unwrap().is_none());
    }

    #[test]
    fn parses_pelias_low_confidence() {
        let body = serde_json::json!({
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [-87.6278, 41.8827]
                },
                "properties": {
                    "label": "100 N State St, Chicago, IL",
                    "confidence": 0.5
                }
            }]
        });
        let result = parse_response(&body).unwrap().unwrap();
        assert_eq!(result.match_quality, MatchQuality::Approximate);
    }
}
