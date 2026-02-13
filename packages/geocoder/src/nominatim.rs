//! Nominatim / OpenStreetMap geocoder client.
//!
//! Used as a fallback when the Census Bureau geocoder fails to match.
//! Nominatim has strict rate limits: **1 request per second** maximum.
//!
//! See <https://nominatim.org/release-docs/develop/api/Search/>

use crate::{GeocodeError, GeocodedAddress, GeocodingProvider, MatchQuality};

/// Geocodes a single address using the Nominatim structured search endpoint.
///
/// The caller is responsible for rate limiting (typically 1 request per
/// second for the public instance; see `rate_limit_ms` in the service
/// TOML configuration).
///
/// # Errors
///
/// Returns [`GeocodeError`] if the HTTP request or response parsing fails.
pub async fn geocode_single(
    client: &reqwest::Client,
    base_url: &str,
    street: &str,
    city: &str,
    state: &str,
) -> Result<Option<GeocodedAddress>, GeocodeError> {
    let resp = client
        .get(base_url)
        .query(&[
            ("street", street),
            ("city", city),
            ("state", state),
            ("countrycodes", "us"),
            ("format", "jsonv2"),
            ("limit", "1"),
        ])
        .send()
        .await?;

    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(GeocodeError::RateLimited);
    }

    let body: serde_json::Value = resp.json().await?;
    parse_response(&body)
}

/// Geocodes a free-form query (e.g., intersection) using Nominatim.
///
/// The caller is responsible for rate limiting (see `rate_limit_ms` in the
/// service TOML configuration).
///
/// # Errors
///
/// Returns [`GeocodeError`] if the HTTP request or response parsing fails.
pub async fn geocode_freeform(
    client: &reqwest::Client,
    base_url: &str,
    query: &str,
) -> Result<Option<GeocodedAddress>, GeocodeError> {
    let resp = client
        .get(base_url)
        .query(&[
            ("q", query),
            ("countrycodes", "us"),
            ("format", "jsonv2"),
            ("limit", "1"),
        ])
        .send()
        .await?;

    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(GeocodeError::RateLimited);
    }

    let body: serde_json::Value = resp.json().await?;
    parse_response(&body)
}

/// Parses Nominatim JSON response.
fn parse_response(body: &serde_json::Value) -> Result<Option<GeocodedAddress>, GeocodeError> {
    let results = body.as_array().ok_or_else(|| GeocodeError::Parse {
        message: "Nominatim response is not an array".to_string(),
    })?;

    let Some(first) = results.first() else {
        return Ok(None);
    };

    let lat = first["lat"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| GeocodeError::Parse {
            message: "Missing lat in Nominatim response".to_string(),
        })?;

    let lon = first["lon"]
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| GeocodeError::Parse {
            message: "Missing lon in Nominatim response".to_string(),
        })?;

    let display_name = first["display_name"].as_str().map(String::from);

    Ok(Some(GeocodedAddress {
        latitude: lat,
        longitude: lon,
        matched_address: display_name,
        provider: GeocodingProvider::Nominatim,
        match_quality: MatchQuality::Approximate,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nominatim_result() {
        let body = serde_json::json!([{
            "lat": "41.8827",
            "lon": "-87.6278",
            "display_name": "100, North State Street, Chicago, IL, USA"
        }]);
        let result = parse_response(&body).unwrap().unwrap();
        assert!((result.latitude - 41.8827).abs() < 1e-4);
        assert!((result.longitude - -87.6278).abs() < 1e-4);
        assert_eq!(result.provider, GeocodingProvider::Nominatim);
    }

    #[test]
    fn parses_nominatim_empty() {
        let body = serde_json::json!([]);
        assert!(parse_response(&body).unwrap().is_none());
    }
}
