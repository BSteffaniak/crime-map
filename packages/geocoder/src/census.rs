//! US Census Bureau Geocoder client.
//!
//! Supports both single-address and batch geocoding via the Census Bureau's
//! free geocoding service. No API key required.
//!
//! - Single: `GET /geocoder/locations/address`
//! - Batch: `POST /geocoder/locations/addressbatch` (up to 10,000 rows)
//!
//! See <https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html>

use std::fmt::Write as _;

use reqwest::multipart;

use crate::{
    AddressInput, BatchResult, GeocodeError, GeocodedAddress, GeocodingProvider, MatchQuality,
};

/// Maximum number of addresses per batch request (Census Bureau limit).
pub const MAX_BATCH_SIZE: usize = 10_000;

/// Geocodes a single address using the Census Bureau structured endpoint.
///
/// # Errors
///
/// Returns [`GeocodeError`] if the HTTP request or response parsing fails.
pub async fn geocode_single(
    client: &reqwest::Client,
    base_url: &str,
    benchmark: &str,
    street: &str,
    city: &str,
    state: &str,
    zip: Option<&str>,
) -> Result<Option<GeocodedAddress>, GeocodeError> {
    let mut url = format!(
        "{base_url}/locations/address\
         ?street={street}\
         &city={city}\
         &state={state}\
         &benchmark={benchmark}\
         &format=json",
        street = urlencoding(street),
        city = urlencoding(city),
        state = urlencoding(state),
    );

    if let Some(z) = zip {
        write!(url, "&zip={}", urlencoding(z)).unwrap();
    }

    let resp = client.get(&url).send().await?;
    let body: serde_json::Value = resp.json().await?;

    parse_single_response(&body)
}

/// Geocodes a batch of addresses using the Census Bureau batch endpoint.
///
/// Addresses are submitted as a CSV file via `multipart/form-data`.
/// The batch endpoint accepts up to [`MAX_BATCH_SIZE`] addresses.
///
/// # Errors
///
/// Returns [`GeocodeError`] if the HTTP request or response parsing fails.
pub async fn geocode_batch(
    client: &reqwest::Client,
    base_url: &str,
    benchmark: &str,
    addresses: &[AddressInput],
) -> Result<BatchResult, GeocodeError> {
    if addresses.is_empty() {
        return Ok(BatchResult {
            matched: Vec::new(),
            unmatched: Vec::new(),
        });
    }

    // Build CSV content (no header row per Census spec)
    let mut csv_content = String::new();
    for addr in addresses {
        // Format: UniqueID, Street, City, State, ZIP
        writeln!(
            csv_content,
            "\"{}\",\"{}\",\"{}\",\"{}\",\"{}\"",
            escape_csv(&addr.id),
            escape_csv(&addr.street),
            escape_csv(&addr.city),
            escape_csv(&addr.state),
            addr.zip.as_deref().unwrap_or(""),
        )
        .unwrap();
    }

    let form = multipart::Form::new()
        .text("benchmark", benchmark.to_string())
        .part(
            "addressFile",
            multipart::Part::text(csv_content)
                .file_name("addresses.csv")
                .mime_str("text/csv")
                .map_err(|e| GeocodeError::Parse {
                    message: format!("Failed to set MIME type: {e}"),
                })?,
        );

    let url = format!("{base_url}/locations/addressbatch");
    let resp = client
        .post(&url)
        .multipart(form)
        .timeout(std::time::Duration::from_secs(180))
        .send()
        .await?;

    let body = resp.text().await?;
    Ok(parse_batch_response(&body))
}

/// Parses the JSON response from the single-address endpoint.
fn parse_single_response(
    body: &serde_json::Value,
) -> Result<Option<GeocodedAddress>, GeocodeError> {
    let matches =
        body["result"]["addressMatches"]
            .as_array()
            .ok_or_else(|| GeocodeError::Parse {
                message: "Missing addressMatches array".to_string(),
            })?;

    let Some(first) = matches.first() else {
        return Ok(None);
    };

    let x = first["coordinates"]["x"]
        .as_f64()
        .ok_or_else(|| GeocodeError::Parse {
            message: "Missing x coordinate".to_string(),
        })?;
    let y = first["coordinates"]["y"]
        .as_f64()
        .ok_or_else(|| GeocodeError::Parse {
            message: "Missing y coordinate".to_string(),
        })?;

    let matched_address = first["matchedAddress"].as_str().map(String::from);

    Ok(Some(GeocodedAddress {
        latitude: y,
        longitude: x,
        matched_address,
        provider: GeocodingProvider::Census,
        match_quality: MatchQuality::Exact,
    }))
}

/// Parses the CSV response from the batch endpoint.
///
/// Response format (one row per input):
/// ```text
/// "ID","InputAddress","Match"|"No_Match","Exact"|"Non_Exact","MatchedAddr","lng,lat","TigerLine","Side"
/// ```
fn parse_batch_response(body: &str) -> BatchResult {
    let mut matched = Vec::new();
    let mut unmatched = Vec::new();

    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        // Parse CSV fields (simple parser for quoted fields)
        let fields = parse_csv_line(line);

        if fields.len() < 3 {
            continue;
        }

        let id = fields[0].clone();

        if fields[2] == "Match" && fields.len() >= 6 {
            let quality = if fields.get(3).is_some_and(|f| f == "Exact") {
                MatchQuality::Exact
            } else {
                MatchQuality::Approximate
            };

            let matched_address = fields.get(4).cloned();
            let coords_str = fields.get(5).map_or("", String::as_str);

            if let Some((lng, lat)) = parse_coord_pair(coords_str) {
                matched.push((
                    id,
                    GeocodedAddress {
                        latitude: lat,
                        longitude: lng,
                        matched_address,
                        provider: GeocodingProvider::Census,
                        match_quality: quality,
                    },
                ));
            } else {
                unmatched.push(id);
            }
        } else {
            unmatched.push(id);
        }
    }

    BatchResult { matched, unmatched }
}

/// Parses a "lng,lat" coordinate pair from Census batch response.
fn parse_coord_pair(s: &str) -> Option<(f64, f64)> {
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 2 {
        return None;
    }
    let lng = parts[0].trim().parse::<f64>().ok()?;
    let lat = parts[1].trim().parse::<f64>().ok()?;
    Some((lng, lat))
}

/// Simple CSV line parser that handles quoted fields.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '"' {
            if in_quotes {
                // Check for escaped quote ("")
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                in_quotes = true;
            }
        } else if ch == ',' && !in_quotes {
            fields.push(current.trim().to_string());
            current = String::new();
        } else {
            current.push(ch);
        }
    }
    fields.push(current.trim().to_string());
    fields
}

/// Simple percent-encoding for URL query parameters.
fn urlencoding(s: &str) -> String {
    s.replace(' ', "+")
        .replace('&', "%26")
        .replace('#', "%23")
        .replace('?', "%3F")
        .replace('/', "%2F")
}

/// Escapes a string for CSV output (doubles any internal quotes).
fn escape_csv(s: &str) -> String {
    s.replace('"', "\"\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_match() {
        let body = serde_json::json!({
            "result": {
                "addressMatches": [{
                    "coordinates": { "x": -76.927_487, "y": 38.846_016 },
                    "matchedAddress": "4600 SILVER HILL RD, WASHINGTON, DC, 20233"
                }]
            }
        });
        let result = parse_single_response(&body).unwrap().unwrap();
        assert!((result.longitude - -76.927_487).abs() < 1e-6);
        assert!((result.latitude - 38.846_016).abs() < 1e-6);
    }

    #[test]
    fn parses_single_no_match() {
        let body = serde_json::json!({
            "result": { "addressMatches": [] }
        });
        assert!(parse_single_response(&body).unwrap().is_none());
    }

    #[test]
    fn parses_batch_response() {
        let csv = r#""1","100 Main St, Chicago, IL, ","Match","Exact","100 MAIN ST, CHICAGO, IL, 60601","-87.627,41.882","12345","L"
"2","Unknown, , , ","No_Match"
"3","200 Oak Ave, Dallas, TX, ","Match","Non_Exact","200 OAK AVE, DALLAS, TX, 75201","-96.795,32.780","67890","R"
"#;
        let result = parse_batch_response(csv);
        assert_eq!(result.matched.len(), 2);
        assert_eq!(result.unmatched.len(), 1);
        assert_eq!(result.matched[0].0, "1");
        assert_eq!(result.matched[0].1.match_quality, MatchQuality::Exact);
        assert_eq!(result.matched[1].1.match_quality, MatchQuality::Approximate);
        assert_eq!(result.unmatched[0], "2");
    }

    #[test]
    fn parses_coord_pair() {
        assert_eq!(parse_coord_pair("-87.627,41.882"), Some((-87.627, 41.882)));
        assert_eq!(parse_coord_pair("invalid"), None);
        assert_eq!(parse_coord_pair(""), None);
    }

    #[test]
    fn parses_csv_with_quotes() {
        let fields = parse_csv_line(r#""hello","world","foo,bar""#);
        assert_eq!(fields, vec!["hello", "world", "foo,bar"]);
    }
}
