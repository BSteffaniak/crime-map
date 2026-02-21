//! `CityProtect` (Motorola `CommandCentral`) incident API fetcher.
//!
//! `CityProtect` is a public-facing crime mapping platform used by hundreds
//! of police agencies. The API uses a POST-based JSON endpoint with
//! geographic bounding-box filtering and mandatory `Origin`/`Referer`
//! headers.
//!
//! Key constraints:
//! - Maximum date range per request: **364 days** (API says "12 months")
//! - Pagination: `limit`/`offset` in the POST body; stop when the
//!   `incidents` array is empty
//! - Response records are nested inside `result.list.incidents`
//! - Coordinates are `GeoJSON` `[lng, lat]` order

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use serde_json::json;
use tokio::sync::mpsc;

use crate::progress::ProgressCallback;
use crate::{FetchOptions, SourceError};

/// Maximum date range the `CityProtect` API accepts per request. The API
/// enforces a "max 12 months" rule; 364 days provides a safety margin.
const MAX_WINDOW_DAYS: i64 = 364;

/// Default page size (`CityProtect`'s own frontend uses 2000).
const DEFAULT_PAGE_SIZE: u64 = 2000;

/// How far back to fetch when no `since` date is specified (~12 months
/// with a 1-day safety margin against the API's calendar-month check).
const DEFAULT_LOOKBACK_DAYS: i64 = 364;

/// All parent incident type IDs used by the `CityProtect` frontend.
const ALL_INCIDENT_TYPE_IDS: &str =
    "149,150,148,8,97,104,165,98,100,179,178,180,101,99,103,163,168,166,12,161,14,16,15,151,169";

/// Configuration for a `CityProtect` fetch operation.
pub struct CityProtectConfig<'a> {
    /// `CityProtect` API endpoint URL.
    pub api_url: &'a str,
    /// Bounding box `[west, south, east, north]` for the agency's jurisdiction.
    pub bbox: &'a [f64; 4],
    /// `CityProtect` agency ID (e.g., `"381"`).
    pub agency_id: &'a str,
    /// Records per page (typically 2000).
    pub page_size: u64,
    /// Override incident type IDs filter (default: all types).
    pub incident_type_ids: Option<&'a str>,
    /// Label for log messages (e.g., `"Prince George's County PD"`).
    pub label: &'a str,
}

/// Builds the `GeoJSON` Polygon from a `[west, south, east, north]` bounding
/// box.
fn build_geojson_polygon(bbox: &[f64; 4]) -> serde_json::Value {
    let [west, south, east, north] = *bbox;
    json!({
        "type": "Polygon",
        "coordinates": [[
            [west, south],
            [east, south],
            [east, north],
            [west, north],
            [west, south],
        ]]
    })
}

/// Builds the POST request body for the incidents endpoint.
fn build_request_body(
    config: &CityProtectConfig<'_>,
    from_date: &DateTime<Utc>,
    to_date: &DateTime<Utc>,
    offset: u64,
) -> serde_json::Value {
    let geojson = build_geojson_polygon(config.bbox);
    let center_lat = f64::midpoint(config.bbox[1], config.bbox[3]);
    let center_lng = f64::midpoint(config.bbox[0], config.bbox[2]);
    let type_ids = config.incident_type_ids.unwrap_or(ALL_INCIDENT_TYPE_IDS);
    let page_size_str = config.page_size.to_string();

    json!({
        "limit": config.page_size,
        "offset": offset,
        "geoJson": geojson,
        "projection": false,
        "propertyMap": {
            "toDate": to_date.format("%Y-%m-%dT%H:%M:%S%.3f+00:00").to_string(),
            "fromDate": from_date.format("%Y-%m-%dT%H:%M:%S%.3f+00:00").to_string(),
            "pageSize": page_size_str,
            "parentIncidentTypeIds": type_ids,
            "zoomLevel": "11",
            "latitude": format!("{center_lat:.6}"),
            "longitude": format!("{center_lng:.6}"),
            "days": "1,2,3,4,5,6,7",
            "startHour": "0",
            "endHour": "24",
            "timezone": "+00:00",
            "relativeDate": "custom",
            "agencyIds": config.agency_id,
        }
    })
}

/// Flattens a `CityProtect` incident record into a top-level JSON object
/// suitable for field mapping. Extracts coordinates from the nested
/// `location.coordinates` array.
fn flatten_incident(incident: &serde_json::Value) -> Option<serde_json::Value> {
    let obj = incident.as_object()?;
    let mut flat = serde_json::Map::new();

    for (key, value) in obj {
        if key == "location" {
            // Extract GeoJSON Point coordinates [lng, lat]
            if let Some(coords) = value.get("coordinates").and_then(|c| c.as_array())
                && let (Some(lng), Some(lat)) = (
                    coords.first().and_then(serde_json::Value::as_f64),
                    coords.get(1).and_then(serde_json::Value::as_f64),
                )
            {
                flat.insert(
                    "_city_protect_lat".to_string(),
                    serde_json::Value::from(lat),
                );
                flat.insert(
                    "_city_protect_lng".to_string(),
                    serde_json::Value::from(lng),
                );
            }
        } else if key == "narrative" {
            // Strip HTML tags and control characters from narratives
            let cleaned = value
                .as_str()
                .map(|s| {
                    s.replace("<br />", " ")
                        .replace("<br/>", " ")
                        .replace("<br>", " ")
                        .chars()
                        .filter(|c| !c.is_control() || *c == '\n')
                        .collect::<String>()
                })
                .unwrap_or_default();
            flat.insert(key.clone(), serde_json::Value::from(cleaned));
        } else {
            flat.insert(key.clone(), value.clone());
        }
    }

    Some(serde_json::Value::Object(flat))
}

/// Fetches incidents from the `CityProtect` API using time-windowed
/// pagination. The API enforces a 365-day maximum per request, so this
/// function automatically splits longer ranges into windows.
///
/// Returns the total number of records fetched.
///
/// # Panics
///
/// Panics if the constant lookback or window duration cannot be
/// represented as a `chrono::Duration` (should never happen in practice).
///
/// # Errors
///
/// Returns [`SourceError`] if HTTP requests or JSON parsing fails.
#[allow(clippy::too_many_lines)]
pub async fn fetch_city_protect(
    config: &CityProtectConfig<'_>,
    options: &FetchOptions,
    tx: &mpsc::Sender<Vec<serde_json::Value>>,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<u64, SourceError> {
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        .build()?;

    let fetch_limit = options.limit.unwrap_or(u64::MAX);
    let page_size = if config.page_size == 0 {
        DEFAULT_PAGE_SIZE
    } else {
        config.page_size
    };

    // ── Determine date range ─────────────────────────────────────────
    let to_date = Utc::now();
    let from_date = options.since.unwrap_or_else(|| {
        to_date - Duration::try_days(DEFAULT_LOOKBACK_DAYS).expect("constant fits in Duration")
    });

    let total_days = (to_date - from_date).num_days();
    let num_windows = ((total_days + MAX_WINDOW_DAYS - 1) / MAX_WINDOW_DAYS).max(1);

    log::info!(
        "{}: fetching from {} to {} ({total_days} days, {num_windows} window(s), agency {})",
        config.label,
        from_date.format("%Y-%m-%d"),
        to_date.format("%Y-%m-%d"),
        config.agency_id,
    );

    let mut total_fetched: u64 = 0;

    // ── Iterate over time windows ────────────────────────────────────
    for window_idx in 0..num_windows {
        if total_fetched >= fetch_limit {
            break;
        }

        let window_start = from_date
            + Duration::try_days(window_idx * MAX_WINDOW_DAYS)
                .expect("window offset fits in Duration");
        let window_end = (from_date
            + Duration::try_days((window_idx + 1) * MAX_WINDOW_DAYS)
                .expect("window offset fits in Duration"))
        .min(to_date);

        if num_windows > 1 {
            log::info!(
                "{}: window {}/{num_windows} ({} to {})",
                config.label,
                window_idx + 1,
                window_start.format("%Y-%m-%d"),
                window_end.format("%Y-%m-%d"),
            );
        }

        // ── Paginate within the window ───────────────────────────────
        let mut offset: u64 = 0;

        loop {
            let remaining = fetch_limit.saturating_sub(total_fetched);
            if remaining == 0 {
                break;
            }

            let body = build_request_body(
                &CityProtectConfig {
                    page_size: page_size.min(remaining),
                    ..*config
                },
                &window_start,
                &window_end,
                offset,
            );

            log::info!(
                "{}: offset={offset}, total_fetched={total_fetched}",
                config.label,
            );

            let response = client
                .post(config.api_url)
                .header("Origin", "https://cityprotect.com")
                .header("Referer", "https://cityprotect.com/")
                .json(&body)
                .send()
                .await?;

            let status = response.status();
            if !status.is_success() {
                let err_body = response
                    .text()
                    .await
                    .unwrap_or_else(|_| String::from("(no body)"));
                return Err(SourceError::Normalization {
                    message: format!(
                        "{}: CityProtect API returned {status}: {err_body}",
                        config.label
                    ),
                });
            }

            let resp_body: serde_json::Value = response.json().await?;

            // Extract incidents from result.list.incidents
            let incidents = resp_body
                .get("result")
                .and_then(|r| r.get("list"))
                .and_then(|l| l.get("incidents"))
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();

            let count = incidents.len() as u64;
            if count == 0 {
                break;
            }

            let page: Vec<serde_json::Value> =
                incidents.iter().filter_map(flatten_incident).collect();

            total_fetched += page.len() as u64;
            offset += count;
            progress.inc(page.len() as u64);

            tx.send(page)
                .await
                .map_err(|e| SourceError::Normalization {
                    message: format!("channel send failed: {e}"),
                })?;

            // If we got fewer than page_size, no more records in this window
            if count < page_size {
                break;
            }
        }
    }

    log::info!(
        "{}: download complete — {total_fetched} records",
        config.label,
    );
    progress.finish(format!(
        "{}: download complete -- {total_fetched} records",
        config.label,
    ));
    Ok(total_fetched)
}
