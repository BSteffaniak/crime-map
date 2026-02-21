//! `LexisNexis` Community Crime Map (CCM) incident API fetcher.
//!
//! The `LexisNexis` CCM is a public-facing crime mapping platform used by
//! hundreds of police agencies across the US. The hidden REST API was
//! reverse-engineered from the Angular SPA at `communitycrimemap.com`.
//!
//! Key details:
//! - Auth: anonymous JWT from `/api/v1/auth/newToken` (no credentials)
//! - Data: POST to `/api/v1/search/load-data` with bounds + date + layers
//! - Response: `data.data.pins` is a dict of incident objects
//! - Each pin has lat/lng, case number, crime type, address, datetime, agency
//! - Must select crime type layers (IDs) to get results
//! - Date format: `"MM/DD/YYYY"` in the request payload
//! - The API returns incidents for ALL agencies within the bounds — we filter
//!   by agency name in `flatten_pin`
//!
//! The API does not paginate in the traditional sense; it returns all matching
//! pins within the bounding box. For large jurisdictions, we split into smaller
//! geographic tiles.

use std::sync::Arc;

use chrono::{Duration, Utc};
use serde_json::json;
use tokio::sync::mpsc;

use crate::progress::ProgressCallback;
use crate::{FetchOptions, SourceError};

/// How far back to fetch when no `since` date is specified (~1 year).
const DEFAULT_LOOKBACK_DAYS: i64 = 365;

/// Maximum date range per request (CCM doesn't seem to enforce a max, but
/// we keep windows to ~90 days to avoid oversized responses).
const MAX_WINDOW_DAYS: i64 = 90;

/// All crime type layer IDs from the CCM `/api/v1/search/map-layers`
/// endpoint. Selecting all ensures we get every incident type.
const ALL_LAYER_IDS: &[u32] = &[
    2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
    28, 141, 142, 143, 144, 145, 146, 147, 148,
];

/// Configuration for a `LexisNexis` CCM fetch operation.
pub struct LexisNexisCcmConfig<'a> {
    /// Bounding box `[west, south, east, north]` for the agency's
    /// jurisdiction.
    pub bbox: &'a [f64; 4],
    /// Agency name filter — only keep incidents from this agency.
    /// Matched case-insensitively as a substring.
    pub agency_filter: &'a str,
    /// Label for log messages.
    pub label: &'a str,
}

/// Builds the layer selection object for the POST body.
fn build_layer_selection() -> serde_json::Value {
    let mut selection = serde_json::Map::new();
    for id in ALL_LAYER_IDS {
        selection.insert(id.to_string(), json!({"selected": true}));
    }
    serde_json::Value::Object(selection)
}

/// Builds the POST request body for the `load-data` endpoint.
fn build_request_body(bbox: &[f64; 4], from_date: &str, to_date: &str) -> serde_json::Value {
    let [west, south, east, north] = *bbox;
    let lat = f64::midpoint(south, north);
    let lng = f64::midpoint(west, east);

    json!({
        "location": {
            "lat": lat,
            "lng": lng,
            "zoom": 14,
            "bounds": {
                "north": north,
                "south": south,
                "east": east,
                "west": west
            }
        },
        "date": {
            "start": from_date,
            "end": to_date
        },
        "agencies": [],
        "layers": {
            "selection": build_layer_selection()
        },
        "buffer": {
            "enabled": false,
            "restrictArea": false,
            "value": []
        }
    })
}

/// Obtains an anonymous JWT token from the CCM auth endpoint.
async fn get_token(client: &reqwest::Client) -> Result<String, SourceError> {
    let resp = client
        .get("https://communitycrimemap.com/api/v1/auth/newToken")
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(SourceError::Normalization {
            message: format!("CCM auth returned {}", resp.status()),
        });
    }

    let body: serde_json::Value = resp.json().await?;
    body.get("data")
        .and_then(|d| d.get("jwt"))
        .and_then(serde_json::Value::as_str)
        .map(String::from)
        .ok_or_else(|| SourceError::Normalization {
            message: "CCM auth: missing data.jwt in response".to_string(),
        })
}

/// Flattens a CCM pin record into a top-level JSON object suitable for
/// the standard field-mapping normalization pipeline.
fn flatten_pin(pin: &serde_json::Value) -> Option<serde_json::Value> {
    let obj = pin.as_object()?;
    let mut flat = serde_json::Map::new();

    // Top-level fields
    for (key, value) in obj {
        if key == "EventRecord" {
            // Flatten the nested MO record
            if let Some(mo) = value
                .get("MOs")
                .and_then(|m| m.get("MO"))
                .and_then(serde_json::Value::as_object)
            {
                for (mk, mv) in mo {
                    // Prefix to avoid collisions, except for fields we want
                    // at the top level
                    match mk.as_str() {
                        "Crime" | "LocationType" | "AddressOfCrime" | "AddressName" => {
                            flat.insert(mk.clone(), mv.clone());
                        }
                        _ => {
                            flat.insert(format!("_mo_{mk}"), mv.clone());
                        }
                    }
                }
            }
        } else {
            flat.insert(key.clone(), value.clone());
        }
    }

    Some(serde_json::Value::Object(flat))
}

/// Fetches incidents from the `LexisNexis` CCM API for a given bounding
/// box and date range, filtered by agency name.
///
/// # Panics
///
/// Panics if the constant lookback or window duration cannot be
/// represented as a `chrono::Duration` (should never happen in practice).
///
/// # Errors
///
/// Returns [`SourceError`] if HTTP requests, JSON parsing, or auth fails.
#[allow(clippy::too_many_lines)]
pub async fn fetch_lexisnexis_ccm(
    config: &LexisNexisCcmConfig<'_>,
    options: &FetchOptions,
    tx: &mpsc::Sender<Vec<serde_json::Value>>,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<u64, SourceError> {
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        .build()?;

    let fetch_limit = options.limit.unwrap_or(u64::MAX);

    // ── Get auth token ───────────────────────────────────────────────
    let token = get_token(&client).await?;
    log::info!("{}: obtained CCM auth token", config.label);

    // ── Determine date range ─────────────────────────────────────────
    let to_date = Utc::now();
    let from_date = options.since.unwrap_or_else(|| {
        to_date - Duration::try_days(DEFAULT_LOOKBACK_DAYS).expect("constant fits in Duration")
    });

    let total_days = (to_date - from_date).num_days();
    let num_windows = ((total_days + MAX_WINDOW_DAYS - 1) / MAX_WINDOW_DAYS).max(1);

    log::info!(
        "{}: fetching from {} to {} ({total_days} days, {num_windows} window(s), agency filter: {:?})",
        config.label,
        from_date.format("%Y-%m-%d"),
        to_date.format("%Y-%m-%d"),
        config.agency_filter,
    );

    let mut total_fetched: u64 = 0;
    let agency_filter_lower = config.agency_filter.to_lowercase();

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

        let start_str = window_start.format("%m/%d/%Y").to_string();
        let end_str = window_end.format("%m/%d/%Y").to_string();

        log::info!(
            "{}: window {}/{num_windows} ({start_str} to {end_str})",
            config.label,
            window_idx + 1,
        );

        let body = build_request_body(config.bbox, &start_str, &end_str);

        let response = client
            .post("https://communitycrimemap.com/api/v1/search/load-data")
            .header("Authorization", format!("Bearer {token}"))
            .header("Origin", "https://communitycrimemap.com")
            .header("Referer", "https://communitycrimemap.com/map")
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
                    "{}: CCM API returned {status}: {}",
                    config.label,
                    &err_body[..err_body.len().min(500)],
                ),
            });
        }

        let resp_body: serde_json::Value = response.json().await?;

        // Extract pins from data.data.pins (object with string keys)
        let pins = resp_body
            .get("data")
            .and_then(|d| d.get("data"))
            .and_then(|d| d.get("pins"))
            .and_then(serde_json::Value::as_object)
            .cloned()
            .unwrap_or_default();

        let total_in_window = pins.len();
        log::info!(
            "{}: window {}/{num_windows} returned {total_in_window} pins (all agencies)",
            config.label,
            window_idx + 1,
        );

        // Filter by agency name and flatten
        let mut page: Vec<serde_json::Value> = Vec::new();
        for (_key, pin) in &pins {
            if total_fetched + page.len() as u64 >= fetch_limit {
                break;
            }

            // Filter by agency
            let agency = pin
                .get("Agency")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            if !agency.to_lowercase().contains(&agency_filter_lower) {
                continue;
            }

            if let Some(flat) = flatten_pin(pin) {
                page.push(flat);
            }
        }

        if !page.is_empty() {
            let count = page.len() as u64;
            total_fetched += count;
            progress.inc(count);

            tx.send(page)
                .await
                .map_err(|e| SourceError::Normalization {
                    message: format!("channel send failed: {e}"),
                })?;
        }

        // Rate limiting — be nice to the API
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }

    log::info!(
        "{}: download complete — {total_fetched} records (filtered by agency)",
        config.label,
    );
    progress.finish(format!(
        "{}: download complete -- {total_fetched} records",
        config.label,
    ));
    Ok(total_fetched)
}
