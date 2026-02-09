//! Shared `ArcGIS` REST API fetcher.
//!
//! Handles paginated fetching from `ArcGIS` `FeatureServer` or `MapServer`
//! endpoints. Supports multiple query URLs (e.g., one per year/layer) and
//! sends each page through a channel for immediate processing.

use tokio::sync::mpsc;

use crate::{FetchOptions, SourceError};

/// Configuration for an `ArcGIS` fetch operation.
pub struct ArcGisConfig<'a> {
    /// Query URLs to fetch from (one per layer/year). Each URL is fetched
    /// with pagination and results are merged.
    pub query_urls: &'a [String],
    /// Label for log messages (e.g., `"DC"`).
    pub label: &'a str,
    /// Max records per request (often 1000 or 2000).
    pub page_size: u64,
    /// Optional `where` clause (e.g., `"REPORT_DAT >= '2020-01-01'"`).
    /// Defaults to `"1=1"` if `None`.
    pub where_clause: Option<&'a str>,
}

/// Queries each `ArcGIS` layer for its record count using
/// `returnCountOnly=true`. Returns `None` if any count request fails
/// (non-fatal).
async fn query_arcgis_counts(
    client: &reqwest::Client,
    config: &ArcGisConfig<'_>,
    where_clause: &str,
) -> Option<u64> {
    let mut total: u64 = 0;
    for query_url in config.query_urls {
        let url = format!("{query_url}?where={where_clause}&returnCountOnly=true&f=json");
        let response = client.get(&url).send().await.ok()?;
        let body: serde_json::Value = response.json().await.ok()?;
        let count = body.get("count")?.as_u64()?;
        total += count;
    }
    Some(total)
}

/// Flattens an `ArcGIS` feature by extracting its attributes and merging
/// geometry `x`/`y` coordinates as `_geometry_x`/`_geometry_y` fields.
fn flatten_feature(feature: &serde_json::Value) -> Option<serde_json::Value> {
    let mut record = feature.get("attributes")?.clone();
    if let Some(geom) = feature.get("geometry")
        && let Some(obj) = record.as_object_mut()
    {
        if let Some(x) = geom.get("x") {
            obj.insert("_geometry_x".to_string(), x.clone());
        }
        if let Some(y) = geom.get("y") {
            obj.insert("_geometry_y".to_string(), y.clone());
        }
    }
    Some(record)
}

/// Fetches features from one or more `ArcGIS` REST endpoints page by page,
/// sending each page through the provided channel.
///
/// Returns the total number of records fetched.
///
/// # Errors
///
/// Returns [`SourceError`] if HTTP requests fail.
#[allow(clippy::too_many_lines)]
pub async fn fetch_arcgis(
    config: &ArcGisConfig<'_>,
    options: &FetchOptions,
    tx: &mpsc::Sender<Vec<serde_json::Value>>,
) -> Result<u64, SourceError> {
    let client = reqwest::Client::new();
    let fetch_limit = options.limit.unwrap_or(u64::MAX);
    let where_clause = config.where_clause.unwrap_or("1=1");
    let num_layers = config.query_urls.len();
    let mut total_fetched: u64 = 0;

    // ── Pre-fetch count ──────────────────────────────────────────────
    let total_available = query_arcgis_counts(&client, config, where_clause).await;

    if let Some(total) = total_available {
        let layers_str = if num_layers > 1 {
            format!(" across {num_layers} layers")
        } else {
            String::new()
        };
        if fetch_limit >= total {
            log::info!(
                "{}: {total} records available{layers_str} (fetching all)",
                config.label
            );
        } else {
            log::info!(
                "{}: {total} records available{layers_str} (fetching up to {fetch_limit})",
                config.label
            );
        }
    }

    // ── Paginated fetch ──────────────────────────────────────────────
    let will_fetch = total_available.map(|t| fetch_limit.min(t));

    for (layer_idx, query_url) in config.query_urls.iter().enumerate() {
        let mut offset: u64 = 0;
        if total_fetched >= fetch_limit {
            break;
        }

        loop {
            let remaining = fetch_limit.saturating_sub(total_fetched);
            if remaining == 0 {
                break;
            }
            let page_limit = remaining.min(config.page_size);

            let url = format!(
                "{query_url}?where={where_clause}&outFields=*&f=json&outSR=4326&resultRecordCount={page_limit}&resultOffset={offset}"
            );

            if let Some(target) = will_fetch {
                let layer_label = if num_layers > 1 {
                    format!("layer {}/{num_layers} — ", layer_idx + 1)
                } else {
                    String::new()
                };
                log::info!(
                    "{}: {layer_label}{total_fetched} / {target} fetched",
                    config.label,
                );
            } else {
                log::info!("{}: offset={offset}, limit={page_limit}", config.label);
            }

            let response = client.get(&url).send().await?;
            let body: serde_json::Value = response.json().await?;

            let features = body
                .get("features")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();

            let count = features.len() as u64;
            if count == 0 {
                break;
            }

            let page: Vec<serde_json::Value> =
                features.iter().filter_map(flatten_feature).collect();

            total_fetched += page.len() as u64;
            offset += count;

            tx.send(page)
                .await
                .map_err(|e| SourceError::Normalization {
                    message: format!("channel send failed: {e}"),
                })?;

            // ArcGIS sets `exceededTransferLimit: true` when more records
            // exist beyond this page.
            let exceeded = body
                .get("exceededTransferLimit")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if !exceeded {
                break;
            }
        }
    }

    log::info!(
        "{}: download complete — {total_fetched} records",
        config.label,
    );
    Ok(total_fetched)
}
