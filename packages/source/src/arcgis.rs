//! Shared `ArcGIS` REST API fetcher.
//!
//! Handles paginated fetching from `ArcGIS` `FeatureServer` or `MapServer`
//! endpoints. Supports multiple query URLs (e.g., one per year/layer) and
//! sends each page through a channel for immediate processing.

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::progress::ProgressCallback;
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
    /// Date column name for incremental `since` filtering (epoch-ms field).
    /// When set alongside `FetchOptions::since`, adds a
    /// `{date_column} >= {epoch_ms}` predicate to the WHERE clause.
    pub date_column: Option<&'a str>,
}

/// Builds the effective WHERE clause by combining the static base clause
/// with an optional `since` date predicate.
///
/// `ArcGIS` REST API uses `DATE 'YYYY-MM-DD HH:MM:SS'` literals for date
/// comparisons.
fn build_where_clause(
    base: &str,
    date_column: Option<&str>,
    since: Option<&chrono::DateTime<chrono::Utc>>,
) -> String {
    if let Some(col) = date_column
        && let Some(since_dt) = since
    {
        let date_str = since_dt.format("%Y-%m-%d %H:%M:%S");
        format!("{base} AND {col} >= DATE '{date_str}'")
    } else {
        base.to_string()
    }
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
        let body = crate::retry::send_json(|| client.get(&url)).await.ok()?;
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
    progress: &Arc<dyn ProgressCallback>,
) -> Result<u64, SourceError> {
    let client = crate::build_http_client()?;
    let fetch_limit = options.limit.unwrap_or(u64::MAX);
    let base_where = config.where_clause.unwrap_or("1=1");
    let where_clause = build_where_clause(base_where, config.date_column, options.since.as_ref());
    let num_layers = config.query_urls.len();
    let mut total_fetched: u64 = 0;
    let display_offset: u64 = options.resume_offset;

    // ── Pre-fetch count ──────────────────────────────────────────────
    let total_available = query_arcgis_counts(&client, config, &where_clause).await;

    if let Some(total) = total_available {
        progress.set_total(fetch_limit.min(total).saturating_sub(options.resume_offset));
        let layers_str = if num_layers > 1 {
            format!(" across {num_layers} layers")
        } else {
            String::new()
        };
        if options.resume_offset > 0 {
            log::info!(
                "{}: {total} records available{layers_str} (resuming from offset {}, page size {})",
                config.label,
                options.resume_offset,
                config.page_size
            );
        } else if fetch_limit >= total {
            log::info!(
                "{}: {total} records available{layers_str} (fetching all, page size {})",
                config.label,
                config.page_size
            );
        } else {
            log::info!(
                "{}: {total} records available{layers_str} (fetching up to {fetch_limit}, page size {})",
                config.label,
                config.page_size
            );
        }
    }

    // ── Compute per-layer counts for resume skipping ──────────────────
    // When resuming, we skip entire layers whose records have already been
    // ingested, then apply the remaining offset within the current layer.
    let mut layer_counts: Vec<u64> = Vec::new();
    if options.resume_offset > 0 && num_layers > 1 {
        for query_url in config.query_urls {
            let url = format!("{query_url}?where={where_clause}&returnCountOnly=true&f=json");
            let count = async {
                let body = crate::retry::send_json(|| client.get(&url)).await.ok()?;
                body.get("count")?.as_u64()
            }
            .await
            .unwrap_or(0);
            layer_counts.push(count);
        }
    }

    // ── Paginated fetch ──────────────────────────────────────────────
    let will_fetch = total_available.map(|t| fetch_limit.min(t));
    let mut skipped: u64 = 0;
    let mut current_page_size = config.page_size;

    for (layer_idx, query_url) in config.query_urls.iter().enumerate() {
        if total_fetched >= fetch_limit {
            break;
        }

        // Resume: skip entire layers that were already ingested
        if options.resume_offset > 0
            && skipped < options.resume_offset
            && let Some(&layer_count) = layer_counts.get(layer_idx)
            && skipped + layer_count <= options.resume_offset
        {
            skipped += layer_count;
            log::info!(
                "{}: skipping layer {}/{num_layers} ({layer_count} records already ingested)",
                config.label,
                layer_idx + 1,
            );
            continue;
        }

        // For the first non-skipped layer, apply the remaining resume offset
        let layer_resume = options.resume_offset.saturating_sub(skipped);
        // Mark all remaining offset as consumed
        skipped = options.resume_offset;

        let mut offset: u64 = layer_resume;

        loop {
            let remaining = fetch_limit.saturating_sub(total_fetched);
            if remaining == 0 {
                break;
            }
            let page_limit = remaining.min(current_page_size);

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
                    "{}: {layer_label}{} / {target} fetched",
                    config.label,
                    display_offset + total_fetched,
                );
            } else {
                log::info!("{}: offset={offset}, limit={page_limit}", config.label);
            }

            let body = match crate::retry::send_json(|| client.get(&url)).await {
                Ok(body) => body,
                Err(e)
                    if crate::is_page_size_reducible(&e)
                        && current_page_size > crate::MIN_PAGE_SIZE =>
                {
                    current_page_size = (current_page_size / 2).max(crate::MIN_PAGE_SIZE);
                    log::warn!(
                        "{}: reducing page size to {current_page_size} after fetch failure, retrying same offset",
                        config.label,
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };

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
            progress.inc(page.len() as u64);

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

    let total_with_resumed = display_offset + total_fetched;
    log::info!(
        "{}: download complete — {total_with_resumed} records",
        config.label,
    );
    progress.finish(format!(
        "{}: download complete -- {total_with_resumed} records",
        config.label
    ));
    Ok(total_fetched)
}
