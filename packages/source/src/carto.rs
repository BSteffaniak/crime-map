//! Shared Carto SQL API fetcher.
//!
//! Handles paginated fetching from Carto SQL endpoints. Pages of raw JSON
//! records are sent through a channel for immediate processing.

use std::fmt::Write as _;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::progress::ProgressCallback;
use crate::{FetchOptions, SourceError};

/// Configuration for a Carto SQL fetch operation.
pub struct CartoConfig<'a> {
    /// Base Carto SQL API URL (e.g., `"https://phl.carto.com/api/v2/sql"`).
    pub api_url: &'a str,
    /// Table name to query (e.g., `"incidents_part1_part2"`).
    pub table_name: &'a str,
    /// Date column for ordering and `WHERE` filtering.
    pub date_column: &'a str,
    /// Label for log messages (e.g., `"Philly"`).
    pub label: &'a str,
    /// Page size for pagination.
    pub page_size: u64,
}

/// Queries the Carto SQL endpoint for the total record count.
/// Returns `None` if the count request fails (non-fatal).
async fn query_carto_count(
    client: &reqwest::Client,
    config: &CartoConfig<'_>,
    options: &FetchOptions,
) -> Option<u64> {
    let query = options.since.as_ref().map_or_else(
        || format!("SELECT count(*) as count FROM {}", config.table_name),
        |since| {
            let since_str = since.format("%Y-%m-%d %H:%M:%S").to_string();
            format!(
                "SELECT count(*) as count FROM {} WHERE {} > '{since_str}'",
                config.table_name, config.date_column
            )
        },
    );
    let response = client
        .get(config.api_url)
        .query(&[("q", &query)])
        .send()
        .await
        .ok()?;
    let body: serde_json::Value = response.json().await.ok()?;
    body.get("rows")?
        .as_array()?
        .first()?
        .get("count")?
        .as_u64()
}

/// Fetches records from a Carto SQL endpoint page by page, sending each
/// page through the provided channel.
///
/// Returns the total number of records fetched.
///
/// # Errors
///
/// Returns [`SourceError`] if HTTP requests fail.
pub async fn fetch_carto(
    config: &CartoConfig<'_>,
    options: &FetchOptions,
    tx: &mpsc::Sender<Vec<serde_json::Value>>,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<u64, SourceError> {
    let client = reqwest::Client::new();
    let mut offset: u64 = options.resume_offset;
    let fetch_limit = options.limit.unwrap_or(u64::MAX);

    // ── Pre-fetch count ──────────────────────────────────────────────
    let total_available = query_carto_count(&client, config, options).await;

    if let Some(total) = total_available {
        progress.set_total(fetch_limit.min(total));
        if offset > 0 {
            log::info!(
                "{}: {total} records available (resuming from offset {offset}, page size {})",
                config.label,
                config.page_size
            );
        } else if fetch_limit >= total {
            log::info!(
                "{}: {total} records available (fetching all, page size {})",
                config.label,
                config.page_size
            );
        } else {
            log::info!(
                "{}: {total} records available (fetching up to {fetch_limit}, page size {})",
                config.label,
                config.page_size
            );
        }
    }

    // ── Paginated fetch ──────────────────────────────────────────────
    let will_fetch = total_available.map(|t| fetch_limit.min(t));

    loop {
        let remaining = fetch_limit.saturating_sub(offset);
        if remaining == 0 {
            break;
        }
        let page_limit = remaining.min(config.page_size);

        let query = options.since.as_ref().map_or_else(
            || {
                format!(
                    "SELECT * FROM {} ORDER BY {} DESC LIMIT {page_limit} OFFSET {offset}",
                    config.table_name, config.date_column
                )
            },
            |since| {
                let since_str = since.format("%Y-%m-%d %H:%M:%S").to_string();
                let mut q = format!(
                    "SELECT * FROM {} WHERE {} > '{since_str}'",
                    config.table_name, config.date_column
                );
                write!(
                    q,
                    " ORDER BY {} DESC LIMIT {page_limit} OFFSET {offset}",
                    config.date_column
                )
                .unwrap();
                q
            },
        );

        if let Some(target) = will_fetch {
            log::info!(
                "{}: page {} — {offset} / {target} fetched",
                config.label,
                (offset / config.page_size) + 1,
            );
        } else {
            log::info!("{}: offset={offset}, limit={page_limit}", config.label);
        }

        let response = client
            .get(config.api_url)
            .query(&[("q", &query)])
            .send()
            .await?;
        let body: serde_json::Value = response.json().await?;

        let rows = body
            .get("rows")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();

        let count = rows.len() as u64;
        if count == 0 {
            break;
        }

        offset += count;
        progress.inc(count);

        tx.send(rows)
            .await
            .map_err(|e| SourceError::Normalization {
                message: format!("channel send failed: {e}"),
            })?;

        if count < page_limit {
            break;
        }
    }

    log::info!("{}: download complete — {offset} records", config.label);
    progress.finish(format!(
        "{}: download complete -- {offset} records",
        config.label
    ));
    Ok(offset)
}
