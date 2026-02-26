//! OData-style REST API fetcher.
//!
//! Handles paginated fetching from APIs that use `$top`/`$skip`/`$orderby`
//! query parameters and return bare JSON arrays. The count endpoint is
//! queried separately via `{base_url}/$count`.
//!
//! Currently used for Arlington County, VA police incident data.

use std::fmt::Write as _;
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::progress::ProgressCallback;
use crate::{FetchOptions, SourceError};

/// Configuration for an OData-style fetch operation.
pub struct ODataConfig<'a> {
    /// Base API URL (e.g., `"https://datahub-v2.arlingtonva.us/api/Police/IncidentLog"`).
    pub api_url: &'a str,
    /// The date column name for ordering and `$filter` (e.g., `"firstReportDtm"`).
    pub date_column: &'a str,
    /// Label for log messages.
    pub label: &'a str,
    /// Page size for `$top` parameter.
    pub page_size: u64,
}

/// Queries the `/$count` endpoint to get the total number of records.
/// Returns `None` if the count request fails (non-fatal).
async fn query_odata_count(
    client: &reqwest::Client,
    config: &ODataConfig<'_>,
    options: &FetchOptions,
) -> Option<u64> {
    let mut url = format!("{}/$count", config.api_url);
    if let Some(since) = &options.since {
        let since_str = since.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        write!(url, "?$filter={} gt {since_str}", config.date_column).unwrap();
    }
    let text = crate::retry::send_text(|| client.get(&url)).await.ok()?;
    text.trim().parse::<u64>().ok()
}

/// Fetches records from an OData-style REST API page by page, sending each
/// page through the provided channel.
///
/// Returns the total number of records fetched.
///
/// # Errors
///
/// Returns [`SourceError`] if HTTP requests fail.
pub async fn fetch_odata(
    config: &ODataConfig<'_>,
    options: &FetchOptions,
    tx: &mpsc::Sender<Vec<serde_json::Value>>,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<u64, SourceError> {
    let client = crate::build_http_client()?;
    let mut offset: u64 = options.resume_offset;
    let fetch_limit = options.limit.unwrap_or(u64::MAX);

    // ── Pre-fetch count ──────────────────────────────────────────────
    let total_available = query_odata_count(&client, config, options).await;

    if let Some(total) = total_available {
        progress.set_total(fetch_limit.min(total).saturating_sub(options.resume_offset));
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
    let mut current_page_size = config.page_size;

    loop {
        let remaining = fetch_limit.saturating_sub(offset);
        if remaining == 0 {
            break;
        }
        let page_limit = remaining.min(current_page_size);

        let mut url = format!(
            "{}?$top={page_limit}&$skip={offset}&$orderby={} asc",
            config.api_url, config.date_column
        );

        if let Some(since) = &options.since {
            let since_str = since.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
            write!(url, "&$filter={} gt {since_str}", config.date_column).unwrap();
        }

        if let Some(target) = will_fetch {
            log::info!(
                "{}: page {} — {offset} / {target} fetched",
                config.label,
                (offset / current_page_size) + 1,
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
        let records: Vec<serde_json::Value> = serde_json::from_value(body)?;

        let count = records.len() as u64;
        if count == 0 {
            break;
        }

        offset += count;
        progress.inc(count);

        // Send page for immediate processing
        tx.send(records)
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
