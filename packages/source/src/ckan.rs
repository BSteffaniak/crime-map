//! Shared CKAN Datastore API fetcher.
//!
//! Handles paginated fetching from CKAN `datastore_search` endpoints.
//! Supports multiple resource IDs (e.g., one per year) and sends pages
//! of raw JSON records through a channel for immediate processing.

use tokio::sync::mpsc;

use crate::{FetchOptions, SourceError};

/// Configuration for a CKAN fetch operation.
pub struct CkanConfig<'a> {
    /// Base API URL (e.g.,
    /// `"https://data.boston.gov/api/3/action/datastore_search"`).
    pub api_url: &'a str,
    /// CKAN resource IDs for the dataset (one per year/split).
    pub resource_ids: &'a [String],
    /// Label for log messages (e.g., `"Boston"`).
    pub label: &'a str,
    /// Page size for pagination.
    pub page_size: u64,
}

/// Fetches records from one or more CKAN Datastore resources page by page,
/// sending each page through the provided channel.
///
/// Returns the total number of records fetched across all resources.
///
/// # Errors
///
/// Returns [`SourceError`] if HTTP requests fail.
pub async fn fetch_ckan(
    config: &CkanConfig<'_>,
    options: &FetchOptions,
    tx: &mpsc::Sender<Vec<serde_json::Value>>,
) -> Result<u64, SourceError> {
    let client = reqwest::Client::new();
    let fetch_limit = options.limit.unwrap_or(u64::MAX);
    let num_resources = config.resource_ids.len();

    // Pre-fetch counts from all resources
    let mut resource_counts: Vec<u64> = Vec::with_capacity(num_resources);
    let mut total_available: u64 = 0;

    for resource_id in config.resource_ids {
        let response = client
            .get(config.api_url)
            .query(&[("resource_id", resource_id.as_str()), ("limit", "0")])
            .send()
            .await?;
        let body: serde_json::Value = response.json().await?;
        let count = body
            .get("result")
            .and_then(|r| r.get("total"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        resource_counts.push(count);
        total_available += count;
    }

    if fetch_limit >= total_available {
        log::info!(
            "{}: {total_available} records available across {num_resources} resource(s) (fetching all)",
            config.label
        );
    } else {
        log::info!(
            "{}: {total_available} records available across {num_resources} resource(s) (fetching up to {fetch_limit})",
            config.label
        );
    }

    let mut grand_total: u64 = 0;

    for (idx, resource_id) in config.resource_ids.iter().enumerate() {
        let remaining_global = fetch_limit.saturating_sub(grand_total);
        if remaining_global == 0 {
            break;
        }

        let resource_count = resource_counts[idx];
        log::info!(
            "{}: resource {}/{num_resources} — {resource_count} records",
            config.label,
            idx + 1,
        );

        let mut offset: u64 = 0;

        loop {
            let remaining = remaining_global.saturating_sub(offset);
            if remaining == 0 {
                break;
            }
            let page_limit = remaining.min(config.page_size);

            log::info!(
                "{}: resource {}/{num_resources}, page {} — {offset} / {} fetched",
                config.label,
                idx + 1,
                (offset / config.page_size) + 1,
                remaining_global.min(resource_count),
            );

            let response = client
                .get(config.api_url)
                .query(&[
                    ("resource_id", resource_id.as_str()),
                    ("limit", &page_limit.to_string()),
                    ("offset", &offset.to_string()),
                ])
                .send()
                .await?;
            let body: serde_json::Value = response.json().await?;

            let records = body
                .get("result")
                .and_then(|r| r.get("records"))
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();

            let count = records.len() as u64;
            if count == 0 {
                break;
            }

            offset += count;

            tx.send(records)
                .await
                .map_err(|e| SourceError::Normalization {
                    message: format!("channel send failed: {e}"),
                })?;

            if count < page_limit {
                break;
            }
        }

        grand_total += offset;
    }

    log::info!(
        "{}: download complete — {grand_total} records",
        config.label
    );
    Ok(grand_total)
}
