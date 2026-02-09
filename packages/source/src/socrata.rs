//! Shared Socrata SODA API fetcher.
//!
//! Handles paginated fetching from any Socrata dataset using the `$limit`,
//! `$offset`, `$order`, and `$where` query parameters. Pages of raw JSON
//! records are sent through a channel for immediate processing.

use std::fmt::Write as _;

use tokio::sync::mpsc;

use crate::{FetchOptions, SourceError};

/// Configuration for a Socrata fetch operation.
pub struct SocrataConfig<'a> {
    /// Base API URL (e.g., `"https://data.lacity.org/resource/2nrs-mtv8.json"`).
    pub api_url: &'a str,
    /// The date column name for ordering and `$where` filtering (e.g., `"date"`,
    /// `"date_occ"`).
    pub date_column: &'a str,
    /// Label for log messages (e.g., `"Chicago"`).
    pub label: &'a str,
    /// Page size for pagination (default 50,000).
    pub page_size: u64,
}

/// Queries the Socrata `$select=count(*)` endpoint to get the total number of
/// records available. Returns `None` if the count request fails (non-fatal).
async fn query_socrata_count(
    client: &reqwest::Client,
    config: &SocrataConfig<'_>,
    options: &FetchOptions,
) -> Option<u64> {
    let mut url = format!("{}?$select=count(*) as count", config.api_url);
    if let Some(since) = &options.since {
        let since_str = since.format("%Y-%m-%dT%H:%M:%S").to_string();
        write!(url, "&$where={} > '{since_str}'", config.date_column).unwrap();
    }
    let response = client.get(&url).send().await.ok()?;
    let body: serde_json::Value = response.json().await.ok()?;
    body.as_array()?
        .first()?
        .get("count")?
        .as_str()?
        .parse::<u64>()
        .ok()
}

/// Fetches records from a Socrata dataset page by page, sending each page
/// through the provided channel.
///
/// Returns the total number of records fetched.
///
/// # Errors
///
/// Returns [`SourceError`] if HTTP requests fail.
pub async fn fetch_socrata(
    config: &SocrataConfig<'_>,
    options: &FetchOptions,
    tx: &mpsc::Sender<Vec<serde_json::Value>>,
) -> Result<u64, SourceError> {
    let client = reqwest::Client::new();
    let mut offset: u64 = 0;
    let fetch_limit = options.limit.unwrap_or(u64::MAX);

    // ── Pre-fetch count ──────────────────────────────────────────────
    let total_available = query_socrata_count(&client, config, options).await;

    if let Some(total) = total_available {
        if fetch_limit >= total {
            log::info!("{}: {total} records available (fetching all)", config.label);
        } else {
            log::info!(
                "{}: {total} records available (fetching up to {fetch_limit})",
                config.label
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

        let mut url = format!(
            "{}?$limit={}&$offset={}&$order={} DESC",
            config.api_url, page_limit, offset, config.date_column
        );

        if let Some(since) = &options.since {
            let since_str = since.format("%Y-%m-%dT%H:%M:%S").to_string();
            write!(url, "&$where={} > '{since_str}'", config.date_column).unwrap();
        }

        if let Some(target) = will_fetch {
            log::info!(
                "{}: page {} — {offset} / {target} fetched",
                config.label,
                (offset / config.page_size) + 1,
            );
        } else {
            log::info!("{}: offset={offset}, limit={page_limit}", config.label);
        }

        let response = client.get(&url).send().await?;
        let records: Vec<serde_json::Value> = response.json().await?;

        let count = records.len() as u64;
        if count == 0 {
            break;
        }

        offset += count;

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
    Ok(offset)
}
