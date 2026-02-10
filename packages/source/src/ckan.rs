//! Shared CKAN Datastore API fetcher.
//!
//! Handles paginated fetching from CKAN `datastore_search` endpoints.
//! Supports multiple resource IDs (e.g., one per year) and sends pages
//! of raw JSON records through a channel for immediate processing.
//!
//! When a `since` timestamp and `date_column` are provided, uses
//! `datastore_search_sql` for server-side date filtering.

use std::fmt::Write as _;

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
    /// Date column for incremental `since` filtering.
    pub date_column: Option<&'a str>,
}

/// Derives the `datastore_search_sql` URL from a `datastore_search` URL.
///
/// e.g., `.../api/3/action/datastore_search` -> `.../api/3/action/datastore_search_sql`
fn sql_api_url(base_url: &str) -> String {
    format!("{base_url}_sql")
}

/// Fetches records from one or more CKAN Datastore resources page by page,
/// sending each page through the provided channel.
///
/// When `options.since` is `Some` and `config.date_column` is `Some`, uses
/// `datastore_search_sql` for server-side date filtering to enable
/// incremental syncing. Otherwise falls back to the standard
/// `datastore_search` endpoint.
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
    let use_sql = config.date_column.is_some() && options.since.is_some();

    if use_sql {
        fetch_ckan_sql(config, options, tx).await
    } else {
        fetch_ckan_standard(config, options, tx).await
    }
}

/// Standard CKAN `datastore_search` fetch (no date filtering).
#[allow(clippy::too_many_lines)]
async fn fetch_ckan_standard(
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

    if options.resume_offset > 0 {
        log::info!(
            "{}: {total_available} records available across {num_resources} resource(s) (resuming from offset {}, page size {})",
            config.label,
            options.resume_offset,
            config.page_size
        );
    } else if fetch_limit >= total_available {
        log::info!(
            "{}: {total_available} records available across {num_resources} resource(s) (fetching all, page size {})",
            config.label,
            config.page_size
        );
    } else {
        log::info!(
            "{}: {total_available} records available across {num_resources} resource(s) (fetching up to {fetch_limit}, page size {})",
            config.label,
            config.page_size
        );
    }

    let mut grand_total: u64 = 0;
    let mut skipped: u64 = 0;

    for (idx, resource_id) in config.resource_ids.iter().enumerate() {
        let remaining_global = fetch_limit.saturating_sub(grand_total);
        if remaining_global == 0 {
            break;
        }

        let resource_count = resource_counts[idx];

        // Resume: skip entire resources that were already ingested
        if options.resume_offset > 0
            && skipped < options.resume_offset
            && skipped + resource_count <= options.resume_offset
        {
            skipped += resource_count;
            log::info!(
                "{}: skipping resource {}/{num_resources} ({resource_count} records already ingested)",
                config.label,
                idx + 1,
            );
            continue;
        }

        // For the first non-skipped resource, apply the remaining resume offset
        let resource_resume = options.resume_offset.saturating_sub(skipped);
        skipped = options.resume_offset;

        log::info!(
            "{}: resource {}/{num_resources} — {resource_count} records",
            config.label,
            idx + 1,
        );

        let mut offset: u64 = resource_resume;

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

/// SQL-based CKAN fetch with date filtering via `datastore_search_sql`.
#[allow(clippy::too_many_lines)]
async fn fetch_ckan_sql(
    config: &CkanConfig<'_>,
    options: &FetchOptions,
    tx: &mpsc::Sender<Vec<serde_json::Value>>,
) -> Result<u64, SourceError> {
    let client = reqwest::Client::new();
    let fetch_limit = options.limit.unwrap_or(u64::MAX);
    let num_resources = config.resource_ids.len();
    let sql_url = sql_api_url(config.api_url);

    // These are guaranteed Some by the caller
    let date_column = config.date_column.unwrap_or("_id");
    let since = options.since.unwrap_or_default();
    let since_str = since.format("%Y-%m-%d %H:%M:%S").to_string();

    // Pre-fetch counts with the WHERE filter
    let mut resource_counts: Vec<u64> = Vec::with_capacity(num_resources);
    let mut total_available: u64 = 0;

    for resource_id in config.resource_ids {
        let count_sql = format!(
            "SELECT COUNT(*) as count FROM \"{resource_id}\" WHERE \"{date_column}\" >= '{since_str}'"
        );
        let response = client
            .get(&sql_url)
            .query(&[("sql", count_sql.as_str())])
            .send()
            .await?;
        let body: serde_json::Value = response.json().await?;
        let count = body
            .get("result")
            .and_then(|r| r.get("records"))
            .and_then(serde_json::Value::as_array)
            .and_then(|arr| arr.first())
            .and_then(|row| row.get("count"))
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(0);
        resource_counts.push(count);
        total_available += count;
    }

    if options.resume_offset > 0 {
        log::info!(
            "{}: {total_available} records available across {num_resources} resource(s) since {since_str} (resuming from offset {}, page size {})",
            config.label,
            options.resume_offset,
            config.page_size
        );
    } else if fetch_limit >= total_available {
        log::info!(
            "{}: {total_available} records available across {num_resources} resource(s) since {since_str} (fetching all, page size {})",
            config.label,
            config.page_size
        );
    } else {
        log::info!(
            "{}: {total_available} records available across {num_resources} resource(s) since {since_str} (fetching up to {fetch_limit}, page size {})",
            config.label,
            config.page_size
        );
    }

    let mut grand_total: u64 = 0;
    let mut skipped: u64 = 0;

    for (idx, resource_id) in config.resource_ids.iter().enumerate() {
        let remaining_global = fetch_limit.saturating_sub(grand_total);
        if remaining_global == 0 {
            break;
        }

        let resource_count = resource_counts[idx];

        // Resume: skip entire resources that were already ingested
        if options.resume_offset > 0
            && skipped < options.resume_offset
            && skipped + resource_count <= options.resume_offset
        {
            skipped += resource_count;
            log::info!(
                "{}: skipping resource {}/{num_resources} ({resource_count} records already ingested)",
                config.label,
                idx + 1,
            );
            continue;
        }

        // For the first non-skipped resource, apply the remaining resume offset
        let resource_resume = options.resume_offset.saturating_sub(skipped);
        skipped = options.resume_offset;

        log::info!(
            "{}: resource {}/{num_resources} — {resource_count} records (since {since_str})",
            config.label,
            idx + 1,
        );

        let mut offset: u64 = resource_resume;

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

            let mut sql = String::new();
            write!(
                sql,
                "SELECT * FROM \"{resource_id}\" WHERE \"{date_column}\" >= '{since_str}' ORDER BY \"{date_column}\" DESC LIMIT {page_limit} OFFSET {offset}"
            ).unwrap();

            let response = client
                .get(&sql_url)
                .query(&[("sql", sql.as_str())])
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
