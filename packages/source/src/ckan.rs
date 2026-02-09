//! Shared CKAN Datastore API fetcher.
//!
//! Handles paginated fetching from CKAN `datastore_search` endpoints.
//! Used by Boston.

use std::path::PathBuf;

use crate::{FetchOptions, SourceError};

/// Configuration for a CKAN fetch operation.
pub struct CkanConfig<'a> {
    /// Base API URL (e.g.,
    /// `"https://data.boston.gov/api/3/action/datastore_search"`).
    pub api_url: &'a str,
    /// CKAN resource ID for the dataset.
    pub resource_id: &'a str,
    /// Output filename (e.g., `"boston_crimes.json"`).
    pub output_filename: &'a str,
    /// Label for log messages (e.g., `"Boston"`).
    pub label: &'a str,
    /// Page size for pagination.
    pub page_size: u64,
}

/// Fetches all records from a CKAN Datastore endpoint with pagination,
/// writes to a JSON file, and returns the output path.
///
/// # Errors
///
/// Returns [`SourceError`] if HTTP requests or file I/O fail.
pub async fn fetch_ckan(
    config: &CkanConfig<'_>,
    options: &FetchOptions,
) -> Result<PathBuf, SourceError> {
    let output_path = options.output_dir.join(config.output_filename);
    std::fs::create_dir_all(&options.output_dir)?;

    let client = reqwest::Client::new();
    let mut all_records: Vec<serde_json::Value> = Vec::new();
    let mut offset: u64 = 0;
    let fetch_limit = options.limit.unwrap_or(u64::MAX);
    let mut total_available: Option<u64> = None;
    let mut will_fetch: Option<u64> = None;

    loop {
        let remaining = fetch_limit.saturating_sub(offset);
        if remaining == 0 {
            break;
        }
        let page_limit = remaining.min(config.page_size);

        if let Some(target) = will_fetch {
            log::info!(
                "{}: page {} — {offset} / {target} fetched",
                config.label,
                (offset / config.page_size) + 1,
            );
        } else {
            log::info!("{}: fetching first page...", config.label);
        }

        let response = client
            .get(config.api_url)
            .query(&[
                ("resource_id", config.resource_id),
                ("limit", &page_limit.to_string()),
                ("offset", &offset.to_string()),
            ])
            .send()
            .await?;
        let body: serde_json::Value = response.json().await?;

        // CKAN includes the total count in every response — capture it from
        // the first page so we can log progress on subsequent pages.
        if total_available.is_none() {
            total_available = body
                .get("result")
                .and_then(|r| r.get("total"))
                .and_then(serde_json::Value::as_u64);

            if let Some(total) = total_available {
                will_fetch = Some(fetch_limit.min(total));
                if fetch_limit >= total {
                    log::info!("{}: {total} records available (fetching all)", config.label);
                } else {
                    log::info!(
                        "{}: {total} records available (fetching up to {fetch_limit})",
                        config.label
                    );
                }
            }
        }

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

        all_records.extend(records);
        offset += count;

        if count < page_limit {
            break;
        }
    }

    log::info!(
        "{}: download complete — {} records",
        config.label,
        all_records.len(),
    );
    let json = serde_json::to_string(&all_records)?;
    std::fs::write(&output_path, json)?;

    Ok(output_path)
}
