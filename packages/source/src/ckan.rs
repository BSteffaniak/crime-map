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

    loop {
        let remaining = fetch_limit.saturating_sub(offset);
        if remaining == 0 {
            break;
        }
        let page_limit = remaining.min(config.page_size);

        log::info!(
            "Fetching {} data: offset={offset}, limit={page_limit}",
            config.label
        );

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
        "Downloaded {} {} records total",
        all_records.len(),
        config.label
    );
    let json = serde_json::to_string(&all_records)?;
    std::fs::write(&output_path, json)?;

    Ok(output_path)
}
