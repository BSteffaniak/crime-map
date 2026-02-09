//! Shared Socrata SODA API fetcher.
//!
//! Handles paginated fetching from any Socrata dataset using the `$limit`,
//! `$offset`, `$order`, and `$where` query parameters. Used by Chicago, LA,
//! SF, Seattle, and NYC sources.

use std::fmt::Write as _;
use std::path::PathBuf;

use crate::{FetchOptions, SourceError};

/// Configuration for a Socrata fetch operation.
pub struct SocrataConfig<'a> {
    /// Base API URL (e.g., `"https://data.lacity.org/resource/2nrs-mtv8.json"`).
    pub api_url: &'a str,
    /// The date column name for ordering and `$where` filtering (e.g., `"date"`,
    /// `"date_occ"`).
    pub date_column: &'a str,
    /// Output filename (e.g., `"chicago_crimes.json"`).
    pub output_filename: &'a str,
    /// Label for log messages (e.g., `"Chicago"`).
    pub label: &'a str,
    /// Page size for pagination (default 50,000).
    pub page_size: u64,
}

/// Fetches all records from a Socrata dataset with pagination, writes to a
/// JSON file, and returns the output path.
///
/// # Errors
///
/// Returns [`SourceError`] if HTTP requests or file I/O fail.
pub async fn fetch_socrata(
    config: &SocrataConfig<'_>,
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

        let mut url = format!(
            "{}?$limit={}&$offset={}&$order={} DESC",
            config.api_url, page_limit, offset, config.date_column
        );

        if let Some(since) = &options.since {
            let since_str = since.format("%Y-%m-%dT%H:%M:%S").to_string();
            write!(url, "&$where={} > '{since_str}'", config.date_column).unwrap();
        }

        log::info!(
            "Fetching {} data: offset={offset}, limit={page_limit}",
            config.label
        );
        let response = client.get(&url).send().await?;
        let records: Vec<serde_json::Value> = response.json().await?;

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
