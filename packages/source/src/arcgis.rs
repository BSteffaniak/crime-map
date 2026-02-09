//! Shared `ArcGIS` REST API fetcher.
//!
//! Handles paginated fetching from `ArcGIS` `FeatureServer` or `MapServer`
//! endpoints. Supports multiple query URLs (e.g., one per year/layer) and
//! merges results into a single output file. Used by DC.

use std::path::PathBuf;

use crate::{FetchOptions, SourceError};

/// Configuration for an `ArcGIS` fetch operation.
pub struct ArcGisConfig<'a> {
    /// Query URLs to fetch from (one per layer/year). Each URL is fetched
    /// with pagination and results are merged.
    pub query_urls: &'a [String],
    /// Output filename (e.g., `"dc_crimes.json"`).
    pub output_filename: &'a str,
    /// Label for log messages (e.g., `"DC"`).
    pub label: &'a str,
    /// Max records per request (often 1000 or 2000).
    pub page_size: u64,
    /// Optional `where` clause (e.g., `"REPORT_DAT >= '2020-01-01'"`).
    /// Defaults to `"1=1"` if `None`.
    pub where_clause: Option<&'a str>,
}

/// Fetches all features from one or more `ArcGIS` REST endpoints with
/// pagination, writes to a JSON file, and returns the output path.
///
/// When multiple `query_urls` are configured (e.g., per-year layers), each URL
/// is fetched independently and all features are merged into a single output
/// file.
///
/// # Errors
///
/// Returns [`SourceError`] if HTTP requests or file I/O fail.
pub async fn fetch_arcgis(
    config: &ArcGisConfig<'_>,
    options: &FetchOptions,
) -> Result<PathBuf, SourceError> {
    let output_path = options.output_dir.join(config.output_filename);
    std::fs::create_dir_all(&options.output_dir)?;

    let client = reqwest::Client::new();
    let mut all_features: Vec<serde_json::Value> = Vec::new();
    let fetch_limit = options.limit.unwrap_or(u64::MAX);
    let where_clause = config.where_clause.unwrap_or("1=1");

    for query_url in config.query_urls {
        let mut offset: u64 = 0;
        let remaining_global =
            fetch_limit.saturating_sub(u64::try_from(all_features.len()).unwrap_or(u64::MAX));
        if remaining_global == 0 {
            break;
        }

        loop {
            let total_fetched = u64::try_from(all_features.len()).unwrap_or(u64::MAX);
            let remaining = fetch_limit.saturating_sub(total_fetched);
            if remaining == 0 {
                break;
            }
            let page_limit = remaining.min(config.page_size);

            let url = format!(
                "{query_url}?where={where_clause}&outFields=*&f=json&outSR=4326&resultRecordCount={page_limit}&resultOffset={offset}"
            );

            log::info!(
                "Fetching {} data: offset={offset}, limit={page_limit} (url={})",
                config.label,
                query_url
            );
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

            // ArcGIS wraps attributes in { "attributes": {...}, "geometry": {...} }
            // We flatten to just the attributes for simpler deserialization
            for feature in &features {
                if let Some(attrs) = feature.get("attributes") {
                    all_features.push(attrs.clone());
                }
            }

            offset += count;

            if count < page_limit {
                break;
            }
        }
    }

    log::info!(
        "Downloaded {} {} records total",
        all_features.len(),
        config.label
    );
    let json = serde_json::to_string(&all_features)?;
    std::fs::write(&output_path, json)?;

    Ok(output_path)
}
