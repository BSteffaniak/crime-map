//! Shared Carto SQL API fetcher.
//!
//! Handles paginated fetching from Carto SQL endpoints. Used by Philadelphia.

use std::fmt::Write as _;
use std::path::PathBuf;

use crate::{FetchOptions, SourceError};

/// Configuration for a Carto SQL fetch operation.
pub struct CartoConfig<'a> {
    /// Base Carto SQL API URL (e.g., `"https://phl.carto.com/api/v2/sql"`).
    pub api_url: &'a str,
    /// Table name to query (e.g., `"incidents_part1_part2"`).
    pub table_name: &'a str,
    /// Date column for ordering and `WHERE` filtering.
    pub date_column: &'a str,
    /// Output filename (e.g., `"philly_crimes.json"`).
    pub output_filename: &'a str,
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

/// Fetches all records from a Carto SQL endpoint with pagination, writes to
/// a JSON file, and returns the output path.
///
/// # Errors
///
/// Returns [`SourceError`] if HTTP requests or file I/O fail.
pub async fn fetch_carto(
    config: &CartoConfig<'_>,
    options: &FetchOptions,
) -> Result<PathBuf, SourceError> {
    let output_path = options.output_dir.join(config.output_filename);
    std::fs::create_dir_all(&options.output_dir)?;

    let client = reqwest::Client::new();
    let mut all_records: Vec<serde_json::Value> = Vec::new();
    let mut offset: u64 = 0;
    let fetch_limit = options.limit.unwrap_or(u64::MAX);

    // ── Pre-fetch count ──────────────────────────────────────────────
    let total_available = query_carto_count(&client, config, options).await;

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

        all_records.extend(rows);
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
