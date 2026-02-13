//! CSV download fetcher bridge.
//!
//! Wraps [`crime_map_scraper::csv_download::CsvDownloadScraper`] to stream
//! pages through the ingest pipeline's [`tokio::sync::mpsc`] channel.

use std::collections::BTreeMap;
use std::sync::Arc;

use crime_map_scraper::Scraper;
use crime_map_scraper::csv_download::CsvDownloadScraper;
use tokio::sync::mpsc;

use crate::progress::ProgressCallback;
use crate::{FetchOptions, SourceError};

/// Configuration for the CSV download fetcher.
pub struct CsvDownloadConfig<'a> {
    /// URLs of CSV files to download.
    pub urls: &'a [String],
    /// Human-readable label for log messages.
    pub label: &'a str,
    /// Field delimiter (default: comma).
    pub delimiter: Option<&'a str>,
    /// Compression format (`"gzip"` or `None`).
    pub compressed: Option<&'a str>,
    /// Maximum records per CSV file.
    pub max_records: Option<u64>,
    /// Additional HTTP headers.
    pub headers: &'a BTreeMap<String, String>,
}

/// Fetches records from CSV file downloads and sends them through the
/// channel.
///
/// # Errors
///
/// Returns [`SourceError`] if any download or CSV parsing fails.
pub async fn fetch_csv_download(
    config: &CsvDownloadConfig<'_>,
    options: &FetchOptions,
    tx: &mpsc::Sender<Vec<serde_json::Value>>,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<u64, SourceError> {
    let mut total: u64 = 0;

    for (i, url) in config.urls.iter().enumerate() {
        log::info!(
            "[{}] Downloading CSV {}/{}: {url}",
            config.label,
            i + 1,
            config.urls.len()
        );

        let mut scraper = CsvDownloadScraper::new(url);

        if let Some(delim) = config.delimiter
            && let Some(byte) = delim.as_bytes().first()
        {
            scraper = scraper.with_delimiter(*byte);
        }

        if config.compressed == Some("gzip") {
            scraper = scraper.with_gzip(true);
        }

        // Apply limit: cap max_records per file by remaining budget
        let remaining = options.limit.map(|l| l.saturating_sub(total));
        let file_limit = match (config.max_records, remaining) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        if let Some(limit) = file_limit {
            scraper = scraper.with_max_records(limit);
        }

        for (key, value) in config.headers {
            scraper = scraper.with_header(key, value);
        }

        let page = scraper.fetch_page(0).await?;
        let count = page.records.len() as u64;
        total += count;
        progress.inc(count);

        if !page.records.is_empty() {
            tx.send(page.records).await.ok();
        }

        log::info!(
            "[{}] CSV {}/{}: {count} records (total so far: {total})",
            config.label,
            i + 1,
            config.urls.len()
        );

        // Stop if we've hit the overall limit
        if let Some(limit) = options.limit
            && total >= limit
        {
            log::info!("[{}] Reached limit of {limit} records", config.label);
            break;
        }
    }

    log::info!("[{}] CSV download complete — {total} records", config.label);
    progress.finish(format!(
        "[{}] download complete -- {total} records",
        config.label
    ));

    Ok(total)
}
