//! HTML table fetcher bridge.
//!
//! Wraps [`crime_map_scraper::html_table::HtmlTableScraper`] to stream
//! pages through the ingest pipeline's [`tokio::sync::mpsc`] channel.

use std::collections::BTreeMap;
use std::sync::Arc;

use crime_map_scraper::Scraper;
use crime_map_scraper::html_table::HtmlTableScraper;
use tokio::sync::mpsc;

use crate::progress::ProgressCallback;
use crate::{FetchOptions, SourceError};

/// Configuration for the HTML table fetcher.
pub struct HtmlTableConfig<'a> {
    /// URL of the page containing the table.
    pub url: &'a str,
    /// Human-readable label for log messages.
    pub label: &'a str,
    /// CSS selector for the target table element.
    pub table_selector: Option<&'a str>,
    /// CSS selector for header cells.
    pub header_selector: Option<&'a str>,
    /// CSS selector for body rows.
    pub row_selector: Option<&'a str>,
    /// CSS selector for cells within a row.
    pub cell_selector: Option<&'a str>,
    /// Delay between page fetches in milliseconds.
    pub delay_ms: Option<u64>,
    /// Additional HTTP headers.
    pub headers: &'a BTreeMap<String, String>,
}

/// Fetches records from an HTML table and sends them through the channel.
///
/// # Errors
///
/// Returns [`SourceError`] if the HTTP request or HTML parsing fails.
pub async fn fetch_html_table(
    config: &HtmlTableConfig<'_>,
    _options: &FetchOptions,
    tx: &mpsc::Sender<Vec<serde_json::Value>>,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<u64, SourceError> {
    let mut scraper = HtmlTableScraper::new(config.url);

    if let Some(sel) = config.table_selector {
        scraper = scraper.with_table_selector(sel);
    }
    if let Some(sel) = config.header_selector {
        scraper = scraper.with_header_row_selector(sel);
    }
    if let Some(sel) = config.row_selector {
        scraper = scraper.with_body_row_selector(sel);
    }
    if let Some(sel) = config.cell_selector {
        scraper = scraper.with_cell_selector(sel);
    }
    for (key, value) in config.headers {
        scraper = scraper.with_header(key, value);
    }

    log::info!("[{}] Fetching HTML table from {}", config.label, config.url);

    let page = scraper.fetch_page(0).await?;
    let count = page.records.len() as u64;
    progress.inc(count);

    if !page.records.is_empty() {
        tx.send(page.records).await.ok();
    }

    log::info!("[{}] Fetched {count} records from HTML table", config.label);
    progress.finish(format!(
        "[{}] download complete -- {count} records",
        config.label
    ));

    Ok(count)
}
