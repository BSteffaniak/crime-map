#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Generic web scraping framework for crime data sources.
//!
//! Provides the [`Scraper`] trait and concrete implementations for common
//! data formats: HTML tables ([`html_table`]), CSV downloads ([`csv_download`]),
//! and paginated JSON APIs ([`json_paginated`]).
//!
//! This crate is a pure scraping library with no awareness of the discovery
//! database. It fetches and normalises raw records into [`serde_json::Value`]
//! objects that callers can store however they like.

pub mod csv_download;
pub mod html_table;
pub mod json_paginated;

use std::collections::BTreeMap;

/// Errors that can occur during scraping operations.
#[derive(Debug, thiserror::Error)]
pub enum ScrapeError {
    /// An HTTP request failed.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// Parsing the response body failed.
    #[error("Parse error: {0}")]
    Parse(String),

    /// An I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// CSV parsing failed.
    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),

    /// The data source uses a format this scraper does not support.
    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),
}

/// A single page of scraped records.
#[derive(Debug, Clone)]
pub struct ScrapedPage {
    /// The records extracted from this page.
    pub records: Vec<serde_json::Value>,
    /// Whether there are more pages available after this one.
    pub has_more: bool,
    /// The zero-indexed page number of this result set.
    pub page_number: u32,
}

/// Configuration shared across scraper implementations.
#[derive(Debug, Clone)]
pub struct ScrapeConfig {
    /// The URL to scrape.
    pub url: String,
    /// Additional HTTP headers to include in requests.
    pub headers: BTreeMap<String, String>,
    /// Number of records per page (used by paginated scrapers).
    pub page_size: Option<u32>,
    /// Maximum number of pages to fetch.
    pub max_pages: Option<u32>,
    /// Delay in milliseconds between page fetches.
    pub delay_ms: Option<u64>,
}

impl ScrapeConfig {
    /// Creates a new `ScrapeConfig` with the given URL and sensible defaults.
    #[must_use]
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_owned(),
            headers: BTreeMap::new(),
            page_size: None,
            max_pages: None,
            delay_ms: None,
        }
    }

    /// Sets the page size.
    #[must_use]
    pub const fn with_page_size(mut self, size: u32) -> Self {
        self.page_size = Some(size);
        self
    }

    /// Sets the maximum number of pages to fetch.
    #[must_use]
    pub const fn with_max_pages(mut self, max: u32) -> Self {
        self.max_pages = Some(max);
        self
    }

    /// Sets the delay between page fetches.
    #[must_use]
    pub const fn with_delay_ms(mut self, ms: u64) -> Self {
        self.delay_ms = Some(ms);
        self
    }

    /// Adds an HTTP header to include in requests.
    #[must_use]
    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_owned(), value.to_owned());
        self
    }
}

/// Trait for fetching structured data from a web source.
///
/// Implementations handle a specific scraping strategy (HTML table parsing,
/// CSV download, paginated JSON API, etc.) and return normalised records
/// as [`serde_json::Value`] objects.
pub trait Scraper: Send + Sync {
    /// Fetches a single page of records.
    ///
    /// # Errors
    ///
    /// Returns [`ScrapeError`] if the HTTP request or response parsing fails.
    fn fetch_page(
        &self,
        page: u32,
    ) -> impl std::future::Future<Output = Result<ScrapedPage, ScrapeError>> + Send;

    /// Returns the total record count if knowable before fetching.
    ///
    /// The default implementation returns `None`.
    fn total_count(&self) -> impl std::future::Future<Output = Option<u64>> + Send {
        async { None }
    }

    /// Returns the name of the scraping strategy (e.g. `"html_table"`,
    /// `"csv_download"`, `"json_paginated"`).
    fn strategy(&self) -> &str;
}

/// Fetches all pages from a scraper sequentially, with an optional delay
/// between requests.
///
/// # Errors
///
/// Returns the first [`ScrapeError`] encountered during fetching.
pub async fn scrape_all(
    scraper: &(impl Scraper + ?Sized),
    max_pages: Option<u32>,
    delay_ms: Option<u64>,
) -> Result<Vec<serde_json::Value>, ScrapeError> {
    let mut all_records = Vec::new();
    let mut page: u32 = 0;

    loop {
        if let Some(max) = max_pages
            && page >= max
        {
            log::info!("Reached max pages ({max}), stopping");
            break;
        }

        log::debug!("Fetching page {page}");
        let result = scraper.fetch_page(page).await?;
        let has_more = result.has_more;

        all_records.extend(result.records);

        if !has_more {
            break;
        }

        page += 1;

        if let Some(ms) = delay_ms {
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        }
    }

    log::info!("Scrape complete — {} total records", all_records.len());
    Ok(all_records)
}
