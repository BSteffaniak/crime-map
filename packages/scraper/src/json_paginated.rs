//! Generic paginated JSON API scraper.
//!
//! Handles APIs that return JSON arrays or wrapped objects with pagination via
//! offset, page number, or cursor parameters.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::{ScrapeConfig, ScrapeError, ScrapedPage, Scraper};

/// The pagination strategy used by the API.
#[derive(Debug, Clone)]
pub enum PaginationType {
    /// Offset-based pagination (e.g. `?offset=100`).
    Offset {
        /// Query parameter name for the offset value.
        param: String,
    },
    /// Page-number-based pagination (e.g. `?page=2`).
    Page {
        /// Query parameter name for the page number.
        param: String,
    },
    /// Cursor-based pagination (e.g. `?cursor=abc123`).
    Cursor {
        /// Query parameter name for the cursor token.
        param: String,
    },
}

/// Describes how the JSON response body is structured.
#[derive(Debug, Clone)]
pub enum ResponseFormat {
    /// The response is a bare JSON array of records.
    BareArray,
    /// The records are nested inside an object at the given JSON-pointer path.
    Wrapped {
        /// Dot-separated path to the data array (e.g. `"results"` or
        /// `"data.items"`).
        data_path: String,
    },
}

/// Scraper for paginated JSON APIs.
///
/// Supports offset, page-number, and cursor-based pagination, as well as
/// both bare-array and wrapped response formats.
#[derive(Debug)]
pub struct JsonPaginatedScraper {
    /// Shared scrape configuration.
    config: ScrapeConfig,
    /// The pagination strategy to use.
    pagination: PaginationType,
    /// How to extract records from the JSON response.
    response_format: ResponseFormat,
    /// Optional query parameter name for the page size.
    page_size_param: Option<String>,
    /// Optional URL to query for the total record count.
    count_url: Option<String>,
    /// Dot-separated path within the count response to the total number.
    count_path: Option<String>,
    /// Tracks the cursor value for cursor-based pagination between pages.
    last_cursor: std::sync::Mutex<Option<String>>,
}

impl JsonPaginatedScraper {
    /// Creates a new `JsonPaginatedScraper` for the given URL with sensible
    /// defaults (offset-based pagination, bare array response, 100 records per
    /// page).
    #[must_use]
    pub fn new(url: &str) -> Self {
        Self {
            config: ScrapeConfig::new(url).with_page_size(100),
            pagination: PaginationType::Offset {
                param: "offset".to_owned(),
            },
            response_format: ResponseFormat::BareArray,
            page_size_param: None,
            count_url: None,
            count_path: None,
            last_cursor: std::sync::Mutex::new(None),
        }
    }

    /// Sets the pagination strategy.
    #[must_use]
    pub fn with_pagination(mut self, pagination: PaginationType) -> Self {
        self.pagination = pagination;
        self
    }

    /// Sets the response format.
    #[must_use]
    pub fn with_response_format(mut self, format: ResponseFormat) -> Self {
        self.response_format = format;
        self
    }

    /// Sets the query parameter name used to communicate page size.
    #[must_use]
    pub fn with_page_size_param(mut self, param: &str) -> Self {
        self.page_size_param = Some(param.to_owned());
        self
    }

    /// Sets the page size (number of records per page).
    #[must_use]
    pub const fn with_page_size(mut self, size: u32) -> Self {
        self.config.page_size = Some(size);
        self
    }

    /// Updates the page size for subsequent requests. Unlike
    /// [`with_page_size`](Self::with_page_size), this takes `&mut self`
    /// so it can be called after construction (e.g., to reduce page size
    /// after a fetch failure).
    pub const fn set_page_size(&mut self, size: u32) {
        self.config.page_size = Some(size);
    }

    /// Sets the URL to query for the total record count.
    #[must_use]
    pub fn with_count_url(mut self, url: &str) -> Self {
        self.count_url = Some(url.to_owned());
        self
    }

    /// Sets the dot-separated path to extract the count from the count
    /// response.
    #[must_use]
    pub fn with_count_path(mut self, path: &str) -> Self {
        self.count_path = Some(path.to_owned());
        self
    }

    /// Adds an HTTP header to include in requests.
    #[must_use]
    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.config.headers.insert(key.to_owned(), value.to_owned());
        self
    }

    /// Sets the maximum number of pages to fetch.
    #[must_use]
    pub const fn with_max_pages(mut self, max: u32) -> Self {
        self.config.max_pages = Some(max);
        self
    }

    /// Sets the delay between page fetches in milliseconds.
    #[must_use]
    pub const fn with_delay_ms(mut self, ms: u64) -> Self {
        self.config.delay_ms = Some(ms);
        self
    }

    /// Returns a reference to the underlying scrape configuration.
    #[must_use]
    pub const fn config(&self) -> &ScrapeConfig {
        &self.config
    }

    /// Builds a [`reqwest::Client`] with the configured headers.
    fn build_client(&self) -> Result<reqwest::Client, ScrapeError> {
        let mut header_map = reqwest::header::HeaderMap::new();
        for (key, value) in &self.config.headers {
            let name = reqwest::header::HeaderName::from_bytes(key.as_bytes())
                .map_err(|e| ScrapeError::Parse(format!("invalid header name '{key}': {e}")))?;
            let val = reqwest::header::HeaderValue::from_str(value)
                .map_err(|e| ScrapeError::Parse(format!("invalid header value '{value}': {e}")))?;
            header_map.insert(name, val);
        }
        reqwest::Client::builder()
            .default_headers(header_map)
            .build()
            .map_err(ScrapeError::Http)
    }

    /// Builds the full request URL for the given page number.
    fn build_url(&self, page: u32) -> String {
        let page_size = self.config.page_size.unwrap_or(100);
        let mut url = self.config.url.clone();

        let separator = if url.contains('?') { '&' } else { '?' };
        let mut first = true;

        let mut append = |key: &str, value: &str| {
            let sep = if first { separator } else { '&' };
            first = false;
            write!(url, "{sep}{key}={value}").unwrap();
        };

        // Add pagination parameter.
        match &self.pagination {
            PaginationType::Offset { param } => {
                let offset = u64::from(page) * u64::from(page_size);
                append(param, &offset.to_string());
            }
            PaginationType::Page { param } => {
                append(param, &page.to_string());
            }
            PaginationType::Cursor { param } => {
                let guard = self
                    .last_cursor
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(cursor) = guard.as_deref() {
                    append(param, cursor);
                }
            }
        }

        // Add page size parameter if configured.
        if let Some(ref size_param) = self.page_size_param {
            append(size_param, &page_size.to_string());
        }

        url
    }

    /// Navigates a dot-separated path into a [`serde_json::Value`].
    fn resolve_path<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
        let mut current = value;
        for segment in path.split('.') {
            current = current.get(segment)?;
        }
        Some(current)
    }

    /// Extracts the record array from a JSON response body according to the
    /// configured [`ResponseFormat`].
    fn extract_records(
        &self,
        body: &serde_json::Value,
    ) -> Result<Vec<serde_json::Value>, ScrapeError> {
        let array_value = match &self.response_format {
            ResponseFormat::BareArray => body,
            ResponseFormat::Wrapped { data_path } => Self::resolve_path(body, data_path)
                .ok_or_else(|| {
                    ScrapeError::Parse(format!("response does not contain path '{data_path}'"))
                })?,
        };

        array_value
            .as_array()
            .cloned()
            .ok_or_else(|| ScrapeError::Parse("expected JSON array of records".to_owned()))
    }

    /// Attempts to extract a cursor token from the response body for
    /// cursor-based pagination.
    fn extract_cursor(body: &serde_json::Value) -> Option<String> {
        // Try common cursor field names.
        for key in &["cursor", "next_cursor", "nextCursor", "next"] {
            if let Some(val) = body.get(*key)
                && let Some(s) = val.as_str()
                && !s.is_empty()
            {
                return Some(s.to_owned());
            }
        }
        None
    }
}

impl Scraper for JsonPaginatedScraper {
    async fn fetch_page(&self, page: u32) -> Result<ScrapedPage, ScrapeError> {
        let client = self.build_client()?;
        let url = self.build_url(page);

        log::debug!("Fetching JSON page {page}: {url}");

        let response = client.get(&url).send().await?.error_for_status()?;
        let body: serde_json::Value = response.json().await?;

        let records = self.extract_records(&body)?;
        let page_size = self.config.page_size.unwrap_or(100);
        let count = records.len();

        // ── Update cursor for next request ──────────────────────────────
        if matches!(self.pagination, PaginationType::Cursor { .. }) {
            let mut guard = self
                .last_cursor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *guard = Self::extract_cursor(&body);
        }

        // ── Determine whether more pages are available ──────────────────
        let has_more = if matches!(self.pagination, PaginationType::Cursor { .. }) {
            let guard = self
                .last_cursor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.is_some()
        } else {
            count >= page_size as usize
        };

        log::debug!("Page {page}: {count} records, has_more={has_more}");

        Ok(ScrapedPage {
            records,
            has_more,
            page_number: page,
        })
    }

    async fn total_count(&self) -> Option<u64> {
        let count_url = self.count_url.as_deref()?;
        let count_path = self.count_path.as_deref()?;

        let client = self.build_client().ok()?;
        let response = client.get(count_url).send().await.ok()?;
        let body: serde_json::Value = response.json().await.ok()?;

        let count_value = Self::resolve_path(&body, count_path)?;

        // Accept both numeric and string representations.
        count_value
            .as_u64()
            .or_else(|| count_value.as_str()?.parse::<u64>().ok())
    }

    fn strategy(&self) -> &'static str {
        "json_paginated"
    }
}

/// A pre-built set of header key-value pairs commonly needed for JSON APIs.
#[must_use]
pub fn default_json_headers() -> BTreeMap<String, String> {
    let mut headers = BTreeMap::new();
    headers.insert("Accept".to_owned(), "application/json".to_owned());
    headers
}
