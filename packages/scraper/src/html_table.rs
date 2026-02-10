//! HTML table scraper.
//!
//! Fetches an HTML page, locates a `<table>` element via CSS selector, and
//! extracts each row into a [`serde_json::Value`] object keyed by the column
//! headers found in the `<thead>`.

use scraper::{Html, Selector};

use crate::{ScrapeConfig, ScrapeError, ScrapedPage, Scraper};

/// Scraper that extracts records from an HTML table.
///
/// The default selectors work with standard `<table>` / `<thead>` / `<tbody>`
/// markup. Use the builder methods to customise selectors for non-standard
/// layouts.
#[derive(Debug, Clone)]
pub struct HtmlTableScraper {
    /// Shared scrape configuration (URL, headers, etc.).
    config: ScrapeConfig,
    /// CSS selector for the target table element.
    table_selector: String,
    /// CSS selector for header cells inside the table.
    header_row_selector: String,
    /// CSS selector for body rows inside the table.
    body_row_selector: String,
    /// CSS selector for cells within a body row.
    cell_selector: String,
}

impl HtmlTableScraper {
    /// Creates a new `HtmlTableScraper` for the given URL with default CSS
    /// selectors.
    #[must_use]
    pub fn new(url: &str) -> Self {
        Self {
            config: ScrapeConfig::new(url),
            table_selector: "table".to_owned(),
            header_row_selector: "thead tr th, thead tr td".to_owned(),
            body_row_selector: "tbody tr".to_owned(),
            cell_selector: "td".to_owned(),
        }
    }

    /// Overrides the CSS selector used to locate the table element.
    #[must_use]
    pub fn with_table_selector(mut self, selector: &str) -> Self {
        selector.clone_into(&mut self.table_selector);
        self
    }

    /// Overrides the CSS selector used to locate header cells.
    #[must_use]
    pub fn with_header_row_selector(mut self, selector: &str) -> Self {
        selector.clone_into(&mut self.header_row_selector);
        self
    }

    /// Overrides the CSS selector used to locate body rows.
    #[must_use]
    pub fn with_body_row_selector(mut self, selector: &str) -> Self {
        selector.clone_into(&mut self.body_row_selector);
        self
    }

    /// Overrides the CSS selector used to locate cells within a body row.
    #[must_use]
    pub fn with_cell_selector(mut self, selector: &str) -> Self {
        selector.clone_into(&mut self.cell_selector);
        self
    }

    /// Adds an HTTP header to include in the request.
    #[must_use]
    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.config.headers.insert(key.to_owned(), value.to_owned());
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

    /// Parses a CSS selector string, returning a [`ScrapeError`] on failure.
    fn parse_selector(selector: &str) -> Result<Selector, ScrapeError> {
        Selector::parse(selector)
            .map_err(|e| ScrapeError::Parse(format!("invalid CSS selector '{selector}': {e}")))
    }
}

impl Scraper for HtmlTableScraper {
    async fn fetch_page(&self, page: u32) -> Result<ScrapedPage, ScrapeError> {
        let client = self.build_client()?;

        let url = if page == 0 {
            self.config.url.clone()
        } else {
            // Append a `page` query parameter for subsequent pages.
            let separator = if self.config.url.contains('?') {
                '&'
            } else {
                '?'
            };
            format!("{}{separator}page={page}", self.config.url)
        };

        let response = client.get(&url).send().await?.error_for_status()?;
        let body = response.text().await?;
        let document = Html::parse_document(&body);

        // ── Locate the table ────────────────────────────────────────────
        let table_sel = Self::parse_selector(&self.table_selector)?;
        let table_element = document.select(&table_sel).next().ok_or_else(|| {
            ScrapeError::Parse(format!(
                "no element matching '{}' found in response",
                self.table_selector
            ))
        })?;

        // ── Extract headers ─────────────────────────────────────────────
        let header_sel = Self::parse_selector(&self.header_row_selector)?;
        let headers: Vec<String> = table_element
            .select(&header_sel)
            .map(|el| el.text().collect::<Vec<_>>().join("").trim().to_owned())
            .collect();

        if headers.is_empty() {
            return Err(ScrapeError::Parse(
                "no header cells found in table".to_owned(),
            ));
        }

        // ── Extract body rows ───────────────────────────────────────────
        let row_sel = Self::parse_selector(&self.body_row_selector)?;
        let cell_sel = Self::parse_selector(&self.cell_selector)?;

        let mut records: Vec<serde_json::Value> = Vec::new();

        for row in table_element.select(&row_sel) {
            let cells: Vec<String> = row
                .select(&cell_sel)
                .map(|el| el.text().collect::<Vec<_>>().join("").trim().to_owned())
                .collect();

            let mut map = serde_json::Map::new();
            for (i, header) in headers.iter().enumerate() {
                let value = cells.get(i).cloned().unwrap_or_default();
                map.insert(header.clone(), serde_json::Value::String(value));
            }

            records.push(serde_json::Value::Object(map));
        }

        // HTML tables are typically returned in full on a single page.
        // If the page param was used and we got zero rows, treat it as
        // end-of-data.
        Ok(ScrapedPage {
            has_more: false,
            page_number: page,
            records,
        })
    }

    fn strategy(&self) -> &'static str {
        "html_table"
    }
}
