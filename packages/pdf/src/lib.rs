#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! PDF table extraction for crime data sources.
//!
//! Some police departments only publish crime data as PDF bulletins or
//! reports.  This crate extracts structured records from those PDFs using
//! pure-Rust text extraction ([`pdf_extract`]) combined with configurable
//! parsing strategies (regex row matching, column-position tables, or
//! line-delimited records).
//!
//! The primary entry point is [`PdfScraper`], which implements the
//! [`crime_map_scraper::Scraper`] trait so it integrates seamlessly with
//! the rest of the scraping pipeline.

pub mod download;
pub mod regex_rows;
pub mod text_table;

use std::collections::BTreeMap;

use crime_map_scraper::{ScrapeError, ScrapedPage, Scraper};

/// Errors specific to PDF extraction.
#[derive(Debug, thiserror::Error)]
pub enum PdfError {
    /// An HTTP request to download a PDF failed.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// PDF text extraction failed.
    #[error("PDF extraction error: {0}")]
    Extraction(String),

    /// The configured regex pattern failed to compile.
    #[error("Invalid regex pattern: {0}")]
    Regex(#[from] regex::Error),

    /// An I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<PdfError> for ScrapeError {
    fn from(e: PdfError) -> Self {
        Self::Parse(e.to_string())
    }
}

/// Strategy for extracting structured records from PDF text.
#[derive(Debug, Clone)]
pub enum ExtractionStrategy {
    /// Match each record using a regex with named capture groups.
    ///
    /// The regex is applied against the full extracted text and each match
    /// becomes a JSON object keyed by the capture group names.
    RegexRows {
        /// Regex pattern with named groups (e.g.
        /// `(?P<date>\d{2}/\d{2}/\d{4})\s+(?P<type>\w+)`).
        pattern: String,
    },

    /// Extract records from space-aligned columnar text using fixed column
    /// boundaries.
    TextTable {
        /// Character positions where each column starts.  For example,
        /// `[0, 12, 30, 55]` means column 1 is chars 0–11, column 2 is
        /// chars 12–29, etc.
        column_starts: Vec<usize>,
        /// Column header names (one per column boundary).
        column_names: Vec<String>,
        /// Number of header lines to skip at the top of each page.
        skip_header_lines: usize,
    },

    /// Each non-empty line is a record with fields separated by a
    /// delimiter.
    LineDelimited {
        /// Field delimiter (e.g. `"|"` or `"\t"`).
        delimiter: String,
        /// Column header names.
        column_names: Vec<String>,
        /// Number of header lines to skip at the top of each page.
        skip_header_lines: usize,
    },
}

/// A scraper that downloads PDF files and extracts structured records.
///
/// Implements [`Scraper`] so it can be used in the same pipeline as
/// HTML table, CSV, and JSON scrapers.
#[derive(Debug)]
pub struct PdfScraper {
    /// URLs of PDF files to download (one "page" per URL).
    urls: Vec<String>,
    /// How to extract records from the PDF text.
    strategy: ExtractionStrategy,
    /// Additional HTTP headers for the download requests.
    headers: BTreeMap<String, String>,
}

impl PdfScraper {
    /// Creates a new `PdfScraper` with the given URLs and extraction
    /// strategy.
    #[must_use]
    pub const fn new(urls: Vec<String>, strategy: ExtractionStrategy) -> Self {
        Self {
            urls,
            strategy,
            headers: BTreeMap::new(),
        }
    }

    /// Adds an HTTP header to include in download requests.
    #[must_use]
    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_owned(), value.to_owned());
        self
    }

    /// Builds a [`reqwest::Client`] with the configured headers.
    fn build_client(&self) -> Result<reqwest::Client, PdfError> {
        let mut header_map = reqwest::header::HeaderMap::new();
        for (key, value) in &self.headers {
            let name = reqwest::header::HeaderName::from_bytes(key.as_bytes())
                .map_err(|e| PdfError::Extraction(format!("invalid header name '{key}': {e}")))?;
            let val = reqwest::header::HeaderValue::from_str(value).map_err(|e| {
                PdfError::Extraction(format!("invalid header value '{value}': {e}"))
            })?;
            header_map.insert(name, val);
        }
        reqwest::Client::builder()
            .default_headers(header_map)
            .build()
            .map_err(PdfError::Http)
    }

    /// Downloads a PDF and extracts its text content.
    async fn download_and_extract_text(&self, url: &str) -> Result<String, PdfError> {
        let client = self.build_client()?;
        let response = client.get(url).send().await?.error_for_status()?;
        let bytes = response.bytes().await?;

        log::debug!("Downloaded {} bytes from {url}", bytes.len());

        let text = pdf_extract::extract_text_from_mem(&bytes)
            .map_err(|e| PdfError::Extraction(format!("failed to extract text from PDF: {e}")))?;

        log::debug!("Extracted {} characters of text from {url}", text.len());

        Ok(text)
    }

    /// Extracts records from text using the configured strategy.
    fn extract_records(&self, text: &str) -> Result<Vec<serde_json::Value>, PdfError> {
        match &self.strategy {
            ExtractionStrategy::RegexRows { pattern } => regex_rows::extract(text, pattern),
            ExtractionStrategy::TextTable {
                column_starts,
                column_names,
                skip_header_lines,
            } => Ok(text_table::extract(
                text,
                column_starts,
                column_names,
                *skip_header_lines,
            )),
            ExtractionStrategy::LineDelimited {
                delimiter,
                column_names,
                skip_header_lines,
            } => Ok(text_table::extract_delimited(
                text,
                delimiter,
                column_names,
                *skip_header_lines,
            )),
        }
    }
}

impl Scraper for PdfScraper {
    async fn fetch_page(&self, page: u32) -> Result<ScrapedPage, ScrapeError> {
        let idx = page as usize;
        if idx >= self.urls.len() {
            return Ok(ScrapedPage {
                records: Vec::new(),
                has_more: false,
                page_number: page,
            });
        }

        let url = &self.urls[idx];
        let text = self.download_and_extract_text(url).await?;
        let records = self.extract_records(&text)?;

        log::info!("Extracted {} records from PDF {url}", records.len());

        Ok(ScrapedPage {
            records,
            has_more: idx + 1 < self.urls.len(),
            page_number: page,
        })
    }

    fn strategy(&self) -> &'static str {
        "pdf_extract"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regex_extraction_works() {
        let text = "01/15/2024  THEFT  100 Main St\n02/20/2024  ASSAULT  200 Oak Ave\n";
        let pattern = r"(?P<date>\d{2}/\d{2}/\d{4})\s+(?P<type>\w+)\s+(?P<address>.+)";
        let records = regex_rows::extract(text, pattern).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["date"], "01/15/2024");
        assert_eq!(records[0]["type"], "THEFT");
        assert_eq!(records[0]["address"], "100 Main St");
    }

    #[test]
    fn text_table_extraction_works() {
        let text = "Date        Type      Address\n01/15/2024  THEFT     100 Main St\n02/20/2024  ASSAULT   200 Oak Ave\n";
        let column_starts = vec![0, 12, 22];
        let column_names = vec!["date".to_owned(), "type".to_owned(), "address".to_owned()];
        let records = text_table::extract(text, &column_starts, &column_names, 1);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["date"], "01/15/2024");
        assert_eq!(records[0]["type"], "THEFT");
        assert_eq!(records[0]["address"], "100 Main St");
    }

    #[test]
    fn line_delimited_extraction_works() {
        let text =
            "date|type|address\n01/15/2024|THEFT|100 Main St\n02/20/2024|ASSAULT|200 Oak Ave\n";
        let column_names = vec!["date".to_owned(), "type".to_owned(), "address".to_owned()];
        let records = text_table::extract_delimited(text, "|", &column_names, 1);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["date"], "01/15/2024");
        assert_eq!(records[0]["type"], "THEFT");
        assert_eq!(records[0]["address"], "100 Main St");
    }
}
