//! CSV file downloader and parser.
//!
//! Downloads a CSV (optionally gzip-compressed) from a URL, parses it, and
//! returns every row as a [`serde_json::Value`] object keyed by the column
//! headers in the first row.

use std::collections::BTreeMap;
use std::io::Read as _;

use crate::{ScrapeError, ScrapedPage, Scraper};

/// Scraper that downloads and parses a CSV file.
///
/// All records are returned in a single [`ScrapedPage`] with `has_more` set to
/// `false`, since the entire file is downloaded at once.
#[derive(Debug, Clone)]
pub struct CsvDownloadScraper {
    /// URL of the CSV file to download.
    url: String,
    /// Additional HTTP headers for the download request.
    headers: BTreeMap<String, String>,
    /// Whether the response body is gzip-compressed.
    is_gzipped: bool,
    /// Field delimiter byte (defaults to `,`).
    delimiter: u8,
    /// Optional cap on the number of records to parse.
    max_records: Option<u64>,
}

impl CsvDownloadScraper {
    /// Creates a new `CsvDownloadScraper` for the given URL with default
    /// settings (comma-delimited, not gzipped, no record limit).
    #[must_use]
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_owned(),
            headers: BTreeMap::new(),
            is_gzipped: false,
            delimiter: b',',
            max_records: None,
        }
    }

    /// Marks the download as gzip-compressed so that the response body will be
    /// decompressed before CSV parsing.
    #[must_use]
    pub const fn with_gzip(mut self, gzipped: bool) -> Self {
        self.is_gzipped = gzipped;
        self
    }

    /// Sets the field delimiter (e.g. `b'\t'` for TSV files).
    #[must_use]
    pub const fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Limits the number of records that will be parsed from the file.
    #[must_use]
    pub const fn with_max_records(mut self, max: u64) -> Self {
        self.max_records = Some(max);
        self
    }

    /// Adds an HTTP header to include in the download request.
    #[must_use]
    pub fn with_header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_owned(), value.to_owned());
        self
    }

    /// Builds a [`reqwest::Client`] with the configured headers.
    fn build_client(&self) -> Result<reqwest::Client, ScrapeError> {
        let mut header_map = reqwest::header::HeaderMap::new();
        for (key, value) in &self.headers {
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
}

impl Scraper for CsvDownloadScraper {
    async fn fetch_page(&self, page: u32) -> Result<ScrapedPage, ScrapeError> {
        // CSV downloads are single-page: only page 0 contains data.
        if page > 0 {
            return Ok(ScrapedPage {
                records: Vec::new(),
                has_more: false,
                page_number: page,
            });
        }

        let client = self.build_client()?;
        let response = client.get(&self.url).send().await?.error_for_status()?;
        let bytes = response.bytes().await?;

        log::debug!("Downloaded {} bytes from {}", bytes.len(), self.url);

        // ── Decompress if needed ────────────────────────────────────────
        let csv_bytes: Vec<u8> = if self.is_gzipped {
            let mut decoder = flate2::read::GzDecoder::new(&bytes[..]);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;
            log::debug!("Decompressed to {} bytes", decompressed.len());
            decompressed
        } else {
            bytes.to_vec()
        };

        // ── Parse CSV ───────────────────────────────────────────────────
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .flexible(true)
            .from_reader(csv_bytes.as_slice());

        let csv_headers: Vec<String> = reader
            .headers()?
            .iter()
            .map(|h| h.trim().to_owned())
            .collect();

        if csv_headers.is_empty() {
            return Err(ScrapeError::Parse(
                "CSV file contains no header row".to_owned(),
            ));
        }

        let mut records: Vec<serde_json::Value> = Vec::new();

        for result in reader.records() {
            let record = result?;

            let mut map = serde_json::Map::new();
            for (i, header) in csv_headers.iter().enumerate() {
                let value = record.get(i).unwrap_or("").trim().to_owned();
                map.insert(header.clone(), serde_json::Value::String(value));
            }
            records.push(serde_json::Value::Object(map));

            if let Some(max) = self.max_records
                && records.len() as u64 >= max
            {
                log::info!("Reached max_records limit ({max}), stopping CSV parse");
                break;
            }
        }

        log::info!("Parsed {} records from CSV at {}", records.len(), self.url);

        Ok(ScrapedPage {
            records,
            has_more: false,
            page_number: 0,
        })
    }

    fn strategy(&self) -> &'static str {
        "csv_download"
    }
}
