//! Config-driven crime data source definition.
//!
//! [`SourceDefinition`] captures everything unique about a data source in a
//! serializable config struct. A single generic implementation handles all
//! sources, eliminating per-city boilerplate.
//!
//! Pages of raw records are streamed through a [`tokio::sync::mpsc`] channel
//! so that normalization and database insertion happen incrementally rather
//! than buffering the entire dataset in memory.

use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use crime_map_source_models::NormalizedIncident;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::arcgis::{ArcGisConfig, fetch_arcgis};
use crate::carto::{CartoConfig, fetch_carto};
use crate::city_protect::{CityProtectConfig, fetch_city_protect};
use crate::ckan::{CkanConfig, fetch_ckan};
use crate::crime_bulletin::{CrimeBulletinConfig, fetch_crime_bulletin};
use crate::csv_download::{CsvDownloadConfig, fetch_csv_download};
use crate::html_table::{HtmlTableConfig, fetch_html_table};
use crate::json_paginated::{JsonPaginatedConfig, fetch_json_paginated};
use crate::lexisnexis_ccm::{LexisNexisCcmConfig, fetch_lexisnexis_ccm};
use crate::odata::{ODataConfig, fetch_odata};
use crate::parsing::parse_socrata_date;
use crate::pdf_extract::{PdfExtractConfig, fetch_pdf_extract};
use crate::press_release::{PressReleaseConfig, fetch_press_release};
use crate::socrata::{SocrataConfig, fetch_socrata};
use crate::type_mapping::map_crime_type;
use crate::{FetchOptions, SourceError};

// ── Top-level source definition ──────────────────────────────────────────

/// A complete, config-driven crime data source definition.
///
/// Loaded from TOML files at compile time and used as the sole source
/// implementation.
#[derive(Debug, Deserialize)]
pub struct SourceDefinition {
    /// Unique identifier (e.g., `"chicago_pd"`).
    pub id: String,
    /// Human-readable name (e.g., `"Chicago Police Department"`).
    pub name: String,
    /// City name for the `NormalizedIncident`.
    pub city: String,
    /// Two-letter state abbreviation.
    pub state: String,
    /// Legacy output filename (kept for config compatibility).
    pub output_filename: String,
    /// Licensing and usage metadata for this data source.
    pub license: LicenseInfo,
    /// How to fetch raw data from the API.
    pub fetcher: FetcherConfig,
    /// Field name mappings for normalization.
    pub fields: FieldMapping,
    /// Whether incidents from this source should be re-geocoded from their
    /// block addresses even when they already have source-provided coordinates.
    /// Set to `true` for sources that provide imprecise block-centroid
    /// coordinates. Defaults to `false`.
    #[serde(default)]
    pub re_geocode: bool,
    /// Optional URL to the human-readable data portal page for this source.
    /// If not set, one may be derived from the fetcher config (e.g., Socrata
    /// dataset pages from the API URL).
    #[serde(default)]
    pub portal_url: Option<String>,
}

// ── License metadata ─────────────────────────────────────────────────────

/// Licensing and usage restrictions for a data source.
///
/// Every source MUST explicitly document its license. This ensures we
/// always know what we can and cannot do with each dataset.
#[derive(Debug, Deserialize)]
pub struct LicenseInfo {
    /// License type identifier.
    ///
    /// One of: `"public_domain"`, `"cc_zero"`, `"cc_by"`, `"cc_by_sa"`,
    /// `"open_data"`, `"tos_restricted"`, `"proprietary"`, `"unknown"`.
    pub license_type: String,
    /// URL to the terms of service or license page, if available.
    pub tos_url: Option<String>,
    /// Whether attribution is required when using this data.
    pub attribution_required: bool,
    /// Verbatim attribution text to display when required.
    pub attribution_text: Option<String>,
    /// Whether redistribution of the data is allowed.
    pub allows_redistribution: bool,
    /// Whether scraping is explicitly allowed or not prohibited by the TOS.
    ///
    /// `None` when the TOS does not address scraping.
    pub allows_scraping: Option<bool>,
    /// If `true`, this source requires explicit opt-in to ingest (e.g.
    /// proprietary or restricted-use data). Restricted sources are skipped
    /// by default during `cargo ingest sync-all`.
    pub restricted: bool,
    /// Free-form notes about usage restrictions or licensing details.
    pub notes: Option<String>,
}

// ── Fetcher config ───────────────────────────────────────────────────────

/// How to fetch raw data from the source API.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FetcherConfig {
    /// Socrata SODA API (`$limit/$offset/$order/$where`).
    Socrata {
        /// Base Socrata API URL.
        api_url: String,
        /// Date column for ordering and filtering.
        date_column: String,
        /// Records per page.
        page_size: u64,
    },
    /// `ArcGIS` REST API (`resultOffset`/`resultRecordCount`).
    Arcgis {
        /// Query URLs (one per layer/year).
        query_urls: Vec<String>,
        /// Records per page.
        page_size: u64,
        /// Optional WHERE clause.
        where_clause: Option<String>,
        /// Date column for incremental `since` filtering (epoch-ms field).
        date_column: Option<String>,
    },
    /// CKAN Datastore API (`limit`/`offset`).
    Ckan {
        /// Base API URL.
        api_url: String,
        /// CKAN resource IDs (one per dataset/year).
        resource_ids: Vec<String>,
        /// Records per page.
        page_size: u64,
        /// Date column for incremental `since` filtering.
        date_column: Option<String>,
    },
    /// Carto SQL API (SQL `LIMIT`/`OFFSET`).
    Carto {
        /// Base Carto SQL API URL.
        api_url: String,
        /// Table name to query.
        table_name: String,
        /// Date column for ordering and filtering.
        date_column: String,
        /// Records per page.
        page_size: u64,
    },
    /// OData-style REST API (`$top`/`$skip`/`$orderby`).
    Odata {
        /// Base API URL (response is a bare JSON array).
        api_url: String,
        /// Date column for ordering and `$filter`.
        date_column: String,
        /// Records per page.
        page_size: u64,
    },
    /// HTML table scraping (police department websites with tabular data).
    HtmlTable {
        /// URL of the page containing the table.
        url: String,
        /// CSS selector for the target table element.
        table_selector: Option<String>,
        /// CSS selector for header cells.
        header_selector: Option<String>,
        /// CSS selector for body rows.
        row_selector: Option<String>,
        /// CSS selector for cells within a row.
        cell_selector: Option<String>,
        /// Delay between page fetches in milliseconds.
        delay_ms: Option<u64>,
        /// Additional HTTP headers.
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    /// CSV file download (single or multiple URLs for yearly files).
    CsvDownload {
        /// URLs of CSV files to download.
        urls: Vec<String>,
        /// Field delimiter (default: comma).
        delimiter: Option<String>,
        /// Compression format: `"gzip"` or omit for uncompressed.
        compressed: Option<String>,
        /// Maximum records per CSV file.
        max_records: Option<u64>,
        /// Additional HTTP headers.
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    /// Generic paginated JSON API (hidden APIs behind dashboards, etc.).
    JsonPaginated {
        /// Base API URL.
        api_url: String,
        /// Pagination strategy: `"offset"`, `"page"`, or `"cursor"`.
        pagination: String,
        /// Response format: `"bare_array"` (default) or `"wrapped"`.
        response_format: Option<String>,
        /// Dot-path to the records array (for `"wrapped"` responses).
        records_path: Option<String>,
        /// Records per page.
        page_size: u64,
        /// Override for the pagination query parameter name.
        page_param: Option<String>,
        /// Override for the page-size query parameter name.
        size_param: Option<String>,
        /// Delay between page fetches in milliseconds.
        delay_ms: Option<u64>,
        /// Additional HTTP headers.
        #[serde(default)]
        headers: BTreeMap<String, String>,
    },
    /// PDF table extraction (crime bulletins, reports, etc.).
    PdfExtract {
        /// URLs of PDF files to download.
        urls: Vec<String>,
        /// Extraction strategy: `"regex_rows"`, `"text_table"`, or
        /// `"line_delimited"`.
        extraction_strategy: String,
        /// Regex pattern with named capture groups (for `regex_rows`).
        row_pattern: Option<String>,
        /// Column start character positions (for `text_table`).
        column_boundaries: Option<Vec<u32>>,
        /// Column names (for `text_table` and `line_delimited`).
        column_names: Option<Vec<String>>,
        /// Delimiter character (for `line_delimited`).
        delimiter: Option<String>,
        /// Number of header lines to skip.
        skip_header_lines: Option<usize>,
    },
    /// `CityProtect` / Motorola `CommandCentral` incident API (POST-based
    /// JSON endpoint with geographic bounding-box filtering).
    CityProtect {
        /// `CityProtect` incidents API URL.
        api_url: String,
        /// `CityProtect` agency ID (e.g., `"381"`).
        agency_id: String,
        /// Bounding box `[west, south, east, north]` for the agency's
        /// jurisdiction.
        bbox: [f64; 4],
        /// Records per page (default 2000).
        page_size: u64,
        /// Override incident type IDs filter (default: all types).
        incident_type_ids: Option<String>,
    },
    /// Press release / news-bulletin scraper (crawls paginated listing pages
    /// and parses individual press release HTML for structured incident data).
    PressRelease {
        /// Base URL of the press release listing page.
        listing_url: String,
        /// Base domain for resolving relative URLs.
        base_url: String,
        /// CSS selector for links on listing pages.
        link_selector: String,
        /// URL substring filter — only follow links containing this string.
        link_filter: String,
        /// CSS selector for the article body on individual pages.
        article_selector: String,
        /// Pagination query parameter name (default `"page"`).
        page_param: Option<String>,
        /// Maximum listing pages to crawl.
        max_pages: Option<u32>,
        /// Parse mode: `"structured"` (default, Anne Arundel style) or
        /// `"drupal_narrative"` (Howard County style, single-incident prose).
        parse_mode: Option<String>,
    },
    /// Daily crime-bulletin scraper (single-page accordion-style bulletins
    /// with structured per-incident entries).
    CrimeBulletin {
        /// URL of the bulletin page.
        url: String,
        /// CSS selector for the accordion content containers.
        content_selector: String,
    },
    /// `LexisNexis` Community Crime Map (CCM) incident API. Uses the
    /// hidden REST API at `communitycrimemap.com` to fetch geocoded
    /// incidents for a specific agency within a bounding box.
    LexisNexisCcm {
        /// Bounding box `[west, south, east, north]` for the agency's
        /// jurisdiction.
        bbox: [f64; 4],
        /// Agency name filter — only incidents from agencies whose name
        /// contains this string (case-insensitive) are kept.
        agency_filter: String,
    },
}

// ── Field mapping ────────────────────────────────────────────────────────

impl FetcherConfig {
    /// Attempts to derive a human-readable portal URL from the fetcher
    /// configuration.
    ///
    /// Currently supports:
    /// - **Socrata**: `/resource/{id}.json` -> `/d/{id}`
    /// - **CKAN**: Returns the base `api_url` (the dataset host)
    /// - **`ArcGIS`**: Strips `/query` suffix to get the service page
    /// - **`CrimeBulletin`**: Returns the bulletin page URL directly
    /// - **`PressRelease`**: Returns the listing page URL
    #[must_use]
    fn derive_portal_url(&self) -> Option<String> {
        match self {
            Self::Socrata { api_url, .. } => {
                // https://data.cityofchicago.org/resource/ijzp-q8t2.json
                // -> https://data.cityofchicago.org/d/ijzp-q8t2
                api_url.find("/resource/").map(|idx| {
                    let base = &api_url[..idx];
                    let rest = &api_url[idx + "/resource/".len()..];
                    let dataset_id = rest.strip_suffix(".json").unwrap_or(rest);
                    format!("{base}/d/{dataset_id}")
                })
            }
            Self::Arcgis { query_urls, .. } => {
                // Use first URL, strip /query suffix
                query_urls
                    .first()
                    .map(|url| url.strip_suffix("/query").unwrap_or(url).to_string())
            }
            Self::Ckan { api_url, .. } => Some(api_url.clone()),
            Self::CrimeBulletin { url, .. } => Some(url.clone()),
            Self::PressRelease { listing_url, .. } => Some(listing_url.clone()),
            _ => None,
        }
    }
}

/// Maps source-specific JSON field names to canonical incident fields.
#[derive(Debug, Deserialize)]
pub struct FieldMapping {
    /// JSON field names for the incident ID, tried in order.
    pub incident_id: Vec<String>,
    /// JSON field names for the crime type, tried in order (first non-empty
    /// wins).
    pub crime_type: Vec<String>,
    /// How to extract the `occurred_at` timestamp.
    pub occurred_at: DateExtractor,
    /// Optional field name for `reported_at` (parsed as Socrata datetime).
    pub reported_at: Option<String>,
    /// Latitude coordinate field. `None` for sources that require geocoding.
    pub lat: Option<CoordField>,
    /// Longitude coordinate field. `None` for sources that require geocoding.
    pub lng: Option<CoordField>,
    /// How to build the description string.
    pub description: DescriptionExtractor,
    /// How to extract the block address.
    pub block_address: Option<BlockAddressExtractor>,
    /// Optional field name for location type.
    pub location_type: Option<String>,
    /// How to extract the arrest flag.
    #[serde(default)]
    pub arrest: ArrestExtractor,
    /// Optional domestic violence flag field (direct bool).
    pub domestic: Option<String>,
}

// ── Strategy enums ───────────────────────────────────────────────────────

/// How to extract the `occurred_at` timestamp from a raw record.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DateExtractor {
    /// Single Socrata datetime field.
    Simple {
        /// JSON field name.
        field: String,
    },
    /// Date field + 4-character HHMM time string.
    DatePlusHhmm {
        /// JSON field for the date portion.
        date_field: String,
        /// JSON field for the HHMM time string.
        time_field: String,
    },
    /// Date field + `"HH:MM:SS"` time string.
    DatePlusHhmmss {
        /// JSON field for the date portion.
        date_field: String,
        /// JSON field for the time string.
        time_field: String,
    },
    /// Epoch milliseconds (f64).
    EpochMs {
        /// JSON field name.
        field: String,
    },
    /// `MM/DD/YYYY` text date (no time component).
    MdyDate {
        /// JSON field name.
        field: String,
    },
}

/// A coordinate field and its type.
#[derive(Debug, Deserialize)]
pub struct CoordField {
    /// JSON field name.
    pub field: String,
    /// Whether the field is a string or f64.
    #[serde(rename = "type")]
    pub coord_type: CoordType,
}

/// Whether a coordinate is stored as a string or float in the API response.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordType {
    /// Coordinate is a JSON string that must be parsed to f64.
    String,
    /// Coordinate is a JSON number (f64).
    F64,
    /// Extract latitude from a `GeoJSON` Point or Socrata location object.
    ///
    /// `GeoJSON`: `{"type":"Point","coordinates":[-96.88,32.71]}` → returns `32.71`
    /// Socrata: `{"latitude":"32.71","longitude":"-96.88"}` → returns `32.71`
    PointLat,
    /// Extract longitude from a `GeoJSON` Point or Socrata location object.
    ///
    /// `GeoJSON`: `{"type":"Point","coordinates":[-96.88,32.71]}` → returns `-96.88`
    /// Socrata: `{"latitude":"32.71","longitude":"-96.88"}` → returns `-96.88`
    PointLng,
}

/// How to build the description string.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DescriptionExtractor {
    /// Use a single field directly.
    Single {
        /// JSON field name.
        field: String,
    },
    /// Combine multiple fields with a separator (skip empty fields).
    Combine {
        /// JSON field names to combine.
        fields: Vec<String>,
        /// Separator between non-empty values.
        separator: String,
    },
    /// Try fields in order, use the first non-empty value.
    FallbackChain {
        /// JSON field names, tried in order.
        fields: Vec<String>,
    },
}

/// How to extract the `arrest_made` flag.
#[derive(Debug, Default, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ArrestExtractor {
    /// No arrest information available.
    #[default]
    None,
    /// Direct boolean field.
    DirectBool {
        /// JSON field name.
        field: String,
    },
    /// String field checked for a configurable substring (case-insensitive).
    StringContains {
        /// JSON field name.
        field: String,
        /// Substring to search for (case-insensitive).
        contains: String,
    },
}

/// How to extract the block address from a JSON record.
///
/// Supports either a bare string (single field name, backward-compatible with
/// all existing TOMLs) or a tagged struct for combining multiple fields.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum BlockAddressExtractor {
    /// A single JSON field name (bare string in TOML: `block_address = "field"`).
    Single(String),
    /// Combine multiple JSON fields with a separator.
    ///
    /// In TOML:
    /// ```toml
    /// [fields.block_address]
    /// type = "combine"
    /// fields = ["street_number", "street_address"]
    /// separator = " "
    /// ```
    Tagged(BlockAddressTagged),
}

/// Tagged variants for structured block address extraction.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BlockAddressTagged {
    /// Combine multiple fields with a separator (skip empty fields).
    Combine {
        /// JSON field names to combine.
        fields: Vec<String>,
        /// Separator between non-empty values.
        separator: String,
    },
}

// ── Helper methods on extractors ─────────────────────────────────────────

/// Gets a string value from a JSON object by field name.
fn get_str<'a>(record: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    record.get(field)?.as_str()
}

/// Gets an f64 value from a JSON object by field name.
fn get_f64(record: &serde_json::Value, field: &str) -> Option<f64> {
    record.get(field)?.as_f64()
}

/// Gets a bool value from a JSON object by field name.
fn get_bool(record: &serde_json::Value, field: &str) -> Option<bool> {
    record.get(field)?.as_bool()
}

impl CoordField {
    /// Extracts a coordinate value from a JSON record.
    fn extract(&self, record: &serde_json::Value) -> Option<f64> {
        match self.coord_type {
            CoordType::String => {
                let s = get_str(record, &self.field)?;
                s.parse::<f64>().ok()
            }
            CoordType::F64 => get_f64(record, &self.field),
            CoordType::PointLat => {
                let obj = record.get(&self.field)?;
                // GeoJSON Point: {"type":"Point","coordinates":[lng, lat]}
                if let Some(coords) = obj.get("coordinates").and_then(|c| c.as_array()) {
                    return coords.get(1)?.as_f64();
                }
                // Socrata location: {"latitude":"32.71","longitude":"-96.88"}
                obj.get("latitude").and_then(|v| {
                    v.as_str()
                        .and_then(|s| s.parse().ok())
                        .or_else(|| v.as_f64())
                })
            }
            CoordType::PointLng => {
                let obj = record.get(&self.field)?;
                // GeoJSON Point: {"type":"Point","coordinates":[lng, lat]}
                if let Some(coords) = obj.get("coordinates").and_then(|c| c.as_array()) {
                    return coords.first()?.as_f64();
                }
                // Socrata location: {"latitude":"32.71","longitude":"-96.88"}
                obj.get("longitude").and_then(|v| {
                    v.as_str()
                        .and_then(|s| s.parse().ok())
                        .or_else(|| v.as_f64())
                })
            }
        }
    }
}

impl DateExtractor {
    /// Extracts a datetime from a JSON record.
    fn extract(&self, record: &serde_json::Value) -> Option<DateTime<Utc>> {
        match self {
            Self::Simple { field } => {
                let s = get_str(record, field)?;
                parse_socrata_date(s)
            }
            Self::DatePlusHhmm {
                date_field,
                time_field,
            } => {
                let date_str = get_str(record, date_field)?;
                let parsed = parse_socrata_date(date_str)?;
                if let Some(time_str) = get_str(record, time_field)
                    && time_str.len() == 4
                {
                    let hour = time_str[..2].parse::<u32>().ok()?;
                    let min = time_str[2..].parse::<u32>().ok()?;
                    let time = NaiveTime::from_hms_opt(hour, min, 0)?;
                    let dt = NaiveDateTime::new(parsed.date_naive(), time);
                    return Some(dt.and_utc());
                }
                Some(parsed)
            }
            Self::DatePlusHhmmss {
                date_field,
                time_field,
            } => {
                let date_str = get_str(record, date_field)?;
                let parsed = parse_socrata_date(date_str)?;
                if let Some(time_str) = get_str(record, time_field)
                    && let Ok(time) = time_str.parse::<NaiveTime>()
                {
                    let dt = NaiveDateTime::new(parsed.date_naive(), time);
                    return Some(dt.and_utc());
                }
                Some(parsed)
            }
            Self::EpochMs { field } => {
                let ms = get_f64(record, field)?;
                #[allow(clippy::cast_possible_truncation)]
                let secs = (ms / 1000.0) as i64;
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let nsecs = ((ms % 1000.0) * 1_000_000.0) as u32;
                DateTime::from_timestamp(secs, nsecs)
            }
            Self::MdyDate { field } => {
                let s = get_str(record, field)?;
                let date = NaiveDate::parse_from_str(s, "%m/%d/%Y").ok()?;
                Some(date.and_hms_opt(0, 0, 0)?.and_utc())
            }
        }
    }
}

impl DescriptionExtractor {
    /// Extracts a description string from a JSON record.
    fn extract(&self, record: &serde_json::Value) -> Option<String> {
        match self {
            Self::Single { field } => get_str(record, field).map(String::from),
            Self::Combine { fields, separator } => {
                let parts: Vec<&str> = fields
                    .iter()
                    .filter_map(|f| get_str(record, f))
                    .filter(|s| !s.is_empty())
                    .collect();
                if parts.is_empty() {
                    None
                } else {
                    Some(parts.join(separator))
                }
            }
            Self::FallbackChain { fields } => fields
                .iter()
                .filter_map(|f| get_str(record, f))
                .find(|s| !s.is_empty())
                .map(String::from),
        }
    }
}

impl ArrestExtractor {
    /// Extracts the arrest flag from a JSON record.
    fn extract(&self, record: &serde_json::Value) -> Option<bool> {
        match self {
            Self::None => Option::None,
            Self::DirectBool { field } => get_bool(record, field),
            Self::StringContains { field, contains } => {
                let s = get_str(record, field)?;
                Some(s.to_lowercase().contains(&contains.to_lowercase()))
            }
        }
    }
}

impl BlockAddressExtractor {
    /// Extracts a block address string from a JSON record.
    fn extract(&self, record: &serde_json::Value) -> Option<String> {
        match self {
            Self::Single(field) => get_str(record, field).map(String::from),
            Self::Tagged(tagged) => tagged.extract(record),
        }
    }
}

impl BlockAddressTagged {
    /// Extracts a block address string from a JSON record.
    fn extract(&self, record: &serde_json::Value) -> Option<String> {
        match self {
            Self::Combine { fields, separator } => {
                let parts: Vec<&str> = fields
                    .iter()
                    .filter_map(|f| get_str(record, f))
                    .filter(|s| !s.is_empty())
                    .collect();
                if parts.is_empty() {
                    None
                } else {
                    Some(parts.join(separator))
                }
            }
        }
    }
}

// ── Streaming fetch + normalize ───────────────────────────────────────

/// Channel buffer size — allows the fetcher to stay one page ahead of
/// the consumer (normalizer/inserter).
const PAGE_CHANNEL_BUFFER: usize = 2;

impl SourceDefinition {
    /// Returns the unique source identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the human-readable source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the licensing metadata for this source.
    #[must_use]
    pub const fn license(&self) -> &LicenseInfo {
        &self.license
    }

    /// Returns `true` if this source is restricted and requires explicit
    /// opt-in to ingest.
    #[must_use]
    pub const fn is_restricted(&self) -> bool {
        self.license.restricted
    }

    /// Returns `true` if incidents from this source should be re-geocoded
    /// from block addresses even when source-provided coordinates exist.
    #[must_use]
    pub const fn re_geocode(&self) -> bool {
        self.re_geocode
    }

    /// Returns the portal URL for this source.
    ///
    /// If a `portal_url` is explicitly set in the TOML, that value is
    /// returned. Otherwise, a URL is derived from the fetcher config when
    /// possible (e.g., Socrata API URLs are transformed from
    /// `/resource/{id}.json` to `/d/{id}`).
    #[must_use]
    pub fn portal_url(&self) -> Option<String> {
        if let Some(ref url) = self.portal_url {
            return Some(url.clone());
        }
        self.fetcher.derive_portal_url()
    }

    /// Returns the configured page size for this source's fetcher.
    #[must_use]
    pub const fn page_size(&self) -> u64 {
        match &self.fetcher {
            FetcherConfig::Socrata { page_size, .. }
            | FetcherConfig::Arcgis { page_size, .. }
            | FetcherConfig::Ckan { page_size, .. }
            | FetcherConfig::Carto { page_size, .. }
            | FetcherConfig::Odata { page_size, .. }
            | FetcherConfig::JsonPaginated { page_size, .. }
            | FetcherConfig::CityProtect { page_size, .. } => *page_size,
            // Non-paginated fetchers: return total-file-at-once sizes
            FetcherConfig::HtmlTable { .. }
            | FetcherConfig::CsvDownload { .. }
            | FetcherConfig::PdfExtract { .. }
            | FetcherConfig::PressRelease { .. }
            | FetcherConfig::CrimeBulletin { .. }
            | FetcherConfig::LexisNexisCcm { .. } => 0,
        }
    }

    /// Starts fetching pages in a background task and returns a receiver
    /// that yields one page of raw JSON records at a time.
    ///
    /// The caller should receive pages, call [`Self::normalize_page`] on
    /// each, and insert the results into the database immediately.
    ///
    /// A fetch error (if any) is returned via the [`tokio::task::JoinHandle`].
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn fetch_pages(
        &self,
        options: &FetchOptions,
        progress: std::sync::Arc<dyn crate::progress::ProgressCallback>,
    ) -> (
        mpsc::Receiver<Vec<serde_json::Value>>,
        tokio::task::JoinHandle<Result<u64, SourceError>>,
    ) {
        let (tx, rx) = mpsc::channel(PAGE_CHANNEL_BUFFER);
        let fetcher = self.fetcher.clone();
        let name = self.name.clone();
        let options = options.clone();

        let handle = tokio::spawn(async move {
            match &fetcher {
                FetcherConfig::Socrata {
                    api_url,
                    date_column,
                    page_size,
                } => {
                    fetch_socrata(
                        &SocrataConfig {
                            api_url,
                            date_column,
                            label: &name,
                            page_size: *page_size,
                        },
                        &options,
                        &tx,
                        &progress,
                    )
                    .await
                }
                FetcherConfig::Arcgis {
                    query_urls,
                    page_size,
                    where_clause,
                    date_column,
                } => {
                    fetch_arcgis(
                        &ArcGisConfig {
                            query_urls,
                            label: &name,
                            page_size: *page_size,
                            where_clause: where_clause.as_deref(),
                            date_column: date_column.as_deref(),
                        },
                        &options,
                        &tx,
                        &progress,
                    )
                    .await
                }
                FetcherConfig::Ckan {
                    api_url,
                    resource_ids,
                    page_size,
                    date_column,
                } => {
                    fetch_ckan(
                        &CkanConfig {
                            api_url,
                            resource_ids,
                            label: &name,
                            page_size: *page_size,
                            date_column: date_column.as_deref(),
                        },
                        &options,
                        &tx,
                        &progress,
                    )
                    .await
                }
                FetcherConfig::Carto {
                    api_url,
                    table_name,
                    date_column,
                    page_size,
                } => {
                    fetch_carto(
                        &CartoConfig {
                            api_url,
                            table_name,
                            date_column,
                            label: &name,
                            page_size: *page_size,
                        },
                        &options,
                        &tx,
                        &progress,
                    )
                    .await
                }
                FetcherConfig::Odata {
                    api_url,
                    date_column,
                    page_size,
                } => {
                    fetch_odata(
                        &ODataConfig {
                            api_url,
                            date_column,
                            label: &name,
                            page_size: *page_size,
                        },
                        &options,
                        &tx,
                        &progress,
                    )
                    .await
                }
                FetcherConfig::HtmlTable {
                    url,
                    table_selector,
                    header_selector,
                    row_selector,
                    cell_selector,
                    delay_ms: _,
                    headers,
                } => {
                    fetch_html_table(
                        &HtmlTableConfig {
                            url,
                            label: &name,
                            table_selector: table_selector.as_deref(),
                            header_selector: header_selector.as_deref(),
                            row_selector: row_selector.as_deref(),
                            cell_selector: cell_selector.as_deref(),
                            delay_ms: None,
                            headers,
                        },
                        &options,
                        &tx,
                        &progress,
                    )
                    .await
                }
                FetcherConfig::CsvDownload {
                    urls,
                    delimiter,
                    compressed,
                    max_records,
                    headers,
                } => {
                    fetch_csv_download(
                        &CsvDownloadConfig {
                            urls,
                            label: &name,
                            delimiter: delimiter.as_deref(),
                            compressed: compressed.as_deref(),
                            max_records: *max_records,
                            headers,
                        },
                        &options,
                        &tx,
                        &progress,
                    )
                    .await
                }
                FetcherConfig::JsonPaginated {
                    api_url,
                    pagination,
                    response_format,
                    records_path,
                    page_size,
                    page_param,
                    size_param,
                    delay_ms,
                    headers,
                } => {
                    fetch_json_paginated(
                        &JsonPaginatedConfig {
                            api_url,
                            label: &name,
                            pagination,
                            response_format: response_format.as_deref(),
                            records_path: records_path.as_deref(),
                            page_size: *page_size,
                            page_param: page_param.as_deref(),
                            size_param: size_param.as_deref(),
                            delay_ms: *delay_ms,
                            headers,
                        },
                        &options,
                        &tx,
                        &progress,
                    )
                    .await
                }
                FetcherConfig::PdfExtract {
                    urls,
                    extraction_strategy,
                    row_pattern,
                    column_boundaries,
                    column_names,
                    delimiter,
                    skip_header_lines,
                } => {
                    fetch_pdf_extract(
                        &PdfExtractConfig {
                            urls,
                            label: &name,
                            extraction_strategy,
                            row_pattern: row_pattern.as_deref(),
                            column_boundaries: column_boundaries.as_deref(),
                            column_names: column_names.as_deref(),
                            delimiter: delimiter.as_deref(),
                            skip_header_lines: *skip_header_lines,
                        },
                        &options,
                        &tx,
                        &progress,
                    )
                    .await
                }
                FetcherConfig::CityProtect {
                    api_url,
                    agency_id,
                    bbox,
                    page_size,
                    incident_type_ids,
                } => {
                    fetch_city_protect(
                        &CityProtectConfig {
                            api_url,
                            bbox,
                            agency_id,
                            page_size: *page_size,
                            incident_type_ids: incident_type_ids.as_deref(),
                            label: &name,
                        },
                        &options,
                        &tx,
                        &progress,
                    )
                    .await
                }
                FetcherConfig::PressRelease {
                    listing_url,
                    base_url,
                    link_selector,
                    link_filter,
                    article_selector,
                    page_param,
                    max_pages,
                    parse_mode,
                } => {
                    fetch_press_release(
                        &PressReleaseConfig {
                            listing_url,
                            base_url,
                            link_selector,
                            link_filter,
                            article_selector,
                            page_param: page_param.as_deref().unwrap_or("page"),
                            max_pages: max_pages.unwrap_or(0),
                            parse_mode: parse_mode.as_deref().unwrap_or("structured"),
                            label: &name,
                        },
                        &options,
                        &tx,
                        &progress,
                    )
                    .await
                }
                FetcherConfig::CrimeBulletin {
                    url,
                    content_selector,
                } => {
                    fetch_crime_bulletin(
                        &CrimeBulletinConfig {
                            url,
                            content_selector,
                            label: &name,
                        },
                        &options,
                        &tx,
                        &progress,
                    )
                    .await
                }
                FetcherConfig::LexisNexisCcm {
                    bbox,
                    agency_filter,
                } => {
                    fetch_lexisnexis_ccm(
                        &LexisNexisCcmConfig {
                            bbox,
                            agency_filter,
                            label: &name,
                        },
                        &options,
                        &tx,
                        &progress,
                    )
                    .await
                }
            }
        });

        (rx, handle)
    }

    /// Normalizes a single page of raw JSON records into canonical
    /// [`NormalizedIncident`]s.
    pub fn normalize_page(&self, records: &[serde_json::Value]) -> Vec<NormalizedIncident> {
        let fields = &self.fields;
        let mut incidents = Vec::with_capacity(records.len());

        for record in records {
            // ── Lat/lng ──────────────────────────────────────────────
            let latitude = fields.lat.as_ref().and_then(|f| f.extract(record));
            let longitude = fields.lng.as_ref().and_then(|f| f.extract(record));

            // Reject zero or out-of-range coordinates (treat as missing).
            // Some sources return projected coordinates (e.g. State Plane)
            // instead of WGS84 decimal degrees — filter those out so they
            // can be geocoded from block addresses instead.
            let (latitude, longitude) = match (latitude, longitude) {
                (Some(lat), Some(lng))
                    if lat != 0.0
                        && lng != 0.0
                        && (-90.0..=90.0).contains(&lat)
                        && (-180.0..=180.0).contains(&lng) =>
                {
                    (Some(lat), Some(lng))
                }
                _ => (None, None),
            };

            // ── Incident ID ──────────────────────────────────────────
            let Some(source_incident_id) = extract_incident_id(record, &fields.incident_id) else {
                continue;
            };

            // ── Crime type ───────────────────────────────────────────
            let crime_str = fields
                .crime_type
                .iter()
                .filter_map(|f| get_str(record, f))
                .find(|s| !s.is_empty())
                .unwrap_or_default();
            let subcategory = map_crime_type(crime_str);

            // ── Dates ────────────────────────────────────────────────
            let occurred_at = fields.occurred_at.extract(record);
            if occurred_at.is_none() {
                log::warn!(
                    "Failed to parse occurred_at for incident {source_incident_id}, storing with NULL date"
                );
            }

            let reported_at = fields
                .reported_at
                .as_deref()
                .and_then(|f| get_str(record, f))
                .and_then(parse_socrata_date);

            // ── Description ──────────────────────────────────────────
            let description = fields.description.extract(record);

            // ── Optional fields ──────────────────────────────────────
            let block_address = fields
                .block_address
                .as_ref()
                .and_then(|ext| ext.extract(record));

            let location_type = fields
                .location_type
                .as_deref()
                .and_then(|f| get_str(record, f))
                .map(String::from);

            let arrest_made = fields.arrest.extract(record);

            let domestic = fields.domestic.as_deref().and_then(|f| get_bool(record, f));

            incidents.push(NormalizedIncident {
                source_incident_id,
                subcategory,
                longitude,
                latitude,
                occurred_at,
                reported_at,
                description,
                block_address,
                city: self.city.clone(),
                state: self.state.clone(),
                arrest_made,
                domestic,
                location_type,
                geocoded: false,
            });
        }

        incidents
    }
}

/// Tries each field name in order and returns the first non-empty string
/// value. Falls back to converting numeric values to strings.
fn extract_incident_id(record: &serde_json::Value, fields: &[String]) -> Option<String> {
    for field in fields {
        if let Some(s) = get_str(record, field)
            && !s.is_empty()
        {
            return Some(s.to_string());
        }
        // Some APIs return numeric IDs (e.g., Philly's objectid is i64)
        if let Some(n) = record.get(field).and_then(serde_json::Value::as_i64) {
            return Some(n.to_string());
        }
    }
    None
}

/// Parses a [`SourceDefinition`] from a TOML string.
///
/// # Errors
///
/// Returns an error if the TOML is malformed or missing required fields.
pub fn parse_source_toml(toml_str: &str) -> Result<SourceDefinition, String> {
    toml::de::from_str(toml_str).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_date() {
        let record = serde_json::json!({"date": "2024-01-15T14:30:00"});
        let extractor = DateExtractor::Simple {
            field: "date".to_string(),
        };
        let dt = extractor.extract(&record).unwrap();
        assert_eq!(dt.to_string(), "2024-01-15 14:30:00 UTC");
    }

    #[test]
    fn parses_date_plus_hhmm() {
        let record = serde_json::json!({"date_occ": "2024-01-15T00:00:00", "time_occ": "1430"});
        let extractor = DateExtractor::DatePlusHhmm {
            date_field: "date_occ".to_string(),
            time_field: "time_occ".to_string(),
        };
        let dt = extractor.extract(&record).unwrap();
        assert_eq!(dt.to_string(), "2024-01-15 14:30:00 UTC");
    }

    #[test]
    fn parses_epoch_ms() {
        let record = serde_json::json!({"report_dat": 1_705_312_200_000.0_f64});
        let extractor = DateExtractor::EpochMs {
            field: "report_dat".to_string(),
        };
        let dt = extractor.extract(&record).unwrap();
        assert_eq!(dt.date_naive().to_string(), "2024-01-15");
    }

    #[test]
    fn extracts_description_combine() {
        let record = serde_json::json!({"type": "THEFT", "detail": "FROM VEHICLE"});
        let extractor = DescriptionExtractor::Combine {
            fields: vec!["type".to_string(), "detail".to_string()],
            separator: ": ".to_string(),
        };
        assert_eq!(extractor.extract(&record).unwrap(), "THEFT: FROM VEHICLE");
    }

    #[test]
    fn extracts_description_fallback() {
        let record = serde_json::json!({"pd_desc": "", "ofns_desc": "ROBBERY"});
        let extractor = DescriptionExtractor::FallbackChain {
            fields: vec!["pd_desc".to_string(), "ofns_desc".to_string()],
        };
        assert_eq!(extractor.extract(&record).unwrap(), "ROBBERY");
    }

    #[test]
    fn extracts_arrest_string_contains() {
        let record = serde_json::json!({"status": "Adult Arrest"});
        let extractor = ArrestExtractor::StringContains {
            field: "status".to_string(),
            contains: "Arrest".to_string(),
        };
        assert_eq!(extractor.extract(&record), Some(true));
    }

    #[test]
    fn extracts_incident_id_fallback() {
        let record = serde_json::json!({"case_number": null, "id": "12345"});
        let fields = vec!["case_number".to_string(), "id".to_string()];
        assert_eq!(extract_incident_id(&record, &fields).unwrap(), "12345");
    }

    #[test]
    fn extracts_numeric_incident_id() {
        let record = serde_json::json!({"objectid": 42});
        let fields = vec!["objectid".to_string()];
        assert_eq!(extract_incident_id(&record, &fields).unwrap(), "42");
    }

    #[test]
    fn parses_chicago_toml() {
        let toml_str = include_str!("../sources/chicago.toml");
        let def = parse_source_toml(toml_str).unwrap();
        assert_eq!(def.id, "chicago_pd");
        assert_eq!(def.city, "Chicago");
        assert_eq!(def.state, "IL");
    }

    #[test]
    fn extracts_geojson_point_coords() {
        let record = serde_json::json!({
            "location": {
                "type": "Point",
                "coordinates": [-122.1994, 37.79242]
            }
        });
        let lat_field = CoordField {
            field: "location".to_string(),
            coord_type: CoordType::PointLat,
        };
        let lng_field = CoordField {
            field: "location".to_string(),
            coord_type: CoordType::PointLng,
        };
        assert!((lat_field.extract(&record).unwrap() - 37.79242).abs() < f64::EPSILON);
        assert!((lng_field.extract(&record).unwrap() - -122.1994).abs() < f64::EPSILON);
    }

    #[test]
    fn extracts_socrata_location_coords() {
        let record = serde_json::json!({
            "geocoded_column": {
                "latitude": "32.714063262",
                "longitude": "-96.888799822"
            }
        });
        let lat_field = CoordField {
            field: "geocoded_column".to_string(),
            coord_type: CoordType::PointLat,
        };
        let lng_field = CoordField {
            field: "geocoded_column".to_string(),
            coord_type: CoordType::PointLng,
        };
        assert!((lat_field.extract(&record).unwrap() - 32.714_063_262).abs() < 1e-6);
        assert!((lng_field.extract(&record).unwrap() - -96.888_799_822).abs() < 1e-6);
    }

    #[test]
    fn extracts_block_address_single() {
        let record = serde_json::json!({"address": "100 MAIN ST"});
        let extractor = BlockAddressExtractor::Single("address".to_string());
        assert_eq!(extractor.extract(&record).unwrap(), "100 MAIN ST");
    }

    #[test]
    fn extracts_block_address_combine() {
        let record =
            serde_json::json!({"street_number": "5900 BLOCK", "street_address": "FISHER RD"});
        let extractor = BlockAddressExtractor::Tagged(BlockAddressTagged::Combine {
            fields: vec!["street_number".to_string(), "street_address".to_string()],
            separator: " ".to_string(),
        });
        assert_eq!(extractor.extract(&record).unwrap(), "5900 BLOCK FISHER RD");
    }

    #[test]
    fn extracts_block_address_combine_partial() {
        // When street_number is missing, just use street_address
        let record = serde_json::json!({"street_address": "MARLBORO PIKE EB"});
        let extractor = BlockAddressExtractor::Tagged(BlockAddressTagged::Combine {
            fields: vec!["street_number".to_string(), "street_address".to_string()],
            separator: " ".to_string(),
        });
        assert_eq!(extractor.extract(&record).unwrap(), "MARLBORO PIKE EB");
    }

    #[test]
    fn parses_pg_county_toml() {
        let toml_str = include_str!("../sources/pg_county_md.toml");
        let def = parse_source_toml(toml_str).unwrap();
        assert_eq!(def.id, "pg_county_md");
        assert!(def.fields.block_address.is_some());
        assert!(def.re_geocode, "pg_county_md should have re_geocode = true");
    }

    #[test]
    fn re_geocode_defaults_to_false() {
        let toml_str = include_str!("../sources/chicago.toml");
        let def = parse_source_toml(toml_str).unwrap();
        assert!(
            !def.re_geocode,
            "sources without re_geocode should default to false"
        );
    }
}
