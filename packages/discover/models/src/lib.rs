#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Shared types for the crime data source discovery system.
//!
//! This crate contains enums and structs used by the discovery pipeline to
//! track leads, sources, legal information, scraping targets, geocoding
//! candidates, and API patterns.

use std::fmt;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Current status of a discovery lead as it moves through the pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeadStatus {
    /// Newly discovered, not yet reviewed.
    New,
    /// Currently being investigated for viability.
    Investigating,
    /// Verified as a good data source with usable crime data.
    VerifiedGood,
    /// Verified but lacks coordinate/location data.
    VerifiedNoCoords,
    /// Verified but contains no usable crime data.
    VerifiedNoData,
    /// Verified but only provides aggregate statistics, not incidents.
    VerifiedAggregateOnly,
    /// Verified but data is behind a proprietary/paid wall.
    VerifiedProprietary,
    /// Has data but needs geocoding to obtain coordinates.
    NeedsGeocoding,
    /// Has data but requires a custom scraper to ingest.
    NeedsScraper,
    /// Fully integrated into the ingestion pipeline.
    Integrated,
    /// Rejected as unsuitable.
    Rejected,
}

impl LeadStatus {
    /// Returns the `snake_case` string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Investigating => "investigating",
            Self::VerifiedGood => "verified_good",
            Self::VerifiedNoCoords => "verified_no_coords",
            Self::VerifiedNoData => "verified_no_data",
            Self::VerifiedAggregateOnly => "verified_aggregate_only",
            Self::VerifiedProprietary => "verified_proprietary",
            Self::NeedsGeocoding => "needs_geocoding",
            Self::NeedsScraper => "needs_scraper",
            Self::Integrated => "integrated",
            Self::Rejected => "rejected",
        }
    }
}

impl fmt::Display for LeadStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for LeadStatus {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "new" => Ok(Self::New),
            "investigating" => Ok(Self::Investigating),
            "verified_good" => Ok(Self::VerifiedGood),
            "verified_no_coords" => Ok(Self::VerifiedNoCoords),
            "verified_no_data" => Ok(Self::VerifiedNoData),
            "verified_aggregate_only" => Ok(Self::VerifiedAggregateOnly),
            "verified_proprietary" => Ok(Self::VerifiedProprietary),
            "needs_geocoding" => Ok(Self::NeedsGeocoding),
            "needs_scraper" => Ok(Self::NeedsScraper),
            "integrated" => Ok(Self::Integrated),
            "rejected" => Ok(Self::Rejected),
            _ => Err(format!("unknown LeadStatus: {value}")),
        }
    }
}

impl std::str::FromStr for LeadStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

// ---------------------------------------------------------------------------

/// Priority level for investigating a lead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    /// High-priority lead that should be investigated first.
    High,
    /// Medium-priority lead.
    Medium,
    /// Low-priority lead.
    Low,
}

impl Priority {
    /// Returns the `snake_case` string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
        }
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for Priority {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "high" => Ok(Self::High),
            "medium" => Ok(Self::Medium),
            "low" => Ok(Self::Low),
            _ => Err(format!("unknown Priority: {value}")),
        }
    }
}

impl std::str::FromStr for Priority {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

// ---------------------------------------------------------------------------

/// The type of API or access method for a data source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiType {
    /// Socrata open-data platform.
    Socrata,
    /// Esri `ArcGIS` REST API.
    Arcgis,
    /// CKAN open-data platform.
    Ckan,
    /// CARTO geospatial platform.
    Carto,
    /// `OData` protocol endpoint.
    Odata,
    /// Plain CSV file download.
    Csv,
    /// Requires web scraping.
    Scrape,
    /// Unknown or unclassified API type.
    Unknown,
}

impl ApiType {
    /// Returns the `snake_case` string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Socrata => "socrata",
            Self::Arcgis => "arcgis",
            Self::Ckan => "ckan",
            Self::Carto => "carto",
            Self::Odata => "odata",
            Self::Csv => "csv",
            Self::Scrape => "scrape",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for ApiType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for ApiType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "socrata" => Ok(Self::Socrata),
            "arcgis" => Ok(Self::Arcgis),
            "ckan" => Ok(Self::Ckan),
            "carto" => Ok(Self::Carto),
            "odata" => Ok(Self::Odata),
            "csv" => Ok(Self::Csv),
            "scrape" => Ok(Self::Scrape),
            "unknown" => Ok(Self::Unknown),
            _ => Err(format!("unknown ApiType: {value}")),
        }
    }
}

impl std::str::FromStr for ApiType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

// ---------------------------------------------------------------------------

/// How a data source provides geographic coordinates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinateType {
    /// GeoJSON-style point geometry object.
    PointGeometry,
    /// Separate latitude/longitude columns as floating-point numbers.
    LatLngF64,
    /// Separate latitude/longitude columns as strings.
    LatLngString,
    /// Only has street addresses, no coordinates.
    AddressOnly,
    /// No location data at all.
    None,
}

impl CoordinateType {
    /// Returns the `snake_case` string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PointGeometry => "point_geometry",
            Self::LatLngF64 => "lat_lng_f64",
            Self::LatLngString => "lat_lng_string",
            Self::AddressOnly => "address_only",
            Self::None => "none",
        }
    }
}

impl fmt::Display for CoordinateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for CoordinateType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "point_geometry" => Ok(Self::PointGeometry),
            "lat_lng_f64" => Ok(Self::LatLngF64),
            "lat_lng_string" => Ok(Self::LatLngString),
            "address_only" => Ok(Self::AddressOnly),
            "none" => Ok(Self::None),
            _ => Err(format!("unknown CoordinateType: {value}")),
        }
    }
}

impl std::str::FromStr for CoordinateType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

// ---------------------------------------------------------------------------

/// License or terms-of-use classification for a data source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LicenseType {
    /// Generic open-data license.
    OpenData,
    /// Creative Commons Attribution.
    CcBy,
    /// Creative Commons Attribution-ShareAlike.
    CcBySa,
    /// Creative Commons Zero (public domain dedication).
    CcZero,
    /// Explicit public domain.
    PublicDomain,
    /// Proprietary or restrictive license.
    Proprietary,
    /// Terms of service with usage restrictions.
    TosRestricted,
    /// Unknown or unspecified license.
    Unknown,
}

impl LicenseType {
    /// Returns the `snake_case` string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenData => "open_data",
            Self::CcBy => "cc_by",
            Self::CcBySa => "cc_by_sa",
            Self::CcZero => "cc_zero",
            Self::PublicDomain => "public_domain",
            Self::Proprietary => "proprietary",
            Self::TosRestricted => "tos_restricted",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for LicenseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for LicenseType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "open_data" => Ok(Self::OpenData),
            "cc_by" => Ok(Self::CcBy),
            "cc_by_sa" => Ok(Self::CcBySa),
            "cc_zero" => Ok(Self::CcZero),
            "public_domain" => Ok(Self::PublicDomain),
            "proprietary" => Ok(Self::Proprietary),
            "tos_restricted" => Ok(Self::TosRestricted),
            "unknown" => Ok(Self::Unknown),
            _ => Err(format!("unknown LicenseType: {value}")),
        }
    }
}

impl std::str::FromStr for LicenseType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

// ---------------------------------------------------------------------------

/// Strategy for scraping data from a website.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScrapeStrategy {
    /// Parse HTML tables from rendered pages.
    HtmlTable,
    /// Paginate through a JSON API.
    JsonPaginated,
    /// Download a CSV file directly.
    CsvDownload,
    /// Extract data from PDF documents.
    PdfExtract,
}

impl ScrapeStrategy {
    /// Returns the `snake_case` string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HtmlTable => "html_table",
            Self::JsonPaginated => "json_paginated",
            Self::CsvDownload => "csv_download",
            Self::PdfExtract => "pdf_extract",
        }
    }
}

impl fmt::Display for ScrapeStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for ScrapeStrategy {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "html_table" => Ok(Self::HtmlTable),
            "json_paginated" => Ok(Self::JsonPaginated),
            "csv_download" => Ok(Self::CsvDownload),
            "pdf_extract" => Ok(Self::PdfExtract),
            _ => Err(format!("unknown ScrapeStrategy: {value}")),
        }
    }
}

impl std::str::FromStr for ScrapeStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

// ---------------------------------------------------------------------------

/// Anti-bot protection detected on a scraping target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AntiBot {
    /// No anti-bot protection detected.
    None,
    /// Cloudflare bot management.
    Cloudflare,
    /// `DataDome` bot protection.
    DataDome,
    /// CAPTCHA challenge required.
    Captcha,
    /// Requires an authenticated session.
    SessionRequired,
    /// Unknown or unclassified protection.
    Unknown,
}

impl AntiBot {
    /// Returns the `snake_case` string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Cloudflare => "cloudflare",
            Self::DataDome => "data_dome",
            Self::Captcha => "captcha",
            Self::SessionRequired => "session_required",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for AntiBot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for AntiBot {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "none" => Ok(Self::None),
            "cloudflare" => Ok(Self::Cloudflare),
            "data_dome" => Ok(Self::DataDome),
            "captcha" => Ok(Self::Captcha),
            "session_required" => Ok(Self::SessionRequired),
            "unknown" => Ok(Self::Unknown),
            _ => Err(format!("unknown AntiBot: {value}")),
        }
    }
}

impl std::str::FromStr for AntiBot {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

// ---------------------------------------------------------------------------

/// Quality rating for geocoded addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GeocodingQuality {
    /// Addresses are well-structured and likely to geocode accurately.
    High,
    /// Addresses have some inconsistencies but are generally usable.
    Medium,
    /// Addresses are poorly formatted or incomplete.
    Low,
}

impl GeocodingQuality {
    /// Returns the `snake_case` string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
        }
    }
}

impl fmt::Display for GeocodingQuality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for GeocodingQuality {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "high" => Ok(Self::High),
            "medium" => Ok(Self::Medium),
            "low" => Ok(Self::Low),
            _ => Err(format!("unknown GeocodingQuality: {value}")),
        }
    }
}

impl std::str::FromStr for GeocodingQuality {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

// ---------------------------------------------------------------------------

/// Operational status of an integrated data source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceStatus {
    /// Source is actively providing data.
    Active,
    /// Source has not updated recently but may still be available.
    Stale,
    /// Source endpoint is broken or unreachable.
    Broken,
    /// Source has been intentionally retired.
    Deprecated,
}

impl SourceStatus {
    /// Returns the `snake_case` string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Stale => "stale",
            Self::Broken => "broken",
            Self::Deprecated => "deprecated",
        }
    }
}

impl fmt::Display for SourceStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for SourceStatus {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "active" => Ok(Self::Active),
            "stale" => Ok(Self::Stale),
            "broken" => Ok(Self::Broken),
            "deprecated" => Ok(Self::Deprecated),
            _ => Err(format!("unknown SourceStatus: {value}")),
        }
    }
}

impl std::str::FromStr for SourceStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

// ---------------------------------------------------------------------------
// Structs
// ---------------------------------------------------------------------------

/// A discovery lead representing a potential crime data source.
///
/// Leads are created during automated discovery (web searches, API catalog
/// crawling, etc.) and then triaged through the [`LeadStatus`] pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Lead {
    /// Database primary key.
    pub id: i64,
    /// Jurisdiction name (e.g., "Washington, DC" or "Chicago, IL").
    pub jurisdiction: String,
    /// Human-readable name for the data source.
    pub source_name: String,
    /// Detected API type, if known.
    pub api_type: Option<ApiType>,
    /// URL for the data source endpoint or landing page.
    pub url: Option<String>,
    /// Current status in the discovery pipeline.
    pub status: LeadStatus,
    /// Investigation priority.
    pub priority: Priority,
    /// Estimated likelihood (0.0–1.0) that this lead contains usable data.
    pub likelihood: Option<f64>,
    /// Approximate number of records available.
    pub record_count: Option<i64>,
    /// Whether the source includes geographic coordinates.
    pub has_coordinates: Option<bool>,
    /// Whether the source includes date/time information.
    pub has_dates: Option<bool>,
    /// How coordinates are represented in the data.
    pub coordinate_type: Option<CoordinateType>,
    /// Format string for date parsing (e.g., `"%Y-%m-%dT%H:%M:%S"`).
    pub date_format: Option<String>,
    /// JSON text of a representative sample record.
    pub sample_record: Option<String>,
    /// Notes about specific fields and their mappings.
    pub field_notes: Option<String>,
    /// Distance from Washington, DC in miles (for geographic prioritization).
    pub distance_from_dc_miles: Option<f64>,
    /// Free-form notes about this lead.
    pub notes: Option<String>,
    /// ISO 8601 timestamp when this lead was first discovered.
    pub discovered_at: String,
    /// ISO 8601 timestamp when this lead was last updated.
    pub updated_at: String,
    /// ISO 8601 timestamp when this lead was last investigated.
    pub investigated_at: Option<String>,
}

/// A tracked data source that has been verified and may be integrated.
///
/// Each source corresponds to a TOML configuration file in the sources
/// directory and is monitored for availability and freshness.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    /// Database primary key.
    pub id: i64,
    /// Unique source identifier matching the TOML `id` field.
    pub source_id: String,
    /// Jurisdiction name.
    pub jurisdiction: String,
    /// API type used to access this source.
    pub api_type: ApiType,
    /// Endpoint URL for data access.
    pub url: String,
    /// Total number of records available.
    pub record_count: Option<i64>,
    /// Earliest date in the dataset (ISO 8601).
    pub date_range_start: Option<String>,
    /// Latest date in the dataset (ISO 8601).
    pub date_range_end: Option<String>,
    /// Path to the TOML configuration file, if any.
    pub toml_filename: Option<String>,
    /// Current operational status.
    pub status: SourceStatus,
    /// ISO 8601 timestamp of the last verification check.
    pub last_verified: Option<String>,
    /// Free-form notes about this source.
    pub notes: Option<String>,
}

/// A record of a discovery search that was performed.
///
/// Tracks web searches, API catalog queries, and other discovery actions
/// to avoid duplicate work and maintain an audit trail.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchEntry {
    /// Database primary key.
    pub id: i64,
    /// Type of search performed (e.g., "web", "socrata\_catalog", "arcgis\_hub").
    pub search_type: String,
    /// The search query or URL that was executed.
    pub query: String,
    /// Geographic scope of the search (e.g., "national", "Virginia").
    pub geographic_scope: Option<String>,
    /// Summary of what was found.
    pub results_summary: Option<String>,
    /// ISO 8601 timestamp when the search was performed.
    pub searched_at: String,
    /// Identifier for the discovery session that performed this search.
    pub session_id: Option<String>,
}

/// Legal and licensing information for a lead or source.
///
/// Tracks terms of use, license types, and what operations are permitted
/// so that only legally compliant sources are integrated.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegalInfo {
    /// Database primary key.
    pub id: i64,
    /// Associated lead ID, if this applies to a lead.
    pub lead_id: Option<i64>,
    /// Associated source ID, if this applies to an integrated source.
    pub source_id: Option<i64>,
    /// Classification of the data license.
    pub license_type: Option<LicenseType>,
    /// URL to the terms of service or license text.
    pub tos_url: Option<String>,
    /// Whether bulk data download is permitted.
    pub allows_bulk_download: Option<bool>,
    /// Whether programmatic API access is permitted.
    pub allows_api_access: Option<bool>,
    /// Whether redistribution of the data is permitted.
    pub allows_redistribution: Option<bool>,
    /// Whether web scraping is permitted.
    pub allows_scraping: Option<bool>,
    /// Whether attribution is required when using the data.
    pub attribution_required: Option<bool>,
    /// Required attribution text, if any.
    pub attribution_text: Option<String>,
    /// Description of any rate limits on API access.
    pub rate_limits: Option<String>,
    /// Free-form notes about legal considerations.
    pub notes: Option<String>,
    /// ISO 8601 timestamp when the legal review was performed.
    pub reviewed_at: Option<String>,
}

/// A website that requires scraping to extract crime data.
///
/// Created for leads where data is not available through a standard API
/// and must be extracted from HTML pages, PDFs, or similar formats.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScrapeTarget {
    /// Database primary key.
    pub id: i64,
    /// Associated lead ID.
    pub lead_id: i64,
    /// URL to scrape.
    pub url: String,
    /// Strategy for extracting data from the page.
    pub scrape_strategy: Option<ScrapeStrategy>,
    /// Description of how pagination works (e.g., "offset+limit", "next page link").
    pub pagination_method: Option<String>,
    /// Whether authentication is required to access the data.
    pub auth_required: bool,
    /// Anti-bot protection detected on the site.
    pub anti_bot: Option<AntiBot>,
    /// Estimated development effort (e.g., "1 hour", "1 day").
    pub estimated_effort: Option<String>,
    /// Free-form notes about scraping challenges.
    pub notes: Option<String>,
    /// ISO 8601 timestamp when this target was created.
    pub created_at: String,
}

/// A lead that has address data suitable for geocoding.
///
/// Tracks which fields contain address components and estimates the
/// quality and success rate of geocoding the addresses.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GeocodingCandidate {
    /// Database primary key.
    pub id: i64,
    /// Associated lead ID.
    pub lead_id: i64,
    /// JSON description of which fields contain address components.
    pub address_fields: String,
    /// Field name containing the city, if separate.
    pub city_field: Option<String>,
    /// Field name containing the state, if separate.
    pub state_field: Option<String>,
    /// Field name containing the ZIP code, if separate.
    pub zip_field: Option<String>,
    /// JSON array of sample address strings for testing.
    pub sample_addresses: Option<String>,
    /// Estimated quality of the address data for geocoding.
    pub geocode_quality: Option<GeocodingQuality>,
    /// Estimated fraction (0.0–1.0) of addresses that will geocode successfully.
    pub estimated_geocode_rate: Option<f64>,
    /// Notes from geocoding evaluation.
    pub geocoder_notes: Option<String>,
    /// ISO 8601 timestamp when this candidate was created.
    pub created_at: String,
}

/// A known API pattern used for discovering and classifying data sources.
///
/// Captures common patterns across open-data platforms to speed up
/// discovery and reduce duplicate investigation effort.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiPattern {
    /// Database primary key.
    pub id: i64,
    /// Human-readable name for this pattern (e.g., "Socrata crime dataset").
    pub pattern_name: String,
    /// How sources matching this pattern are typically discovered.
    pub discovery_strategy: String,
    /// JSON description of fields commonly found in this pattern.
    pub typical_fields: Option<String>,
    /// Common data quality issues encountered with this pattern.
    pub typical_issues: Option<String>,
    /// Overall quality rating for sources matching this pattern.
    pub quality_rating: Option<String>,
    /// Free-form notes about this pattern.
    pub notes: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lead_status_round_trip() {
        let variants = [
            LeadStatus::New,
            LeadStatus::Investigating,
            LeadStatus::VerifiedGood,
            LeadStatus::VerifiedNoCoords,
            LeadStatus::VerifiedNoData,
            LeadStatus::VerifiedAggregateOnly,
            LeadStatus::VerifiedProprietary,
            LeadStatus::NeedsGeocoding,
            LeadStatus::NeedsScraper,
            LeadStatus::Integrated,
            LeadStatus::Rejected,
        ];
        for v in variants {
            let s = v.as_str();
            let parsed = LeadStatus::try_from(s).unwrap();
            assert_eq!(v, parsed);
            assert_eq!(v.to_string(), s);
        }
    }

    #[test]
    fn priority_round_trip() {
        for v in [Priority::High, Priority::Medium, Priority::Low] {
            let s = v.as_str();
            let parsed = Priority::try_from(s).unwrap();
            assert_eq!(v, parsed);
            assert_eq!(v.to_string(), s);
        }
    }

    #[test]
    fn api_type_round_trip() {
        let variants = [
            ApiType::Socrata,
            ApiType::Arcgis,
            ApiType::Ckan,
            ApiType::Carto,
            ApiType::Odata,
            ApiType::Csv,
            ApiType::Scrape,
            ApiType::Unknown,
        ];
        for v in variants {
            let s = v.as_str();
            let parsed = ApiType::try_from(s).unwrap();
            assert_eq!(v, parsed);
            assert_eq!(v.to_string(), s);
        }
    }

    #[test]
    fn coordinate_type_round_trip() {
        let variants = [
            CoordinateType::PointGeometry,
            CoordinateType::LatLngF64,
            CoordinateType::LatLngString,
            CoordinateType::AddressOnly,
            CoordinateType::None,
        ];
        for v in variants {
            let s = v.as_str();
            let parsed = CoordinateType::try_from(s).unwrap();
            assert_eq!(v, parsed);
            assert_eq!(v.to_string(), s);
        }
    }

    #[test]
    fn license_type_round_trip() {
        let variants = [
            LicenseType::OpenData,
            LicenseType::CcBy,
            LicenseType::CcBySa,
            LicenseType::CcZero,
            LicenseType::PublicDomain,
            LicenseType::Proprietary,
            LicenseType::TosRestricted,
            LicenseType::Unknown,
        ];
        for v in variants {
            let s = v.as_str();
            let parsed = LicenseType::try_from(s).unwrap();
            assert_eq!(v, parsed);
            assert_eq!(v.to_string(), s);
        }
    }

    #[test]
    fn scrape_strategy_round_trip() {
        let variants = [
            ScrapeStrategy::HtmlTable,
            ScrapeStrategy::JsonPaginated,
            ScrapeStrategy::CsvDownload,
            ScrapeStrategy::PdfExtract,
        ];
        for v in variants {
            let s = v.as_str();
            let parsed = ScrapeStrategy::try_from(s).unwrap();
            assert_eq!(v, parsed);
            assert_eq!(v.to_string(), s);
        }
    }

    #[test]
    fn anti_bot_round_trip() {
        let variants = [
            AntiBot::None,
            AntiBot::Cloudflare,
            AntiBot::DataDome,
            AntiBot::Captcha,
            AntiBot::SessionRequired,
            AntiBot::Unknown,
        ];
        for v in variants {
            let s = v.as_str();
            let parsed = AntiBot::try_from(s).unwrap();
            assert_eq!(v, parsed);
            assert_eq!(v.to_string(), s);
        }
    }

    #[test]
    fn geocoding_quality_round_trip() {
        for v in [
            GeocodingQuality::High,
            GeocodingQuality::Medium,
            GeocodingQuality::Low,
        ] {
            let s = v.as_str();
            let parsed = GeocodingQuality::try_from(s).unwrap();
            assert_eq!(v, parsed);
            assert_eq!(v.to_string(), s);
        }
    }

    #[test]
    fn source_status_round_trip() {
        let variants = [
            SourceStatus::Active,
            SourceStatus::Stale,
            SourceStatus::Broken,
            SourceStatus::Deprecated,
        ];
        for v in variants {
            let s = v.as_str();
            let parsed = SourceStatus::try_from(s).unwrap();
            assert_eq!(v, parsed);
            assert_eq!(v.to_string(), s);
        }
    }

    #[test]
    fn unknown_variant_returns_error() {
        assert!(LeadStatus::try_from("nonexistent").is_err());
        assert!(Priority::try_from("nonexistent").is_err());
        assert!(ApiType::try_from("nonexistent").is_err());
        assert!(CoordinateType::try_from("nonexistent").is_err());
        assert!(LicenseType::try_from("nonexistent").is_err());
        assert!(ScrapeStrategy::try_from("nonexistent").is_err());
        assert!(AntiBot::try_from("nonexistent").is_err());
        assert!(GeocodingQuality::try_from("nonexistent").is_err());
        assert!(SourceStatus::try_from("nonexistent").is_err());
    }

    #[test]
    fn lead_serde_round_trip() {
        let lead = Lead {
            id: 1,
            jurisdiction: "Washington, DC".to_owned(),
            source_name: "DC Open Data".to_owned(),
            api_type: Some(ApiType::Socrata),
            url: Some("https://opendata.dc.gov".to_owned()),
            status: LeadStatus::VerifiedGood,
            priority: Priority::High,
            likelihood: Some(0.95),
            record_count: Some(500_000),
            has_coordinates: Some(true),
            has_dates: Some(true),
            coordinate_type: Some(CoordinateType::PointGeometry),
            date_format: Some("%Y-%m-%dT%H:%M:%S".to_owned()),
            sample_record: Some(r#"{"id": 1}"#.to_owned()),
            field_notes: None,
            distance_from_dc_miles: Some(0.0),
            notes: None,
            discovered_at: "2025-01-15T10:00:00Z".to_owned(),
            updated_at: "2025-01-15T10:00:00Z".to_owned(),
            investigated_at: Some("2025-01-16T08:00:00Z".to_owned()),
        };

        let json = serde_json::to_string(&lead).unwrap();
        let deserialized: Lead = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, lead.id);
        assert_eq!(deserialized.jurisdiction, lead.jurisdiction);
        assert_eq!(deserialized.status, lead.status);
        assert_eq!(deserialized.priority, lead.priority);
    }

    #[test]
    fn source_serde_round_trip() {
        let source = Source {
            id: 1,
            source_id: "dc-metro-police".to_owned(),
            jurisdiction: "Washington, DC".to_owned(),
            api_type: ApiType::Socrata,
            url: "https://opendata.dc.gov/api/crime".to_owned(),
            record_count: Some(1_000_000),
            date_range_start: Some("2020-01-01".to_owned()),
            date_range_end: Some("2025-01-01".to_owned()),
            toml_filename: Some("dc_metro_police.toml".to_owned()),
            status: SourceStatus::Active,
            last_verified: Some("2025-01-15T10:00:00Z".to_owned()),
            notes: None,
        };

        let json = serde_json::to_string(&source).unwrap();
        let deserialized: Source = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.source_id, source.source_id);
        assert_eq!(deserialized.status, source.status);
    }
}
