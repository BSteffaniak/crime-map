#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Data source configuration types and the canonical normalized incident format.
//!
//! Every crime data provider (city API, FBI bulk download, etc.) produces
//! [`NormalizedIncident`] records that conform to the shared taxonomy in
//! [`crime_map_crime_models`].

use chrono::{DateTime, Utc};
use crime_map_crime_models::CrimeSubcategory;
use serde::{Deserialize, Serialize};
use strum_macros::{AsRefStr, Display, EnumString};

/// The type of data provider.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    AsRefStr,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum SourceType {
    /// Socrata or similar city open-data API
    CityApi,
    /// FBI NIBRS incident-level data
    FbiNibrs,
    /// FBI UCR aggregated statistics
    FbiUcr,
    /// Bulk CSV or similar flat-file download
    CsvBulk,
    /// `ArcGIS` REST API
    ArcgisApi,
}

/// Configuration for a crime data source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceConfig {
    /// Unique identifier for this source.
    pub id: String,
    /// Human-readable name (e.g., "Chicago Police Department").
    pub name: String,
    /// What kind of data provider this is.
    pub source_type: SourceType,
    /// Base URL or API endpoint for fetching data.
    pub api_url: Option<String>,
    /// Geographic coverage description (e.g., "Chicago, IL" or "National").
    pub coverage_area: String,
    /// City name, if this is a city-level source.
    pub city: Option<String>,
    /// Two-letter state abbreviation, if applicable.
    pub state: Option<String>,
}

/// A crime incident normalized to the canonical schema.
///
/// All data sources produce this type after parsing and mapping their
/// source-specific formats. Coordinates are optional — incidents without
/// precise lat/lng can still be stored for counting purposes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedIncident {
    /// Original incident ID from the data source (for deduplication).
    pub source_incident_id: String,
    /// Mapped crime subcategory from the canonical taxonomy.
    pub subcategory: CrimeSubcategory,
    /// Longitude (WGS84). `None` if the source lacks coordinates.
    pub longitude: Option<f64>,
    /// Latitude (WGS84). `None` if the source lacks coordinates.
    pub latitude: Option<f64>,
    /// When the crime occurred. `None` when the source record has a missing
    /// or unparseable date field.
    pub occurred_at: Option<DateTime<Utc>>,
    /// When the crime was reported (may differ from occurrence).
    pub reported_at: Option<DateTime<Utc>>,
    /// Short description of the incident.
    pub description: Option<String>,
    /// Block-level address (e.g., "100 N STATE ST").
    pub block_address: Option<String>,
    /// City where the incident occurred.
    pub city: String,
    /// Two-letter state abbreviation.
    pub state: String,
    /// Whether an arrest was made.
    pub arrest_made: Option<bool>,
    /// Whether this was a domestic incident.
    pub domestic: Option<bool>,
    /// Type of location (e.g., "STREET", "RESIDENCE", "COMMERCIAL").
    pub location_type: Option<String>,
    /// Whether this incident was geocoded by us (vs coordinates from source).
    pub geocoded: bool,
}

/// A record in the `crime_sources` table tracking a data provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceRecord {
    /// Database primary key.
    pub id: i32,
    /// Human-readable source name.
    pub name: String,
    /// Type of data provider.
    pub source_type: SourceType,
    /// API endpoint URL, if applicable.
    pub api_url: Option<String>,
    /// Human-readable portal URL for the dataset.
    pub portal_url: Option<String>,
    /// When this source was last synced.
    pub last_synced_at: Option<DateTime<Utc>>,
    /// Total number of records from this source.
    pub record_count: i64,
    /// Coverage area description.
    pub coverage_area: String,
}
