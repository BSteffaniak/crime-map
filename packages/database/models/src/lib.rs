#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions)]

//! Database row types and query parameter definitions.
//!
//! These types represent the shapes of data as stored in and retrieved from
//! the `PostGIS` database. They are distinct from the API response types in
//! `crime_map_server_models` and the normalized ingestion types in
//! `crime_map_source_models`.

use chrono::{DateTime, Utc};
use crime_map_crime_models::{CrimeCategory, CrimeSeverity, CrimeSubcategory};
use serde::{Deserialize, Serialize};

/// A geographic bounding box in WGS84 coordinates.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BoundingBox {
    /// Western longitude boundary.
    pub west: f64,
    /// Southern latitude boundary.
    pub south: f64,
    /// Eastern longitude boundary.
    pub east: f64,
    /// Northern latitude boundary.
    pub north: f64,
}

impl BoundingBox {
    /// Creates a new bounding box from the given coordinates.
    #[must_use]
    pub const fn new(west: f64, south: f64, east: f64, north: f64) -> Self {
        Self {
            west,
            south,
            east,
            north,
        }
    }
}

/// Parameters for querying crime incidents from the database.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IncidentQuery {
    /// Spatial bounding box filter.
    pub bbox: Option<BoundingBox>,
    /// Minimum occurrence date.
    pub from: Option<DateTime<Utc>>,
    /// Maximum occurrence date.
    pub to: Option<DateTime<Utc>>,
    /// Filter by specific top-level categories.
    pub categories: Vec<CrimeCategory>,
    /// Filter by specific subcategories.
    pub subcategories: Vec<CrimeSubcategory>,
    /// Minimum severity level.
    pub severity_min: Option<CrimeSeverity>,
    /// Filter by source IDs.
    pub source_ids: Vec<i32>,
    /// Whether arrest was made (`None` = don't filter).
    pub arrest_made: Option<bool>,
    /// Maximum number of results to return.
    pub limit: u32,
    /// Number of results to skip.
    pub offset: u32,
}

/// A crime incident row as retrieved from the database.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IncidentRow {
    /// Primary key.
    pub id: i64,
    /// Source that provided this incident.
    pub source_id: i32,
    /// Original incident ID from the source.
    pub source_incident_id: String,
    /// Subcategory from the canonical taxonomy.
    pub subcategory: CrimeSubcategory,
    /// Top-level crime category (derived from subcategory).
    pub category: CrimeCategory,
    /// Severity level.
    pub severity: CrimeSeverity,
    /// Longitude (WGS84).
    pub longitude: f64,
    /// Latitude (WGS84).
    pub latitude: f64,
    /// When the crime occurred.
    pub occurred_at: DateTime<Utc>,
    /// When the crime was reported.
    pub reported_at: Option<DateTime<Utc>>,
    /// Short description.
    pub description: Option<String>,
    /// Block-level address.
    pub block_address: Option<String>,
    /// City.
    pub city: String,
    /// Two-letter state abbreviation.
    pub state: String,
    /// Whether an arrest was made.
    pub arrest_made: Option<bool>,
    /// Whether this was a domestic incident.
    pub domestic: Option<bool>,
    /// Location type (street, residence, etc.).
    pub location_type: Option<String>,
}

/// A row from the county-level FBI statistics table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CountyStatsRow {
    /// Primary key.
    pub id: i32,
    /// FIPS county code.
    pub fips_code: String,
    /// Two-letter state abbreviation.
    pub state: String,
    /// County name.
    pub county_name: String,
    /// Data year.
    pub year: i16,
    /// County population.
    pub population: Option<i32>,
    /// Total violent crimes.
    pub violent_crime_total: Option<i32>,
    /// Total property crimes.
    pub property_crime_total: Option<i32>,
    /// Homicides.
    pub murder: Option<i32>,
    /// Rapes.
    pub rape: Option<i32>,
    /// Robberies.
    pub robbery: Option<i32>,
    /// Aggravated assaults.
    pub aggravated_assault: Option<i32>,
    /// Burglaries.
    pub burglary: Option<i32>,
    /// Larceny-thefts.
    pub larceny: Option<i32>,
    /// Motor vehicle thefts.
    pub motor_vehicle_theft: Option<i32>,
    /// Arsons.
    pub arson: Option<i32>,
    /// County centroid longitude.
    pub centroid_longitude: Option<f64>,
    /// County centroid latitude.
    pub centroid_latitude: Option<f64>,
}
