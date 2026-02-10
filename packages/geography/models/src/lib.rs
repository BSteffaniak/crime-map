#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Census tract and geographic boundary types.
//!
//! These types represent geographic areas (census tracts) used to answer
//! questions like "which neighborhood is safest". They are independent of
//! the main crime incident data.

use serde::{Deserialize, Serialize};

/// A census tract row as stored in the database.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CensusTract {
    /// Census GEOID (state FIPS + county FIPS + tract code, e.g. "11001000100").
    pub geoid: String,
    /// Human-readable tract name (e.g. "Census Tract 1").
    pub name: String,
    /// Two-digit state FIPS code.
    pub state_fips: String,
    /// Three-digit county FIPS code.
    pub county_fips: String,
    /// State abbreviation (e.g. "DC", "IL").
    pub state_abbr: Option<String>,
    /// County name.
    pub county_name: Option<String>,
    /// Land area in square miles.
    pub land_area_sq_mi: Option<f64>,
    /// Population from ACS estimates.
    pub population: Option<i32>,
    /// Centroid longitude.
    pub centroid_lon: Option<f64>,
    /// Centroid latitude.
    pub centroid_lat: Option<f64>,
}

/// Summary statistics for a geographic area over a time period.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AreaStats {
    /// Geographic area identifier (GEOID for tracts, city name, etc.).
    pub area_id: String,
    /// Human-readable area name.
    pub area_name: String,
    /// Total incident count in this area.
    pub total_incidents: u64,
    /// Incidents per 1,000 residents (if population data available).
    pub incidents_per_1k: Option<f64>,
    /// Land area in square miles.
    pub land_area_sq_mi: Option<f64>,
    /// Incidents per square mile (if land area data available).
    pub incidents_per_sq_mi: Option<f64>,
    /// Breakdown by top-level category.
    pub by_category: Vec<CategoryCount>,
}

/// Count of incidents in a single category.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CategoryCount {
    /// Category name (`SCREAMING_SNAKE_CASE`).
    pub category: String,
    /// Number of incidents.
    pub count: u64,
}

/// A time-series data point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TimeSeriesPoint {
    /// Period label (e.g. "2025-01", "2025-W03", "2025-01-15").
    pub period: String,
    /// Total incident count in this period.
    pub count: u64,
}

/// Comparison between two time periods for an area.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PeriodComparison {
    /// Area identifier.
    pub area_id: String,
    /// Area name.
    pub area_name: String,
    /// Count in the first (earlier) period.
    pub period_a_count: u64,
    /// Count in the second (later) period.
    pub period_b_count: u64,
    /// Percentage change from period A to period B.
    pub percent_change: f64,
}
