#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! API request and response types for the crime map server.
//!
//! These types are serialized to JSON for the REST API. They are separate
//! from the database row types to allow independent evolution of the API
//! contract.

use chrono::{DateTime, Utc};
use crime_map_crime_models::{CrimeCategory, CrimeSeverity, CrimeSubcategory};
use crime_map_database_models::IncidentRow;
use serde::{Deserialize, Serialize};

/// A crime incident as returned by the API.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiIncident {
    /// Unique incident ID.
    pub id: i64,
    /// Top-level crime category.
    pub category: CrimeCategory,
    /// Specific subcategory.
    pub subcategory: CrimeSubcategory,
    /// Severity level name.
    pub severity: CrimeSeverity,
    /// Severity numeric value (1-5).
    pub severity_value: u8,
    /// Longitude.
    pub longitude: f64,
    /// Latitude.
    pub latitude: f64,
    /// When the crime occurred (ISO 8601). `None` if the source had no
    /// parseable date.
    pub occurred_at: Option<DateTime<Utc>>,
    /// Short description.
    pub description: Option<String>,
    /// Block-level address.
    pub block_address: Option<String>,
    /// City.
    pub city: String,
    /// State abbreviation.
    pub state: String,
    /// Whether an arrest was made.
    pub arrest_made: Option<bool>,
    /// Location type.
    pub location_type: Option<String>,
}

impl From<IncidentRow> for ApiIncident {
    fn from(row: IncidentRow) -> Self {
        Self {
            id: row.id,
            category: row.category,
            subcategory: row.subcategory,
            severity: row.severity,
            severity_value: row.severity.value(),
            longitude: row.longitude,
            latitude: row.latitude,
            occurred_at: row.occurred_at,
            description: row.description,
            block_address: row.block_address,
            city: row.city,
            state: row.state,
            arrest_made: row.arrest_made,
            location_type: row.location_type,
        }
    }
}

/// Query parameters for the incidents endpoint.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncidentQueryParams {
    /// Bounding box as `west,south,east,north`.
    pub bbox: Option<String>,
    /// Start date for temporal filtering (ISO 8601).
    pub from: Option<DateTime<Utc>>,
    /// End date for temporal filtering (ISO 8601).
    pub to: Option<DateTime<Utc>>,
    /// Comma-separated list of category names to include.
    pub categories: Option<String>,
    /// Comma-separated list of subcategory names to include.
    pub subcategories: Option<String>,
    /// Minimum severity value (1-5).
    pub severity_min: Option<u8>,
    /// Maximum number of results.
    pub limit: Option<u32>,
    /// Offset for pagination.
    pub offset: Option<u32>,
}

/// A node in the crime category tree returned by the API.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiCategoryNode {
    /// Category name.
    pub name: String,
    /// Default severity for this category.
    pub severity: u8,
    /// Child subcategories (empty for leaf nodes).
    pub children: Vec<ApiSubcategoryNode>,
}

/// A subcategory leaf node in the category tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiSubcategoryNode {
    /// Subcategory name.
    pub name: String,
    /// Severity level for this subcategory.
    pub severity: u8,
}

/// Summary statistics for a viewport or query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiStatsSummary {
    /// Total incident count matching the query.
    pub total_count: u64,
    /// Breakdown by top-level category.
    pub by_category: Vec<ApiCategoryCount>,
}

/// Count of incidents for a single category.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiCategoryCount {
    /// Category name.
    pub category: CrimeCategory,
    /// Number of incidents.
    pub count: u64,
}

/// Health check response.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiHealth {
    /// Whether the service is healthy.
    pub healthy: bool,
    /// Service version.
    pub version: String,
    /// Whether pre-generated data files are loaded and ready.
    pub data_ready: bool,
}

/// Query parameters for the sidebar endpoint.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SidebarQueryParams {
    /// Bounding box as `west,south,east,north`.
    pub bbox: Option<String>,
    /// Maximum number of feature results per page.
    pub limit: Option<u32>,
    /// Offset for feature pagination.
    pub offset: Option<u32>,
    /// Start date for temporal filtering (ISO 8601).
    pub from: Option<String>,
    /// End date for temporal filtering (ISO 8601).
    pub to: Option<String>,
    /// Comma-separated list of category names to include.
    pub categories: Option<String>,
    /// Comma-separated list of subcategory names to include.
    pub subcategories: Option<String>,
    /// Minimum severity value (1-5).
    pub severity_min: Option<u8>,
    /// Filter by arrest status.
    pub arrest_made: Option<bool>,
    /// Comma-separated list of source IDs to include.
    pub sources: Option<String>,
    /// Comma-separated list of state FIPS codes to include.
    pub state_fips: Option<String>,
    /// Comma-separated list of county GEOIDs to include.
    pub county_geoids: Option<String>,
    /// Comma-separated list of place GEOIDs to include.
    pub place_geoids: Option<String>,
    /// Comma-separated list of tract GEOIDs to include.
    pub tract_geoids: Option<String>,
    /// Comma-separated list of neighborhood IDs to include.
    pub neighborhood_ids: Option<String>,
}

/// Response from the sidebar endpoint.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SidebarResponse {
    /// Paginated incident features.
    pub features: Vec<SidebarIncident>,
    /// Total count of incidents matching the query within the bbox.
    pub total_count: u64,
    /// Whether more features are available beyond this page.
    pub has_more: bool,
}

/// A crime incident as returned by the sidebar endpoint.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SidebarIncident {
    /// Unique incident ID.
    pub id: i64,
    /// Source identifier (e.g., `"dc_mpd"`).
    pub source_id: String,
    /// Human-readable data source name.
    pub source_name: String,
    /// Source-specific incident ID.
    pub source_incident_id: Option<String>,
    /// Specific subcategory.
    pub subcategory: String,
    /// Top-level crime category.
    pub category: String,
    /// Severity numeric value (1-5).
    pub severity: i32,
    /// Longitude.
    pub longitude: f64,
    /// Latitude.
    pub latitude: f64,
    /// When the crime occurred (ISO 8601). `None` if the source had no
    /// parseable date.
    pub occurred_at: Option<String>,
    /// Short description.
    pub description: Option<String>,
    /// Block-level address.
    pub block_address: Option<String>,
    /// City.
    pub city: Option<String>,
    /// State abbreviation.
    pub state: Option<String>,
    /// Whether an arrest was made.
    pub arrest_made: Option<bool>,
    /// Location type.
    pub location_type: Option<String>,
}

/// Shared filter fields used by both the sidebar and cluster count queries
/// against the `DuckDB` `count_summary` table.
#[derive(Debug, Clone)]
pub struct CountFilterParams {
    /// Start date for temporal filtering (ISO 8601).
    pub from: Option<String>,
    /// End date for temporal filtering (ISO 8601).
    pub to: Option<String>,
    /// Comma-separated list of category names to include.
    pub categories: Option<String>,
    /// Comma-separated list of subcategory names to include.
    pub subcategories: Option<String>,
    /// Minimum severity value (1-5).
    pub severity_min: Option<u8>,
    /// Filter by arrest status.
    pub arrest_made: Option<bool>,
    /// Comma-separated list of source IDs to include.
    pub sources: Option<String>,
    /// Comma-separated list of state FIPS codes to include.
    pub state_fips: Option<String>,
    /// Comma-separated list of county GEOIDs to include.
    pub county_geoids: Option<String>,
    /// Comma-separated list of place GEOIDs to include.
    pub place_geoids: Option<String>,
    /// Comma-separated list of tract GEOIDs to include.
    pub tract_geoids: Option<String>,
    /// Comma-separated list of neighborhood IDs to include.
    pub neighborhood_ids: Option<String>,
}

impl From<&SidebarQueryParams> for CountFilterParams {
    fn from(p: &SidebarQueryParams) -> Self {
        Self {
            from: p.from.clone(),
            to: p.to.clone(),
            categories: p.categories.clone(),
            subcategories: p.subcategories.clone(),
            severity_min: p.severity_min,
            arrest_made: p.arrest_made,
            sources: p.sources.clone(),
            state_fips: p.state_fips.clone(),
            county_geoids: p.county_geoids.clone(),
            place_geoids: p.place_geoids.clone(),
            tract_geoids: p.tract_geoids.clone(),
            neighborhood_ids: p.neighborhood_ids.clone(),
        }
    }
}

impl From<&ClusterQueryParams> for CountFilterParams {
    fn from(p: &ClusterQueryParams) -> Self {
        Self {
            from: p.from.clone(),
            to: p.to.clone(),
            categories: p.categories.clone(),
            subcategories: p.subcategories.clone(),
            severity_min: p.severity_min,
            arrest_made: p.arrest_made,
            sources: p.sources.clone(),
            state_fips: p.state_fips.clone(),
            county_geoids: p.county_geoids.clone(),
            place_geoids: p.place_geoids.clone(),
            tract_geoids: p.tract_geoids.clone(),
            neighborhood_ids: p.neighborhood_ids.clone(),
        }
    }
}

impl From<&HexbinQueryParams> for CountFilterParams {
    fn from(p: &HexbinQueryParams) -> Self {
        Self {
            from: p.from.clone(),
            to: p.to.clone(),
            categories: p.categories.clone(),
            subcategories: p.subcategories.clone(),
            severity_min: p.severity_min,
            arrest_made: p.arrest_made,
            sources: p.sources.clone(),
            state_fips: p.state_fips.clone(),
            county_geoids: p.county_geoids.clone(),
            place_geoids: p.place_geoids.clone(),
            tract_geoids: p.tract_geoids.clone(),
            neighborhood_ids: p.neighborhood_ids.clone(),
        }
    }
}

/// Query parameters for the clusters endpoint.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterQueryParams {
    /// Bounding box as `west,south,east,north`.
    pub bbox: Option<String>,
    /// Current map zoom level.
    pub zoom: Option<u8>,
    /// Target number of output clusters (overrides server default).
    pub k: Option<usize>,
    /// Start date for temporal filtering (ISO 8601).
    pub from: Option<String>,
    /// End date for temporal filtering (ISO 8601).
    pub to: Option<String>,
    /// Comma-separated list of category names to include.
    pub categories: Option<String>,
    /// Comma-separated list of subcategory names to include.
    pub subcategories: Option<String>,
    /// Minimum severity value (1-5).
    pub severity_min: Option<u8>,
    /// Filter by arrest status.
    pub arrest_made: Option<bool>,
    /// Comma-separated list of source IDs to include.
    pub sources: Option<String>,
    /// Comma-separated list of state FIPS codes to include.
    pub state_fips: Option<String>,
    /// Comma-separated list of county GEOIDs to include.
    pub county_geoids: Option<String>,
    /// Comma-separated list of place GEOIDs to include.
    pub place_geoids: Option<String>,
    /// Comma-separated list of tract GEOIDs to include.
    pub tract_geoids: Option<String>,
    /// Comma-separated list of neighborhood IDs to include.
    pub neighborhood_ids: Option<String>,
}

/// A single cluster entry in the clusters endpoint response.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterEntry {
    /// Weighted centroid longitude.
    pub lng: f64,
    /// Weighted centroid latitude.
    pub lat: f64,
    /// Filtered incident count in this cluster.
    pub count: u64,
}

/// Query parameters for the hexbins endpoint.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HexbinQueryParams {
    /// Bounding box as `west,south,east,north`.
    pub bbox: Option<String>,
    /// Current map zoom level.
    pub zoom: Option<u8>,
    /// Start date for temporal filtering (ISO 8601).
    pub from: Option<String>,
    /// End date for temporal filtering (ISO 8601).
    pub to: Option<String>,
    /// Comma-separated list of category names to include.
    pub categories: Option<String>,
    /// Comma-separated list of subcategory names to include.
    pub subcategories: Option<String>,
    /// Minimum severity value (1-5).
    pub severity_min: Option<u8>,
    /// Filter by arrest status.
    pub arrest_made: Option<bool>,
    /// Comma-separated list of source IDs to include.
    pub sources: Option<String>,
    /// Comma-separated list of state FIPS codes to include.
    pub state_fips: Option<String>,
    /// Comma-separated list of county GEOIDs to include.
    pub county_geoids: Option<String>,
    /// Comma-separated list of place GEOIDs to include.
    pub place_geoids: Option<String>,
    /// Comma-separated list of tract GEOIDs to include.
    pub tract_geoids: Option<String>,
    /// Comma-separated list of neighborhood IDs to include.
    pub neighborhood_ids: Option<String>,
}

/// A single hexbin entry in the hexbins endpoint response.
///
/// Serialized via `MessagePack` for compact binary payloads. Each entry
/// contains an H3 cell's boundary vertices and the aggregated incident
/// count within that cell.
#[derive(Debug, Clone, Serialize)]
pub struct HexbinEntry {
    /// Hex boundary vertices as `[[lng, lat], ...]` (typically 6 points).
    pub vertices: Vec<[f64; 2]>,
    /// Filtered incident count in this hexagonal cell.
    pub count: u64,
}

/// A data source as returned by the `GET /api/sources` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiSource {
    /// Source identifier (e.g., `"dc_mpd"`, `"chicago_pd"`).
    pub id: String,
    /// Human-readable source name.
    pub name: String,
    /// Total number of records from this source.
    pub record_count: i64,
    /// City covered by this source.
    #[serde(default)]
    pub city: String,
    /// State abbreviation covered by this source.
    #[serde(default)]
    pub state: String,
    /// Human-readable portal URL for the dataset.
    pub portal_url: Option<String>,
}

/// Query parameters for the source-counts endpoint.
///
/// Returns per-source incident counts within a viewport, filtered by
/// the same dimensions as the sidebar and hexbin endpoints.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceCountsQueryParams {
    /// Bounding box as `west,south,east,north`.
    pub bbox: Option<String>,
    /// Start date for temporal filtering (ISO 8601).
    pub from: Option<String>,
    /// End date for temporal filtering (ISO 8601).
    pub to: Option<String>,
    /// Comma-separated list of category names to include.
    pub categories: Option<String>,
    /// Comma-separated list of subcategory names to include.
    pub subcategories: Option<String>,
    /// Minimum severity value (1-5).
    pub severity_min: Option<u8>,
    /// Filter by arrest status.
    pub arrest_made: Option<bool>,
    /// Comma-separated list of state FIPS codes to include.
    pub state_fips: Option<String>,
    /// Comma-separated list of county GEOIDs to include.
    pub county_geoids: Option<String>,
    /// Comma-separated list of place GEOIDs to include.
    pub place_geoids: Option<String>,
    /// Comma-separated list of tract GEOIDs to include.
    pub tract_geoids: Option<String>,
    /// Comma-separated list of neighborhood IDs to include.
    pub neighborhood_ids: Option<String>,
}

impl From<&SourceCountsQueryParams> for CountFilterParams {
    fn from(p: &SourceCountsQueryParams) -> Self {
        Self {
            from: p.from.clone(),
            to: p.to.clone(),
            categories: p.categories.clone(),
            subcategories: p.subcategories.clone(),
            severity_min: p.severity_min,
            arrest_made: p.arrest_made,
            sources: None, // Don't filter by source — we want counts for ALL sources
            state_fips: p.state_fips.clone(),
            county_geoids: p.county_geoids.clone(),
            place_geoids: p.place_geoids.clone(),
            tract_geoids: p.tract_geoids.clone(),
            neighborhood_ids: p.neighborhood_ids.clone(),
        }
    }
}

/// Query parameters for the boundary-counts endpoint.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BoundaryCountsQueryParams {
    /// Boundary type: "state", "county", "place", "tract", or "neighborhood".
    #[serde(rename = "type")]
    pub boundary_type: String,
    /// Bounding box as `west,south,east,north`.
    pub bbox: Option<String>,
    /// Start date for temporal filtering (ISO 8601).
    pub from: Option<String>,
    /// End date for temporal filtering (ISO 8601).
    pub to: Option<String>,
    /// Comma-separated list of category names to include.
    pub categories: Option<String>,
    /// Comma-separated list of subcategory names to include.
    pub subcategories: Option<String>,
    /// Minimum severity value (1-5).
    pub severity_min: Option<u8>,
    /// Filter by arrest status.
    pub arrest_made: Option<bool>,
    /// Comma-separated list of source IDs to include.
    pub sources: Option<String>,
    /// Comma-separated list of state FIPS codes to include.
    pub state_fips: Option<String>,
    /// Comma-separated list of county GEOIDs to include.
    pub county_geoids: Option<String>,
    /// Comma-separated list of place GEOIDs to include.
    pub place_geoids: Option<String>,
    /// Comma-separated list of tract GEOIDs to include.
    pub tract_geoids: Option<String>,
    /// Comma-separated list of neighborhood IDs to include.
    pub neighborhood_ids: Option<String>,
}

impl From<&BoundaryCountsQueryParams> for CountFilterParams {
    fn from(p: &BoundaryCountsQueryParams) -> Self {
        Self {
            from: p.from.clone(),
            to: p.to.clone(),
            categories: p.categories.clone(),
            subcategories: p.subcategories.clone(),
            severity_min: p.severity_min,
            arrest_made: p.arrest_made,
            sources: p.sources.clone(),
            state_fips: p.state_fips.clone(),
            county_geoids: p.county_geoids.clone(),
            place_geoids: p.place_geoids.clone(),
            tract_geoids: p.tract_geoids.clone(),
            neighborhood_ids: p.neighborhood_ids.clone(),
        }
    }
}

/// Response for the boundary-counts endpoint.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BoundaryCountsResponse {
    /// Boundary type that was queried.
    #[serde(rename = "type")]
    pub boundary_type: String,
    /// Map of geoid -> incident count.
    pub counts: std::collections::BTreeMap<String, u64>,
}

/// Query parameters for the boundary search endpoint.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BoundarySearchParams {
    /// Search query string.
    pub q: String,
    /// Boundary type filter: "state", "county", "place", "tract", "neighborhood".
    #[serde(rename = "type")]
    pub boundary_type: Option<String>,
    /// Maximum number of results.
    pub limit: Option<u32>,
}

/// A boundary search result.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BoundarySearchResult {
    /// Boundary GEOID or synthetic ID.
    pub geoid: String,
    /// Display name.
    pub name: String,
    /// Full name including state.
    pub full_name: Option<String>,
    /// State abbreviation.
    pub state_abbr: Option<String>,
    /// Population (if available).
    pub population: Option<i64>,
    /// Boundary type.
    #[serde(rename = "type")]
    pub boundary_type: String,
}
