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
    /// When the crime occurred (ISO 8601).
    pub occurred_at: DateTime<Utc>,
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
    /// When the crime occurred (ISO 8601).
    pub occurred_at: String,
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
