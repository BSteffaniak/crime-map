#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Analytics query result types and tool definitions for the AI agent.
//!
//! Defines the input/output types for each analytical tool that the AI
//! agent can invoke, along with JSON Schema descriptions for the LLM
//! tool-use protocol.

use crime_map_geography_models::{AreaStats, CategoryCount, PeriodComparison, TimeSeriesPoint};
use serde::{Deserialize, Serialize};

/// Granularity for time-series queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeGranularity {
    /// Daily counts.
    Daily,
    /// Weekly counts.
    Weekly,
    /// Monthly counts.
    Monthly,
    /// Yearly counts.
    Yearly,
}

impl std::fmt::Display for TimeGranularity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Daily => write!(f, "day"),
            Self::Weekly => write!(f, "week"),
            Self::Monthly => write!(f, "month"),
            Self::Yearly => write!(f, "year"),
        }
    }
}

/// Parameters for counting incidents in an area.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CountIncidentsParams {
    /// City name to filter by.
    pub city: Option<String>,
    /// State abbreviation to filter by.
    pub state: Option<String>,
    /// Census tract GEOID to filter by.
    pub geoid: Option<String>,
    /// Start date (ISO 8601).
    pub date_from: Option<String>,
    /// End date (ISO 8601).
    pub date_to: Option<String>,
    /// Filter by category name.
    pub category: Option<String>,
    /// Filter by subcategory name.
    pub subcategory: Option<String>,
    /// Minimum severity (1-5).
    pub severity_min: Option<u8>,
}

/// Result of counting incidents.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CountIncidentsResult {
    /// Total incidents matching the query.
    pub total: u64,
    /// Breakdown by category.
    pub by_category: Vec<CategoryCount>,
    /// Area description for context.
    pub area_description: String,
    /// Date range description.
    pub date_range: String,
}

/// Parameters for ranking areas by safety.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RankAreaParams {
    /// City to analyze (required — limits tract scope).
    pub city: String,
    /// State abbreviation.
    pub state: Option<String>,
    /// Start date (ISO 8601).
    pub date_from: Option<String>,
    /// End date (ISO 8601).
    pub date_to: Option<String>,
    /// Category to filter by.
    pub category: Option<String>,
    /// Number of results to return.
    pub limit: Option<u32>,
    /// Whether to rank safest-first (true) or most-dangerous-first (false).
    pub safest_first: Option<bool>,
}

/// Result of ranking areas.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RankAreaResult {
    /// Ranked areas.
    pub areas: Vec<AreaStats>,
    /// Description of the ranking criteria.
    pub description: String,
}

/// Parameters for comparing two time periods.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComparePeriodParams {
    /// City name.
    pub city: Option<String>,
    /// State abbreviation.
    pub state: Option<String>,
    /// Census tract GEOID.
    pub geoid: Option<String>,
    /// Period A start date (ISO 8601).
    pub period_a_from: String,
    /// Period A end date (ISO 8601).
    pub period_a_to: String,
    /// Period B start date (ISO 8601).
    pub period_b_from: String,
    /// Period B end date (ISO 8601).
    pub period_b_to: String,
    /// Category to filter by.
    pub category: Option<String>,
}

/// Result of comparing two periods.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComparePeriodResult {
    /// Overall comparison.
    pub overall: PeriodComparison,
    /// Per-category breakdown.
    pub by_category: Vec<PeriodComparison>,
    /// Description of the comparison.
    pub description: String,
}

/// Parameters for time-series trend queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrendParams {
    /// City name.
    pub city: Option<String>,
    /// State abbreviation.
    pub state: Option<String>,
    /// Census tract GEOID.
    pub geoid: Option<String>,
    /// Time granularity.
    pub granularity: TimeGranularity,
    /// Start date (ISO 8601).
    pub date_from: Option<String>,
    /// End date (ISO 8601).
    pub date_to: Option<String>,
    /// Category to filter by.
    pub category: Option<String>,
}

/// Result of a time-series trend query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrendResult {
    /// Time-series data points.
    pub data: Vec<TimeSeriesPoint>,
    /// Description of the trend.
    pub description: String,
}

/// Parameters for finding the top crime types in an area.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopCrimeTypesParams {
    /// City name.
    pub city: Option<String>,
    /// State abbreviation.
    pub state: Option<String>,
    /// Census tract GEOID.
    pub geoid: Option<String>,
    /// Start date (ISO 8601).
    pub date_from: Option<String>,
    /// End date (ISO 8601).
    pub date_to: Option<String>,
    /// Number of results.
    pub limit: Option<u32>,
}

/// Result of a top crime types query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopCrimeTypesResult {
    /// Top subcategories by count.
    pub subcategories: Vec<CategoryCount>,
    /// Top categories by count.
    pub categories: Vec<CategoryCount>,
    /// Total incidents in the query area/period.
    pub total: u64,
    /// Description.
    pub description: String,
}

/// Parameters for listing available cities in the dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListCitiesParams {
    /// Optional state filter.
    pub state: Option<String>,
}

/// Result of listing cities.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListCitiesResult {
    /// Available cities with state abbreviation.
    pub cities: Vec<CityInfo>,
}

/// Information about an available city.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CityInfo {
    /// City name.
    pub city: String,
    /// State abbreviation.
    pub state: String,
    /// Approximate incident count.
    pub incident_count: Option<u64>,
}

/// Parameters for searching available locations by name.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchLocationsParams {
    /// Location name to search for (partial match supported).
    pub query: String,
    /// Optional state filter to narrow results.
    pub state: Option<String>,
}

/// Result of a location search.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchLocationsResult {
    /// Matching locations with incident counts.
    pub matches: Vec<CityInfo>,
    /// Human-readable description of the search.
    pub description: String,
}

/// Enumeration of all tool names the AI agent can invoke.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolName {
    /// Count incidents in a geographic area with filters.
    CountIncidents,
    /// Rank neighborhoods or areas by safety.
    RankAreas,
    /// Compare crime between two time periods.
    ComparePeriods,
    /// Get crime trends over time.
    GetTrend,
    /// Find the most common crime types.
    TopCrimeTypes,
    /// List available cities in the dataset.
    ListCities,
    /// Search for locations by name.
    SearchLocations,
}

impl std::fmt::Display for ToolName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CountIncidents => write!(f, "count_incidents"),
            Self::RankAreas => write!(f, "rank_areas"),
            Self::ComparePeriods => write!(f, "compare_periods"),
            Self::GetTrend => write!(f, "get_trend"),
            Self::TopCrimeTypes => write!(f, "top_crime_types"),
            Self::ListCities => write!(f, "list_cities"),
            Self::SearchLocations => write!(f, "search_locations"),
        }
    }
}

/// Returns the JSON Schema definitions for all available tools.
///
/// These are used in the LLM tool-use protocol to describe what
/// tools the agent can invoke.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn tool_definitions() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "name": "count_incidents",
            "description": "Count crime incidents in a geographic area with optional filters. Use this to answer questions about total crime counts, crime in a city, or filtered queries.",
            "parameters": {
                "type": "object",
                "properties": {
                    "city": { "type": "string", "description": "City name (e.g., 'Chicago', 'Washington')" },
                    "state": { "type": "string", "description": "Two-letter state abbreviation (e.g., 'IL', 'DC')" },
                    "geoid": { "type": "string", "description": "Census tract GEOID for tract-level queries" },
                    "dateFrom": { "type": "string", "description": "Start date in ISO 8601 format (e.g., '2025-01-01')" },
                    "dateTo": { "type": "string", "description": "End date in ISO 8601 format (e.g., '2025-12-31')" },
                    "category": { "type": "string", "description": "Crime category filter: VIOLENT, PROPERTY, DRUG_NARCOTICS, PUBLIC_ORDER, FRAUD_FINANCIAL, OTHER" },
                    "subcategory": { "type": "string", "description": "Crime subcategory filter (e.g., HOMICIDE, BURGLARY, DUI)" },
                    "severityMin": { "type": "integer", "description": "Minimum severity level 1-5 (1=minimal, 5=critical)" }
                },
                "required": []
            }
        }),
        serde_json::json!({
            "name": "rank_areas",
            "description": "Rank neighborhoods or areas within a city by crime rate (per-capita when population data is available, otherwise total count). Results use real neighborhood names when boundary data is loaded, otherwise census tract names. Use this to find the safest or most dangerous neighborhoods.",
            "parameters": {
                "type": "object",
                "properties": {
                    "city": { "type": "string", "description": "City to analyze (required)" },
                    "state": { "type": "string", "description": "Two-letter state abbreviation" },
                    "dateFrom": { "type": "string", "description": "Start date in ISO 8601 format" },
                    "dateTo": { "type": "string", "description": "End date in ISO 8601 format" },
                    "category": { "type": "string", "description": "Crime category filter" },
                    "limit": { "type": "integer", "description": "Number of results to return (default 10)" },
                    "safestFirst": { "type": "boolean", "description": "If true, rank safest first; if false, most dangerous first (default true)" }
                },
                "required": ["city"]
            }
        }),
        serde_json::json!({
            "name": "compare_periods",
            "description": "Compare crime between two time periods for a city or area. Use this for year-over-year analysis, before/after comparisons, etc.",
            "parameters": {
                "type": "object",
                "properties": {
                    "city": { "type": "string", "description": "City name" },
                    "state": { "type": "string", "description": "Two-letter state abbreviation" },
                    "geoid": { "type": "string", "description": "Census tract GEOID" },
                    "periodAFrom": { "type": "string", "description": "Period A start date (ISO 8601)" },
                    "periodATo": { "type": "string", "description": "Period A end date (ISO 8601)" },
                    "periodBFrom": { "type": "string", "description": "Period B start date (ISO 8601)" },
                    "periodBTo": { "type": "string", "description": "Period B end date (ISO 8601)" },
                    "category": { "type": "string", "description": "Crime category filter" }
                },
                "required": ["periodAFrom", "periodATo", "periodBFrom", "periodBTo"]
            }
        }),
        serde_json::json!({
            "name": "get_trend",
            "description": "Get crime count trends over time at a given granularity. Use this for time-series analysis, spotting seasonal patterns, or tracking changes.",
            "parameters": {
                "type": "object",
                "properties": {
                    "city": { "type": "string", "description": "City name" },
                    "state": { "type": "string", "description": "Two-letter state abbreviation" },
                    "geoid": { "type": "string", "description": "Census tract GEOID" },
                    "granularity": { "type": "string", "enum": ["daily", "weekly", "monthly", "yearly"], "description": "Time granularity" },
                    "dateFrom": { "type": "string", "description": "Start date (ISO 8601)" },
                    "dateTo": { "type": "string", "description": "End date (ISO 8601)" },
                    "category": { "type": "string", "description": "Crime category filter" }
                },
                "required": ["granularity"]
            }
        }),
        serde_json::json!({
            "name": "top_crime_types",
            "description": "Find the most common crime types (categories and subcategories) in an area. Use this to understand what kinds of crime are most prevalent.",
            "parameters": {
                "type": "object",
                "properties": {
                    "city": { "type": "string", "description": "City name" },
                    "state": { "type": "string", "description": "Two-letter state abbreviation" },
                    "geoid": { "type": "string", "description": "Census tract GEOID" },
                    "dateFrom": { "type": "string", "description": "Start date (ISO 8601)" },
                    "dateTo": { "type": "string", "description": "End date (ISO 8601)" },
                    "limit": { "type": "integer", "description": "Number of results (default 10)" }
                },
                "required": []
            }
        }),
        serde_json::json!({
            "name": "list_cities",
            "description": "List all cities available in the crime dataset. Use this first to discover what data is available before querying.",
            "parameters": {
                "type": "object",
                "properties": {
                    "state": { "type": "string", "description": "Optional state filter (two-letter abbreviation)" }
                },
                "required": []
            }
        }),
        serde_json::json!({
            "name": "search_locations",
            "description": "Search for available locations in the dataset by name. Use this when a user asks about a specific city, town, or area to find matching or related jurisdictions. Returns cities and counties whose names match the query. If no exact match is found, use your geographic knowledge to search for parent jurisdictions (e.g., search for the county name if a small town isn't found).",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Location name to search for (partial match supported, e.g., 'Capitol Heights', 'Prince George')" },
                    "state": { "type": "string", "description": "Optional two-letter state filter to narrow results" }
                },
                "required": ["query"]
            }
        }),
    ]
}
