#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Neighborhood boundary source definition types.
//!
//! Defines the TOML schema for neighborhood data sources and the
//! normalized boundary type produced after fetching and parsing.

use serde::{Deserialize, Serialize};

/// A neighborhood boundary data source, deserialized from TOML.
///
/// Each source defines how to fetch neighborhood polygons for a single
/// city from a specific open data API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeighborhoodSource {
    /// Unique source identifier (e.g., `"dc_neighborhoods"`).
    pub id: String,
    /// Human-readable name (e.g., "Washington DC Neighborhood Clusters").
    pub name: String,
    /// City name as it appears in `crime_incidents.city`.
    pub city: String,
    /// Two-letter state abbreviation.
    pub state: String,
    /// Fetcher configuration.
    pub fetcher: NeighborhoodFetcherConfig,
    /// Field mapping for extracting name and geometry.
    pub fields: NeighborhoodFieldMapping,
}

impl NeighborhoodSource {
    /// Returns the source identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the human-readable source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// How to fetch neighborhood boundary data.
///
/// Each variant corresponds to a different open data API type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NeighborhoodFetcherConfig {
    /// `ArcGIS` `FeatureServer` or `MapServer` query endpoint.
    ///
    /// Appends `?where=1%3D1&outFields=...&f=geojson&returnGeometry=true`.
    Arcgis {
        /// Base query URL (up to `.../query`).
        url: String,
        /// Maximum records to request per page (default: 1000).
        max_records: Option<u32>,
    },
    /// Socrata `GeoJSON` export endpoint.
    ///
    /// Fetches `resource.geojson?$limit=N`.
    SocrataGeo {
        /// `GeoJSON` resource URL.
        url: String,
        /// Record limit (default: 5000).
        limit: Option<u32>,
    },
    /// Direct `GeoJSON` URL (static file or API returning standard `GeoJSON`).
    GeojsonUrl {
        /// Full URL that returns a `GeoJSON` `FeatureCollection`.
        url: String,
    },
}

/// Field mapping for extracting neighborhood name and geometry from
/// raw API responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeighborhoodFieldMapping {
    /// Property field containing the neighborhood name.
    pub name: String,
    /// How to extract polygon geometry from each feature.
    pub geometry: GeometryExtractor,
}

/// How to extract polygon geometry from a feature.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GeometryExtractor {
    /// Geometry is already a standard `GeoJSON` object.
    Geojson,
    /// Geometry uses Esri JSON format (`{ "rings": [...] }`), needs
    /// conversion to `GeoJSON`.
    EsriRings,
}

/// A normalized neighborhood boundary, ready for database insertion.
#[derive(Debug, Clone)]
pub struct NormalizedBoundary {
    /// Human-readable neighborhood name.
    pub name: String,
    /// `GeoJSON` geometry as a JSON string for `ST_GeomFromGeoJSON`.
    pub geometry_json: String,
}
