#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Shared types for the Tantivy-based geocoder index.
//!
//! This crate contains only data types, configuration structs, and simple
//! conversions. It has no heavyweight dependencies (no Tantivy, no I/O).

use serde::{Deserialize, Serialize};

/// Configuration for building or opening a geocoder index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeocoderIndexConfig {
    /// Directory where the Tantivy index is stored.
    pub index_dir: String,

    /// Memory budget for the Tantivy `IndexWriter` in bytes.
    /// Defaults to 256 MB.
    #[serde(default = "default_writer_heap")]
    pub writer_heap_bytes: usize,
}

const fn default_writer_heap() -> usize {
    256 * 1024 * 1024 // 256 MB
}

impl Default for GeocoderIndexConfig {
    fn default() -> Self {
        Self {
            index_dir: String::new(),
            writer_heap_bytes: default_writer_heap(),
        }
    }
}

/// A single geocoding search result from the index.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Latitude (WGS84).
    pub latitude: f64,
    /// Longitude (WGS84).
    pub longitude: f64,
    /// The matched street address from the index.
    pub matched_street: String,
    /// The matched city.
    pub matched_city: String,
    /// The matched state (two-letter abbreviation).
    pub matched_state: String,
    /// The data source this address came from.
    pub source: AddressSource,
    /// Tantivy relevance score (higher is better).
    pub score: f32,
}

/// Origin of an address record in the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AddressSource {
    /// `OpenAddresses` dataset.
    OpenAddresses,
    /// OpenStreetMap.
    Osm,
}

impl AddressSource {
    /// String tag used in the Tantivy index.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenAddresses => "oa",
            Self::Osm => "osm",
        }
    }

    /// Parses from the stored string tag.
    #[must_use]
    pub fn from_str_tag(s: &str) -> Option<Self> {
        match s {
            "oa" => Some(Self::OpenAddresses),
            "osm" => Some(Self::Osm),
            _ => None,
        }
    }
}

/// Statistics about a built geocoder index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    /// Total number of documents in the index.
    pub total_documents: u64,
    /// Number of documents from `OpenAddresses`.
    pub openaddresses_count: u64,
    /// Number of documents from OSM.
    pub osm_count: u64,
    /// Index size on disk in bytes.
    pub index_size_bytes: u64,
    /// Time taken to build the index in seconds.
    pub build_time_secs: f64,
}
