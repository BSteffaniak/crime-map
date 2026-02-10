#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Crime data source definitions and fetchers.
//!
//! Each data provider is defined by a TOML config file in `packages/source/sources/`
//! and handled by the generic [`source_def::SourceDefinition`] implementation.
//! Use [`registry::all_sources`] to get all configured sources.
//!
//! Data flows through a streaming pipeline: pages of raw JSON records are
//! fetched and sent through a channel, then normalized and inserted into
//! the database one page at a time.

pub mod arcgis;
pub mod carto;
pub mod ckan;
pub mod parsing;
pub mod registry;
pub mod socrata;
pub mod source_def;
pub mod type_mapping;

/// Errors that can occur during data source operations.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    /// HTTP request failed.
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// JSON parsing failed.
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    /// I/O error (file read/write).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Data normalization error.
    #[error("Normalization error: {message}")]
    Normalization {
        /// Description of what went wrong.
        message: String,
    },
}

/// Configuration for fetching data from a source.
#[derive(Debug, Clone)]
pub struct FetchOptions {
    /// Only fetch records newer than this timestamp.
    pub since: Option<chrono::DateTime<chrono::Utc>>,
    /// Maximum number of records to fetch.
    pub limit: Option<u64>,
    /// Starting offset for resume after an interrupted sync. When non-zero,
    /// fetchers skip this many records (via API pagination offset) to avoid
    /// re-downloading pages that were already ingested. The database's
    /// `ON CONFLICT DO NOTHING` handles any small overlap at the boundary.
    pub resume_offset: u64,
}
