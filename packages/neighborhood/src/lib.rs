#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Neighborhood boundary data fetching, normalization, and ingestion.
//!
//! Downloads neighborhood polygon boundaries from city open data portals
//! and Census Bureau APIs, normalizes them, and loads them into `PostGIS`.
//! Sources are defined as TOML files embedded at compile time, following
//! the same registry pattern as the crime data source definitions.

pub mod fetchers;
pub mod ingest;
pub mod normalize;
pub mod registry;

use thiserror::Error;

/// Errors that can occur during neighborhood operations.
#[derive(Debug, Error)]
pub enum NeighborhoodError {
    /// Database operation failed.
    #[error("Database error: {0}")]
    Database(#[from] switchy_database::DatabaseError),

    /// HTTP request failed.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// JSON parsing failed.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Data conversion or normalization error.
    #[error("Conversion error: {message}")]
    Conversion {
        /// Description of what went wrong.
        message: String,
    },
}
