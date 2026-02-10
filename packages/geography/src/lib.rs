#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Census tract boundary data management and ingestion.
//!
//! Downloads Census Bureau TIGER/Line boundary data via the `TIGERweb`
//! REST API and loads census tract polygons into `PostGIS`. These boundaries
//! enable geographic aggregation for AI-powered analytics queries.

pub mod ingest;
pub mod queries;

use thiserror::Error;

/// Errors that can occur during geography operations.
#[derive(Debug, Error)]
pub enum GeoError {
    /// Database operation failed.
    #[error("Database error: {0}")]
    Database(#[from] switchy_database::DatabaseError),

    /// HTTP request failed.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// JSON parsing failed.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Data conversion error.
    #[error("Conversion error: {message}")]
    Conversion {
        /// Description of what went wrong.
        message: String,
    },
}
