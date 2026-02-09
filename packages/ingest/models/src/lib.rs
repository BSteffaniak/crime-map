#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Ingestion configuration, progress, and result types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for a data fetch operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchConfig {
    /// Only fetch records newer than this timestamp.
    pub since: Option<DateTime<Utc>>,
    /// Maximum number of records to fetch (useful for testing).
    pub limit: Option<u64>,
    /// Directory where downloaded files are stored.
    pub output_dir: PathBuf,
}

/// Result of a completed import operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportResult {
    /// Name of the data source that was imported.
    pub source_name: String,
    /// Total number of records fetched from the source.
    pub records_fetched: u64,
    /// Number of new records inserted into the database.
    pub records_inserted: u64,
    /// Number of records skipped (duplicates).
    pub records_skipped: u64,
    /// How long the import took.
    pub duration: Duration,
}

/// Progress reporting during import.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ImportProgress {
    /// Downloading raw data from the source.
    Downloading {
        /// Percentage complete (0.0 - 1.0).
        progress: f32,
        /// Number of records downloaded so far.
        records: u64,
    },
    /// Normalizing records to the canonical schema.
    Normalizing {
        /// Percentage complete (0.0 - 1.0).
        progress: f32,
    },
    /// Inserting normalized records into the database.
    Inserting {
        /// Percentage complete (0.0 - 1.0).
        progress: f32,
        /// Number of records inserted so far.
        inserted: u64,
    },
    /// Import is complete.
    Complete(ImportResult),
}
