#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions)]

//! Crime data source trait and normalization logic.
//!
//! Each data provider implements the [`CrimeSource`] trait to define how
//! raw data is fetched, parsed, and mapped to the canonical taxonomy.

pub mod arcgis;
pub mod socrata;
pub mod sources;
pub mod type_mapping;

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use crime_map_source_models::NormalizedIncident;

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
    /// Directory to store downloaded files.
    pub output_dir: PathBuf,
}

/// Trait that all crime data sources must implement.
///
/// Each source knows how to fetch its raw data, parse it, and normalize
/// it into the canonical [`NormalizedIncident`] format.
#[async_trait]
pub trait CrimeSource: Send + Sync {
    /// Returns a unique identifier for this source (e.g., `"chicago_pd"`).
    fn id(&self) -> &str;

    /// Returns the human-readable name of this source.
    fn name(&self) -> &str;

    /// Downloads raw data from the source, returning the path to the
    /// downloaded file(s).
    ///
    /// # Errors
    ///
    /// Returns [`SourceError`] if the download fails.
    async fn fetch(&self, options: &FetchOptions) -> Result<PathBuf, SourceError>;

    /// Parses the raw downloaded data and normalizes it into canonical
    /// incidents.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError`] if parsing or normalization fails.
    async fn normalize(&self, raw_path: &Path) -> Result<Vec<NormalizedIncident>, SourceError>;
}
