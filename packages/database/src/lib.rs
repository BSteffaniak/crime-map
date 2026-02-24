#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! DuckDB-based data storage for the crime map ingestion and generation
//! pipeline.
//!
//! Provides per-source incident storage, shared boundary data, and a
//! geocoding cache — all backed by `DuckDB` files on disk.
//!
//! ## File Layout
//!
//! ```text
//! data/
//! ├── sources/                    # Per-source DuckDB files
//! │   ├── chicago_pd.duckdb
//! │   └── ...
//! ├── shared/                     # Shared databases
//! │   ├── boundaries.duckdb
//! │   └── geocode_cache.duckdb
//! └── generated/                  # Output artifacts
//! ```

pub mod boundaries_db;
pub mod geocode_cache;
pub mod paths;
pub mod source_db;

/// Errors that can occur during database operations.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// `DuckDB` error.
    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),

    /// Data conversion error.
    #[error("Data conversion error: {message}")]
    Conversion {
        /// Description of what went wrong.
        message: String,
    },

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
