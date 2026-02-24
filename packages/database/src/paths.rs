#![allow(clippy::module_name_repetitions)]
//! Canonical file paths for the `DuckDB` data directory.
//!
//! All paths are relative to the project root's `data/` directory.

use std::path::{Path, PathBuf};

/// Returns the workspace root directory.
///
/// Resolved at compile time from `CARGO_MANIFEST_DIR`.
///
/// # Panics
///
/// Panics if the project root cannot be resolved.
#[must_use]
pub fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("Failed to find project root from CARGO_MANIFEST_DIR")
        .to_path_buf()
}

/// Returns the `data/` directory path.
#[must_use]
pub fn data_dir() -> PathBuf {
    project_root().join("data")
}

/// Returns the `data/sources/` directory for per-source `DuckDB` files.
#[must_use]
pub fn sources_dir() -> PathBuf {
    data_dir().join("sources")
}

/// Returns the `data/shared/` directory for shared databases.
#[must_use]
pub fn shared_dir() -> PathBuf {
    data_dir().join("shared")
}

/// Returns the path for a specific source's `DuckDB` file.
#[must_use]
pub fn source_db_path(source_id: &str) -> PathBuf {
    sources_dir().join(format!("{source_id}.duckdb"))
}

/// Returns the path for the boundaries `DuckDB` file.
#[must_use]
pub fn boundaries_db_path() -> PathBuf {
    shared_dir().join("boundaries.duckdb")
}

/// Returns the path for the geocode cache `DuckDB` file.
#[must_use]
pub fn geocode_cache_db_path() -> PathBuf {
    shared_dir().join("geocode_cache.duckdb")
}

/// Returns the `data/generated/` directory for output artifacts.
#[must_use]
pub fn generated_dir() -> PathBuf {
    data_dir().join("generated")
}

/// Ensures a directory exists, creating it if necessary.
///
/// # Errors
///
/// Returns an I/O error if the directory cannot be created.
pub fn ensure_dir(path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        std::fs::create_dir_all(path)?;
    }
    Ok(())
}
