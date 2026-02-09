#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions)]

//! Database connection, queries, and migrations for the crime map.
//!
//! Uses `switchy_database` for the query builder and `switchy_schema` for
//! embedded SQL migrations. `PostGIS` spatial queries use raw SQL via
//! `query_raw_params()`.

pub mod db;
pub mod queries;

use include_dir::{Dir, include_dir};
use switchy_database::Database;
use switchy_schema::discovery::embedded::EmbeddedMigrationSource;
use switchy_schema::runner::MigrationRunner;

/// Embedded SQL migrations from the `migrations/` directory.
static MIGRATIONS_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/../../migrations");

/// Errors that can occur during database operations.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// Database query error.
    #[error("Database error: {0}")]
    Database(#[from] switchy_database::DatabaseError),

    /// Migration error.
    #[error("Migration error: {0}")]
    Migration(#[from] switchy_schema::MigrationError),

    /// Data conversion error.
    #[error("Data conversion error: {message}")]
    Conversion {
        /// Description of what went wrong.
        message: String,
    },
}

/// Runs all pending database migrations.
///
/// # Errors
///
/// Returns [`DbError`] if any migration fails to apply.
pub async fn run_migrations(db: &dyn Database) -> Result<(), DbError> {
    let source = EmbeddedMigrationSource::new(&MIGRATIONS_DIR);
    let runner = MigrationRunner::new(Box::new(source));
    runner.run(db).await?;
    log::info!("Database migrations completed successfully");
    Ok(())
}
