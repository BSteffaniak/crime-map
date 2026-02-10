#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Analytical query engine for AI agent tool execution.
//!
//! Each public function corresponds to a tool that the AI agent can invoke.
//! Functions accept structured parameter types, execute optimized `PostGIS`
//! queries, and return typed results that the agent feeds back to the LLM.

pub mod tools;

use thiserror::Error;

/// Errors that can occur during analytics operations.
#[derive(Debug, Error)]
pub enum AnalyticsError {
    /// Database operation failed.
    #[error("Database error: {0}")]
    Database(#[from] switchy_database::DatabaseError),

    /// Data conversion error.
    #[error("Conversion error: {message}")]
    Conversion {
        /// Description of what went wrong.
        message: String,
    },
}
