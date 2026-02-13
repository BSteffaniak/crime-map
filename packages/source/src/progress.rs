//! Progress reporting trait for long-running operations.
//!
//! Defines a [`ProgressCallback`] trait that decouples progress reporting
//! from any specific rendering backend (e.g., `indicatif` progress bars,
//! log-only reporting, or silence). Implementations are provided upstream
//! in crates that choose a rendering strategy.

use std::sync::Arc;

/// Trait for reporting progress from long-running operations.
///
/// Implementations must be `Send + Sync` to support use across spawned
/// tokio tasks and `Arc`-based sharing.
pub trait ProgressCallback: Send + Sync {
    /// Set the total expected units of work (enables percentage/ETA).
    fn set_total(&self, total: u64);

    /// Set the current position (absolute, not delta).
    fn set_position(&self, pos: u64);

    /// Advance progress by `delta` units.
    fn inc(&self, delta: u64);

    /// Update the message displayed alongside the progress indicator.
    fn set_message(&self, msg: String);

    /// Mark progress as complete with a final message.
    fn finish(&self, msg: String);

    /// Mark progress as complete and remove the progress indicator.
    fn finish_and_clear(&self);
}

/// A no-op implementation of [`ProgressCallback`] that silently ignores
/// all progress updates.
///
/// Useful for CLI commands and tests that do not need visual progress
/// reporting.
pub struct NullProgress;

impl ProgressCallback for NullProgress {
    fn set_total(&self, _total: u64) {}
    fn set_position(&self, _pos: u64) {}
    fn inc(&self, _delta: u64) {}
    fn set_message(&self, _msg: String) {}
    fn finish(&self, _msg: String) {}
    fn finish_and_clear(&self) {}
}

/// Returns a shared [`NullProgress`] instance for convenient use.
#[must_use]
pub fn null_progress() -> Arc<dyn ProgressCallback> {
    Arc::new(NullProgress)
}
