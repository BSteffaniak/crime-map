//! Spatial index re-exported from the shared `crime_map_spatial` package.
//!
//! This module previously contained the full R-tree implementation. It now
//! delegates to [`crime_map_spatial`] so the same index can be shared with
//! the ingestion enrichment step.

pub use crime_map_spatial::SpatialIndex;
