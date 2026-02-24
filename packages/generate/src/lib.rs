#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Library for generating `PMTiles`, sidebar `SQLite`, and count `DuckDB`
//! databases from `PostGIS` crime incident data.
//!
//! Exports crime incident data as `GeoJSONSeq`, then runs tippecanoe to
//! produce `PMTiles` (heatmap/points). A `SQLite` database with R-tree
//! spatial indexing is generated for server-side sidebar queries.
//!
//! Supports checksum-based caching: a manifest file tracks per-source
//! fingerprints so unchanged data is not re-exported. Each output is
//! tracked independently, allowing partial regeneration after interrupted
//! runs or when only some outputs are missing.
//!
//! Uses keyset pagination and streaming writes to keep memory usage constant
//! regardless of dataset size.

pub mod interactive;
pub mod merge;
pub mod spatial;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use crime_map_source::progress::ProgressCallback;
use crime_map_source::registry::all_sources;
use moosicbox_json_utils::database::ToValue as _;
use serde::{Deserialize, Serialize};
use switchy_database::{Database, DatabaseValue};
use switchy_database_connection::init_sqlite_rusqlite;

/// Number of rows to fetch per database query batch.
const BATCH_SIZE: i64 = 10_000;

/// Current manifest schema version. Bump this when the manifest format
/// changes in a backward-incompatible way.
const MANIFEST_VERSION: u32 = 1;

/// Output name constant for the incidents `PMTiles` file.
pub const OUTPUT_INCIDENTS_PMTILES: &str = "incidents_pmtiles";

/// Output name constant for the sidebar `SQLite` database.
pub const OUTPUT_INCIDENTS_DB: &str = "incidents_db";

/// Output name constant for the count `DuckDB` database.
pub const OUTPUT_COUNT_DB: &str = "count_duckdb";

/// Output name constant for the H3 hexbin `DuckDB` database.
pub const OUTPUT_H3_DB: &str = "h3_duckdb";

/// Output name constant for the server metadata JSON file.
pub const OUTPUT_METADATA: &str = "metadata";

/// Output name constant for the boundaries `PMTiles` file.
pub const OUTPUT_BOUNDARIES_PMTILES: &str = "boundaries_pmtiles";

/// Output name constant for the boundaries search `SQLite` database.
pub const OUTPUT_BOUNDARIES_DB: &str = "boundaries_db";

/// Output name constant for the analytics `DuckDB` database.
pub const OUTPUT_ANALYTICS_DB: &str = "analytics_duckdb";

/// Per-source fingerprint capturing the data state at generation time.
///
/// Since `crime_incidents` is insert-only (`ON CONFLICT DO NOTHING`),
/// the combination of `record_count`, `last_synced_at`, and
/// `max_incident_id` is a reliable change indicator.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SourceFingerprint {
    source_id: i32,
    name: String,
    record_count: i64,
    last_synced_at: Option<String>,
    max_incident_id: i64,
}

/// Generation manifest stored at `data/generated/manifest.json`.
///
/// Records the data state and CLI config at the time of last generation
/// so subsequent runs can skip unchanged outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    source_fingerprints: Vec<SourceFingerprint>,
    /// Sorted list of `--sources` short IDs, or `None` for all sources.
    sources_filter: Option<Vec<String>>,
    /// The `--limit` value used, or `None` for unlimited.
    limit: Option<u64>,
    /// Map of output name to ISO 8601 timestamp of last successful
    /// generation.
    outputs: BTreeMap<String, String>,
}

/// Returns the workspace root directory.
///
/// Resolved at compile time from `CARGO_MANIFEST_DIR`. This ensures output
/// paths are always relative to the project root regardless of the caller's
/// working directory.
///
/// # Panics
///
/// Panics if the project root cannot be resolved from `CARGO_MANIFEST_DIR`.
#[must_use]
pub fn output_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("Failed to find project root from CARGO_MANIFEST_DIR")
        .join("data/generated")
}

/// Shared arguments for all generate subcommands.
pub struct GenerateArgs {
    /// Maximum number of records to export (useful for testing).
    pub limit: Option<u64>,

    /// Comma-separated list of source IDs to include (e.g., "chicago,la,sf").
    /// Only incidents from these sources will be exported.
    pub sources: Option<String>,

    /// Comma-separated state FIPS codes to include (e.g., "24,11" for MD+DC).
    /// Sources whose `state` field matches the given FIPS codes will be
    /// included. Combined with `--sources` via union if both are provided.
    pub states: Option<String>,

    /// Keep the intermediate `.geojsonseq` file after generation instead of
    /// deleting it.
    pub keep_intermediate: bool,

    /// Force regeneration even if source data hasn't changed.
    pub force: bool,
}

/// Runs the generation pipeline with manifest-based caching.
///
/// Compares current source fingerprints against the stored manifest to
/// determine which `requested_outputs` actually need regeneration. Skips
/// outputs that are already up-to-date unless `--force` is specified.
///
/// # Errors
///
/// Returns an error if the database query, file I/O, or any generation
/// step fails.
///
/// # Panics
///
/// Panics if a spatial-index-dependent output is requested but the index
/// failed to load (this should never happen in practice since load errors
/// are propagated before the output runs).
#[allow(clippy::too_many_lines)]
pub async fn run_with_cache(
    db: &dyn Database,
    args: &GenerateArgs,
    source_ids: &[i32],
    dir: &Path,
    requested_outputs: &[&str],
    progress: Option<Arc<dyn ProgressCallback>>,
) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Querying source fingerprints...");
    let fingerprints = query_fingerprints(db, source_ids).await?;

    // Count the actual exportable records (must match the export WHERE clause)
    let total_records = count_exportable_records(db, source_ids).await?;
    log::info!(
        "Found {} sources, {total_records} exportable records",
        fingerprints.len()
    );

    // Each output processes all records, so total work = outputs_needing_regen * total_records.
    // But we use a single progress bar showing records for the current output being generated.
    let progress = progress.unwrap_or_else(crime_map_source::progress::null_progress);

    let mut manifest = load_manifest(dir);
    let sources_filter = sorted_sources_filter(args);

    // Determine what needs regeneration
    let needs: BTreeMap<&str, bool> = requested_outputs
        .iter()
        .map(|&name| {
            let path = output_file_path(dir, name);
            let needed = output_needs_regen(
                manifest.as_ref(),
                &fingerprints,
                name,
                &path,
                sources_filter.as_deref(),
                args.limit,
                args.force,
            );
            (name, needed)
        })
        .collect();

    if needs.values().all(|&v| !v) {
        log::info!("All requested outputs are up-to-date, nothing to regenerate");
        return Ok(());
    }

    for (&name, &needed) in &needs {
        if needed {
            log::info!("{name}: needs regeneration");
        } else {
            log::info!("{name}: up-to-date, skipping");
        }
    }

    // Ensure we have a manifest to update
    let manifest = manifest.get_or_insert_with(|| Manifest {
        version: MANIFEST_VERSION,
        source_fingerprints: Vec::new(),
        sources_filter: None,
        limit: None,
        outputs: BTreeMap::new(),
    });

    // Build spatial index if any output that uses it is needed
    let needs_spatial = needs.get(OUTPUT_INCIDENTS_DB) == Some(&true)
        || needs.get(OUTPUT_COUNT_DB) == Some(&true)
        || needs.get(OUTPUT_H3_DB) == Some(&true)
        || needs.get(OUTPUT_INCIDENTS_PMTILES) == Some(&true)
        || needs.get(OUTPUT_ANALYTICS_DB) == Some(&true);

    let spatial_index = if needs_spatial {
        progress.set_message("Loading spatial index (tracts + places)...".to_string());
        progress.set_total(0);
        progress.set_position(0);
        Some(crate::spatial::SpatialIndex::load(db).await?)
    } else {
        None
    };

    // Run each output that needs it
    if needs.get(OUTPUT_INCIDENTS_PMTILES) == Some(&true) {
        progress.set_message("Generating PMTiles...".to_string());
        progress.set_total(total_records);
        progress.set_position(0);
        generate_pmtiles(
            db,
            args,
            source_ids,
            dir,
            spatial_index
                .as_ref()
                .expect("spatial index required for PMTiles"),
            &progress,
        )
        .await?;
        record_output(manifest, OUTPUT_INCIDENTS_PMTILES);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_INCIDENTS_DB) == Some(&true) {
        progress.set_message("Generating sidebar DB...".to_string());
        progress.set_total(total_records);
        progress.set_position(0);
        generate_sidebar_db(
            db,
            args,
            source_ids,
            dir,
            spatial_index.as_ref().expect("spatial index required"),
            &progress,
        )
        .await?;
        record_output(manifest, OUTPUT_INCIDENTS_DB);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_COUNT_DB) == Some(&true) {
        progress.set_message("Generating count DB...".to_string());
        progress.set_total(total_records);
        progress.set_position(0);
        generate_count_db(
            db,
            args,
            source_ids,
            dir,
            spatial_index.as_ref().expect("spatial index required"),
            &progress,
        )
        .await?;
        record_output(manifest, OUTPUT_COUNT_DB);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_H3_DB) == Some(&true) {
        progress.set_message("Generating H3 hexbin DB...".to_string());
        progress.set_total(total_records);
        progress.set_position(0);
        generate_h3_db(
            db,
            args,
            source_ids,
            dir,
            spatial_index.as_ref().expect("spatial index required"),
            &progress,
        )
        .await?;
        record_output(manifest, OUTPUT_H3_DB);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_METADATA) == Some(&true) {
        progress.set_message("Generating server metadata...".to_string());
        progress.set_total(0);
        progress.set_position(0);
        generate_metadata(db, source_ids, dir).await?;
        record_output(manifest, OUTPUT_METADATA);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_BOUNDARIES_PMTILES) == Some(&true) {
        progress.set_message("Generating boundaries PMTiles...".to_string());
        progress.set_total(0);
        progress.set_position(0);
        generate_boundaries_pmtiles(db, dir, &progress).await?;
        record_output(manifest, OUTPUT_BOUNDARIES_PMTILES);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_BOUNDARIES_DB) == Some(&true) {
        progress.set_message("Generating boundaries search DB...".to_string());
        progress.set_total(0);
        progress.set_position(0);
        generate_boundaries_db(db, dir).await?;
        record_output(manifest, OUTPUT_BOUNDARIES_DB);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_ANALYTICS_DB) == Some(&true) {
        progress.set_message("Generating analytics DB...".to_string());
        progress.set_total(total_records);
        progress.set_position(0);
        generate_analytics_db(
            db,
            args,
            source_ids,
            dir,
            spatial_index.as_ref().expect("spatial index required"),
            &progress,
        )
        .await?;
        record_output(manifest, OUTPUT_ANALYTICS_DB);
        save_manifest(dir, manifest)?;
    }

    // Update manifest with current fingerprints and config
    manifest.source_fingerprints.clone_from(&fingerprints);
    manifest.sources_filter.clone_from(&sources_filter);
    manifest.limit = args.limit;
    manifest.version = MANIFEST_VERSION;
    save_manifest(dir, manifest)?;

    cleanup_intermediate(args, dir);

    Ok(())
}

// ============================================================
// Manifest / caching infrastructure
// ============================================================

/// Queries `PostGIS` for per-source fingerprints used to detect data changes.
///
/// Returns one [`SourceFingerprint`] per source, ordered by `source_id`.
/// If `source_ids` is empty, returns fingerprints for all sources.
///
/// # Errors
///
/// Returns an error if the database query fails.
async fn query_fingerprints(
    db: &dyn Database,
    source_ids: &[i32],
) -> Result<Vec<SourceFingerprint>, Box<dyn std::error::Error>> {
    let (query, params) = if source_ids.is_empty() {
        (
            "SELECT cs.id as source_id, cs.name, cs.record_count,
                    cs.last_synced_at, COALESCE(MAX(ci.id), 0) as max_incident_id
             FROM crime_sources cs
             LEFT JOIN crime_incidents ci ON ci.source_id = cs.id
             GROUP BY cs.id, cs.name, cs.record_count, cs.last_synced_at
             ORDER BY cs.id"
                .to_string(),
            Vec::new(),
        )
    } else {
        let mut params: Vec<DatabaseValue> = Vec::new();
        let placeholders: Vec<String> = source_ids
            .iter()
            .enumerate()
            .map(|(i, &sid)| {
                params.push(DatabaseValue::Int32(sid));
                format!("${}", i + 1)
            })
            .collect();
        let query = format!(
            "SELECT cs.id as source_id, cs.name, cs.record_count,
                    cs.last_synced_at, COALESCE(MAX(ci.id), 0) as max_incident_id
             FROM crime_sources cs
             LEFT JOIN crime_incidents ci ON ci.source_id = cs.id
             WHERE cs.id IN ({})
             GROUP BY cs.id, cs.name, cs.record_count, cs.last_synced_at
             ORDER BY cs.id",
            placeholders.join(", ")
        );
        (query, params)
    };

    let rows = db.query_raw_params(&query, &params).await?;

    let fingerprints = rows
        .iter()
        .map(|row| {
            let last_synced: Option<chrono::NaiveDateTime> =
                row.to_value("last_synced_at").unwrap_or(None);
            let last_synced_str = last_synced.map(|dt| {
                chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc)
                    .to_rfc3339()
            });

            SourceFingerprint {
                source_id: row.to_value("source_id").unwrap_or(0),
                name: row.to_value("name").unwrap_or_default(),
                record_count: row.to_value("record_count").unwrap_or(0),
                last_synced_at: last_synced_str,
                max_incident_id: row.to_value("max_incident_id").unwrap_or(0),
            }
        })
        .collect();

    Ok(fingerprints)
}

/// Counts incidents that will actually be exported.
///
/// Uses the same `has_coordinates = TRUE` filter as [`build_batch_query`] so
/// the progress bar total matches the real feature count.
///
/// # Errors
///
/// Returns an error if the database query fails.
async fn count_exportable_records(
    db: &dyn Database,
    source_ids: &[i32],
) -> Result<u64, Box<dyn std::error::Error>> {
    let (query, params) = if source_ids.is_empty() {
        (
            "SELECT COUNT(*) as cnt FROM crime_incidents WHERE has_coordinates = TRUE".to_string(),
            Vec::new(),
        )
    } else {
        let mut params: Vec<DatabaseValue> = Vec::new();
        let placeholders: Vec<String> = source_ids
            .iter()
            .enumerate()
            .map(|(i, &sid)| {
                params.push(DatabaseValue::Int32(sid));
                format!("${}", i + 1)
            })
            .collect();
        let query = format!(
            "SELECT COUNT(*) as cnt FROM crime_incidents
             WHERE has_coordinates = TRUE AND source_id IN ({})",
            placeholders.join(", ")
        );
        (query, params)
    };

    let rows = db.query_raw_params(&query, &params).await?;
    let count: i64 = rows
        .first()
        .map_or(0, |row| row.to_value("cnt").unwrap_or(0));

    #[allow(clippy::cast_sign_loss)]
    Ok(count as u64)
}

/// Loads the generation manifest from `dir/manifest.json`.
///
/// Returns `None` if the file does not exist or cannot be parsed.
fn load_manifest(dir: &Path) -> Option<Manifest> {
    let path = dir.join("manifest.json");
    let Ok(contents) = std::fs::read_to_string(&path) else {
        log::info!("No existing manifest found");
        return None;
    };
    match serde_json::from_str(&contents) {
        Ok(m) => {
            log::info!("Loaded manifest from {}", path.display());
            Some(m)
        }
        Err(e) => {
            log::warn!("Failed to parse manifest {}: {e}", path.display());
            None
        }
    }
}

/// Writes the generation manifest to `dir/manifest.json`.
///
/// Uses an atomic write pattern (write to `.tmp`, then rename) to avoid
/// corrupt manifests from interrupted writes.
///
/// # Errors
///
/// Returns an error if the file cannot be written.
fn save_manifest(dir: &Path, manifest: &Manifest) -> Result<(), Box<dyn std::error::Error>> {
    let path = dir.join("manifest.json");
    let tmp_path = dir.join("manifest.json.tmp");
    let contents = serde_json::to_string_pretty(manifest)?;
    std::fs::write(&tmp_path, contents)?;
    std::fs::rename(&tmp_path, &path)?;
    log::info!("Saved manifest to {}", path.display());
    Ok(())
}

/// Records a successful output generation in the manifest.
fn record_output(manifest: &mut Manifest, output_name: &str) {
    manifest
        .outputs
        .insert(output_name.to_string(), chrono::Utc::now().to_rfc3339());
}

/// Returns the file path for a given output name.
#[must_use]
fn output_file_path(dir: &Path, output_name: &str) -> PathBuf {
    match output_name {
        OUTPUT_INCIDENTS_PMTILES => dir.join("incidents.pmtiles"),
        OUTPUT_INCIDENTS_DB => dir.join("incidents.db"),
        OUTPUT_COUNT_DB => dir.join("counts.duckdb"),
        OUTPUT_H3_DB => dir.join("h3.duckdb"),
        OUTPUT_METADATA => dir.join("metadata.json"),
        OUTPUT_BOUNDARIES_PMTILES => dir.join("boundaries.pmtiles"),
        OUTPUT_BOUNDARIES_DB => dir.join("boundaries.db"),
        OUTPUT_ANALYTICS_DB => dir.join("analytics.duckdb"),
        _ => dir.join(output_name),
    }
}

/// Normalizes the `--sources` flag into a sorted list for manifest comparison.
fn sorted_sources_filter(args: &GenerateArgs) -> Option<Vec<String>> {
    args.sources.as_ref().map(|s| {
        let mut v: Vec<String> = s.split(',').map(|x| x.trim().to_string()).collect();
        v.sort();
        v
    })
}

/// Determines whether a specific output needs regeneration.
///
/// Returns `true` if any of: `force` is set, no manifest exists, manifest
/// version mismatch, source fingerprints changed, CLI config changed
/// (`--sources` or `--limit`), output not recorded in manifest, or output
/// file missing from disk.
fn output_needs_regen(
    manifest: Option<&Manifest>,
    current_fingerprints: &[SourceFingerprint],
    output_name: &str,
    output_path: &Path,
    sources_filter: Option<&[String]>,
    limit: Option<u64>,
    force: bool,
) -> bool {
    if force {
        return true;
    }

    let Some(m) = manifest else {
        return true;
    };

    if m.version != MANIFEST_VERSION {
        return true;
    }

    if m.source_fingerprints != current_fingerprints {
        return true;
    }

    if m.sources_filter.as_deref() != sources_filter {
        return true;
    }

    if m.limit != limit {
        return true;
    }

    if !m.outputs.contains_key(output_name) {
        return true;
    }

    if !output_path.exists() {
        return true;
    }

    false
}

/// Resolves `--sources` and/or `--states` filters to database integer
/// `source_id` values.
///
/// When `--sources` is provided, looks up each short ID in the TOML
/// registry and then resolves the human-readable name in `crime_sources`.
///
/// When `--states` is provided, maps FIPS codes to state abbreviations
/// and filters the registry by the `state` field on each source.
///
/// If both are provided, their results are unioned (deduplicated).
///
/// Returns an empty `Vec` if neither flag was provided (meaning: export
/// all sources).
///
/// # Errors
///
/// Returns an error if a provided source ID does not match any configured
/// source, or if the database lookup fails.
pub async fn resolve_source_ids(
    db: &dyn Database,
    args: &GenerateArgs,
) -> Result<Vec<i32>, Box<dyn std::error::Error>> {
    if args.sources.is_none() && args.states.is_none() {
        return Ok(Vec::new());
    }

    let registry = all_sources();
    let mut short_ids: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    // Collect source IDs from --sources
    if let Some(ref sources_str) = args.sources {
        for id in sources_str.split(',').map(str::trim) {
            if !id.is_empty() {
                short_ids.insert(id.to_string());
            }
        }
    }

    // Collect source IDs from --states (FIPS -> abbreviation -> matching sources)
    if let Some(ref states_str) = args.states {
        let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
        let abbrs: Vec<String> = fips_codes
            .iter()
            .map(|f| crime_map_geography_models::fips::state_abbr(f).to_string())
            .collect();

        for source in &registry {
            if abbrs.iter().any(|a| a.eq_ignore_ascii_case(&source.state)) {
                short_ids.insert(source.id().to_string());
            }
        }
    }

    if short_ids.is_empty() {
        return Err("No sources matched the provided --sources / --states filters".into());
    }

    let mut source_ids = Vec::with_capacity(short_ids.len());
    for short_id in &short_ids {
        let Some(def) = registry.iter().find(|s| s.id() == short_id.as_str()) else {
            return Err(format!("Unknown source ID: {short_id}").into());
        };

        let rows = db
            .query_raw_params(
                "SELECT id FROM crime_sources WHERE name = $1",
                &[DatabaseValue::String(def.name().to_string())],
            )
            .await?;

        let Some(row) = rows.first() else {
            log::warn!(
                "Source '{}' ({}) not found in database — skipping",
                short_id,
                def.name()
            );
            continue;
        };

        let id: i32 = row
            .to_value("id")
            .map_err(|e| format!("Failed to parse source_id for {short_id}: {e}"))?;
        source_ids.push(id);
        log::info!("Resolved source '{short_id}' -> source_id {id}");
    }

    if source_ids.is_empty() {
        return Err("None of the requested sources were found in the database".into());
    }

    Ok(source_ids)
}

/// Deletes the intermediate `.geojsonseq` file unless `--keep-intermediate`
/// was specified.
fn cleanup_intermediate(args: &GenerateArgs, dir: &Path) {
    let path = dir.join("incidents.geojsonseq");
    if args.keep_intermediate {
        log::info!("Keeping intermediate file: {}", path.display());
        return;
    }
    if path.exists() {
        match std::fs::remove_file(&path) {
            Ok(()) => log::info!("Cleaned up intermediate file: {}", path.display()),
            Err(e) => log::warn!("Failed to remove intermediate file {}: {e}", path.display()),
        }
    }
}

/// Exports incidents as `GeoJSONSeq` and generates `PMTiles` via tippecanoe.
async fn generate_pmtiles(
    db: &dyn Database,
    args: &GenerateArgs,
    source_ids: &[i32],
    dir: &Path,
    geo_index: &crate::spatial::SpatialIndex,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<(), Box<dyn std::error::Error>> {
    let geojsonseq_path = dir.join("incidents.geojsonseq");

    log::info!("Exporting incidents to GeoJSONSeq...");
    export_geojsonseq(
        db,
        &geojsonseq_path,
        args.limit,
        source_ids,
        geo_index,
        progress,
    )
    .await?;

    log::info!("Running tippecanoe to generate PMTiles...");

    let output_path = dir.join("incidents.pmtiles");

    let status = Command::new("tippecanoe")
        .args([
            "-o",
            &output_path.to_string_lossy(),
            "--force",
            "--no-feature-limit",
            "--no-tile-size-limit",
            "--minimum-zoom=0",
            "--maximum-zoom=14",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            "--layer=incidents",
            &geojsonseq_path.to_string_lossy(),
        ])
        .status()?;

    if !status.success() {
        return Err("tippecanoe failed".into());
    }

    log::info!("PMTiles generated: {}", output_path.display());
    Ok(())
}

/// Inserts a single `PostGIS` incident row into the `SQLite` sidebar database.
///
/// Returns the row's primary key ID for keyset pagination tracking.
///
/// # Errors
///
/// Returns an error if the row extraction or `SQLite` insert fails.
async fn insert_sidebar_row(
    txn: &dyn Database,
    row: &switchy_database::Row,
    geo_index: &crate::spatial::SpatialIndex,
) -> Result<i64, Box<dyn std::error::Error>> {
    let id: i64 = row.to_value("id").unwrap_or(0);
    let source_id: i32 = row.to_value("source_id").unwrap_or(0);
    let source_name: String = row.to_value("source_name").unwrap_or_default();
    let lng: f64 = row.to_value("longitude").unwrap_or(0.0);
    let lat: f64 = row.to_value("latitude").unwrap_or(0.0);
    let source_incident_id: String = row.to_value("source_incident_id").unwrap_or_default();
    let subcategory: String = row.to_value("subcategory").unwrap_or_default();
    let category: String = row.to_value("category").unwrap_or_default();
    let severity: i32 = row.to_value("severity").unwrap_or(1);
    let city: String = row.to_value("city").unwrap_or_default();
    let state: String = row.to_value("state").unwrap_or_default();
    let arrest_made: Option<bool> = row.to_value("arrest_made").unwrap_or(None);
    let description: Option<String> = row.to_value("description").unwrap_or(None);
    let block_address: Option<String> = row.to_value("block_address").unwrap_or(None);
    let location_type: Option<String> = row.to_value("location_type").unwrap_or(None);

    let occurred_at_naive: Option<chrono::NaiveDateTime> =
        row.to_value("occurred_at").unwrap_or(None);
    let occurred_at = occurred_at_naive.map(|naive| {
        chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive, chrono::Utc).to_rfc3339()
    });

    let arrest_int = arrest_made.map(|b| DatabaseValue::Int32(i32::from(b)));

    // Boundary GEOIDs — computed via Rust spatial index
    let tract_geoid = geo_index.lookup_tract(lng, lat).map(str::to_owned);
    let state_fips = tract_geoid
        .as_deref()
        .and_then(crate::spatial::SpatialIndex::derive_state_fips)
        .map(str::to_owned);
    let county_geoid = tract_geoid
        .as_deref()
        .and_then(crate::spatial::SpatialIndex::derive_county_geoid)
        .map(str::to_owned);
    let place_geoid = geo_index.lookup_place(lng, lat).map(str::to_owned);
    let neighborhood_id = tract_geoid
        .as_deref()
        .and_then(|g| geo_index.lookup_neighborhood(g))
        .map(str::to_owned);

    txn.exec_raw_params(
        "INSERT INTO incidents (id, source_id, source_name, source_incident_id,
            subcategory, category,
            severity, longitude, latitude, occurred_at, description,
            block_address, city, state, arrest_made, location_type,
            state_fips, county_geoid, place_geoid, tract_geoid, neighborhood_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)",
        &[
            DatabaseValue::Int64(id),
            DatabaseValue::Int32(source_id),
            DatabaseValue::String(source_name),
            DatabaseValue::String(source_incident_id),
            DatabaseValue::String(subcategory),
            DatabaseValue::String(category),
            DatabaseValue::Int32(severity),
            DatabaseValue::Real64(lng),
            DatabaseValue::Real64(lat),
            occurred_at.map_or(DatabaseValue::Null, DatabaseValue::String),
            description.map_or(DatabaseValue::Null, DatabaseValue::String),
            block_address.map_or(DatabaseValue::Null, DatabaseValue::String),
            DatabaseValue::String(city),
            DatabaseValue::String(state),
            arrest_int.unwrap_or(DatabaseValue::Null),
            location_type.map_or(DatabaseValue::Null, DatabaseValue::String),
            state_fips.map_or(DatabaseValue::Null, DatabaseValue::String),
            county_geoid.map_or(DatabaseValue::Null, DatabaseValue::String),
            place_geoid.map_or(DatabaseValue::Null, DatabaseValue::String),
            tract_geoid.map_or(DatabaseValue::Null, DatabaseValue::String),
            neighborhood_id.map_or(DatabaseValue::Null, DatabaseValue::String),
        ],
    )
    .await?;

    Ok(id)
}

/// Generates a `SQLite` database for server-side sidebar queries.
///
/// Creates `incidents.db` with:
/// - A flat `incidents` table containing all incident data
/// - An R-tree spatial index for efficient count queries
/// - A date index for efficient paginated feature queries
///
/// The R-tree enables fast `COUNT(*)` within a bounding box.
/// Feature queries walk the date index and check bbox inline,
/// relying on `LIMIT` to short-circuit early.
///
/// # Errors
///
/// Returns an error if the `PostGIS` export, `SQLite` creation, or
/// index population fails.
#[allow(clippy::too_many_lines)]
async fn generate_sidebar_db(
    db: &dyn Database,
    args: &GenerateArgs,
    source_ids: &[i32],
    dir: &Path,
    geo_index: &crate::spatial::SpatialIndex,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = dir.join("incidents.db");

    // Remove any existing file so we start fresh
    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
    }

    log::info!("Creating sidebar SQLite database...");

    let sqlite = init_sqlite_rusqlite(Some(&db_path))?;

    // Create schema
    sqlite
        .exec_raw(
            "CREATE TABLE incidents (
                id INTEGER PRIMARY KEY,
                source_id INTEGER NOT NULL,
                source_name TEXT NOT NULL,
                source_incident_id TEXT,
                subcategory TEXT NOT NULL,
                category TEXT NOT NULL,
                severity INTEGER NOT NULL,
                longitude REAL NOT NULL,
                latitude REAL NOT NULL,
                occurred_at TEXT,
                description TEXT,
                block_address TEXT,
                city TEXT,
                state TEXT,
                arrest_made INTEGER,
                location_type TEXT,
                state_fips TEXT,
                county_geoid TEXT,
                place_geoid TEXT,
                tract_geoid TEXT,
                neighborhood_id TEXT
            )",
        )
        .await?;

    sqlite
        .exec_raw(
            "CREATE VIRTUAL TABLE incidents_rtree USING rtree(
                id, min_lng, max_lng, min_lat, max_lat
            )",
        )
        .await?;

    // Populate from PostGIS using keyset pagination
    let mut last_id: i64 = 0;
    let mut total_count: u64 = 0;
    let mut remaining = args.limit;

    loop {
        #[allow(clippy::cast_sign_loss)]
        let batch_limit = match remaining {
            Some(0) => break,
            Some(r) => i64::try_from(r.min(BATCH_SIZE as u64))?,
            None => BATCH_SIZE,
        };

        let (query, params) = build_batch_query(last_id, batch_limit, source_ids);
        let rows = db.query_raw_params(&query, &params).await?;

        if rows.is_empty() {
            break;
        }

        #[allow(clippy::cast_possible_truncation)]
        let batch_len = rows.len() as u64;

        // Use a transaction for each batch
        let txn = sqlite.begin_transaction().await?;

        for row in &rows {
            last_id = insert_sidebar_row(txn.as_ref(), row, geo_index).await?;
        }

        txn.commit().await?;

        total_count += batch_len;
        if let Some(ref mut r) = remaining {
            *r = r.saturating_sub(batch_len);
        }

        progress.inc(batch_len);
        log::info!("Inserted {total_count} rows into sidebar DB...");

        #[allow(clippy::cast_sign_loss)]
        let batch_limit_u64 = batch_limit as u64;
        if batch_len < batch_limit_u64 {
            break;
        }
    }

    // Populate R-tree from incidents table
    log::info!("Populating R-tree spatial index...");
    sqlite
        .exec_raw(
            "INSERT INTO incidents_rtree (id, min_lng, max_lng, min_lat, max_lat)
             SELECT id, longitude, longitude, latitude, latitude FROM incidents",
        )
        .await?;

    // Create date index for feature queries
    log::info!("Creating date index...");
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_occurred_at ON incidents(occurred_at DESC)")
        .await?;

    // Create source index for source filtering
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_source_id ON incidents(source_id)")
        .await?;

    // Create boundary indexes for boundary filtering
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_state_fips ON incidents(state_fips)")
        .await?;
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_county_geoid ON incidents(county_geoid)")
        .await?;
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_place_geoid ON incidents(place_geoid)")
        .await?;
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_tract_geoid ON incidents(tract_geoid)")
        .await?;
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_neighborhood_id ON incidents(neighborhood_id)")
        .await?;

    // Analyze for query planner
    sqlite.exec_raw("ANALYZE").await?;

    log::info!(
        "Sidebar SQLite database generated: {} ({total_count} rows)",
        db_path.display()
    );
    Ok(())
}

/// Builds the SQL query and parameters for a single batch of incidents.
///
/// Uses keyset pagination (`WHERE i.id > $1 ORDER BY i.id ASC LIMIT $N`)
/// for efficient, index-backed iteration over the entire table.
///
/// Includes boundary GEOIDs derived from the incident's attributed census
/// data. Boundary GEOIDs (`state_fips`, `county_geoid`, `place_geoid`,
/// `tract_geoid`, `neighborhood_id`) are computed in Rust via the
/// [`SpatialIndex`](crate::spatial::SpatialIndex) rather than from
/// pre-attributed `PostGIS` columns, eliminating the need for the expensive
/// `cargo ingest attribute` step.
fn build_batch_query(
    last_id: i64,
    batch_limit: i64,
    source_ids: &[i32],
) -> (String, Vec<DatabaseValue>) {
    let mut params: Vec<DatabaseValue> = Vec::new();
    let mut param_idx: usize = 1;

    // Always filter by id > last_id and only include geocoded points
    let mut where_clause = format!("i.id > ${param_idx} AND i.has_coordinates = TRUE");
    params.push(DatabaseValue::Int64(last_id));
    param_idx += 1;

    // Optional source filter
    if !source_ids.is_empty() {
        let placeholders: Vec<String> = source_ids
            .iter()
            .map(|_| {
                let p = format!("${param_idx}");
                param_idx += 1;
                p
            })
            .collect();
        write!(
            where_clause,
            " AND i.source_id IN ({})",
            placeholders.join(", ")
        )
        .unwrap();
        for &sid in source_ids {
            params.push(DatabaseValue::Int32(sid));
        }
    }

    let limit_placeholder = format!("${param_idx}");
    params.push(DatabaseValue::Int64(batch_limit));

    let query = format!(
        "SELECT i.id, i.source_id, cs.name as source_name,
                i.source_incident_id,
                c.name as subcategory, c.severity,
                pc.name as category,
                i.occurred_at, i.description, i.block_address,
                i.city, i.state, i.arrest_made, i.domestic,
                i.location_type,
                ST_X(i.location::geometry) as longitude,
                ST_Y(i.location::geometry) as latitude
         FROM crime_incidents i
         JOIN crime_categories c ON i.category_id = c.id
         LEFT JOIN crime_categories pc ON c.parent_id = pc.id
         JOIN crime_sources cs ON i.source_id = cs.id
         WHERE {where_clause}
         ORDER BY i.id ASC
         LIMIT {limit_placeholder}"
    );

    (query, params)
}

/// Generates a `DuckDB` database with a pre-aggregated `count_summary` table
/// for fast count queries.
///
/// Creates `counts.duckdb` with:
/// - A raw `incidents` table populated from `PostGIS` via keyset pagination
/// - A `count_summary` table aggregated by spatial cell, subcategory, severity,
///   arrest status, and day
///
/// At runtime, count queries become a simple `SUM(cnt)` over the summary table
/// filtered by cell coordinates, completing in under 10ms for any bounding box.
///
/// # Errors
///
/// Returns an error if the `PostGIS` export, `DuckDB` creation, or
/// aggregation fails.
async fn generate_count_db(
    db: &dyn Database,
    args: &GenerateArgs,
    source_ids: &[i32],
    dir: &Path,
    geo_index: &crate::spatial::SpatialIndex,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = dir.join("counts.duckdb");

    // Remove any existing file so we start fresh
    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
    }
    // DuckDB may also create a .wal file
    let wal_path = dir.join("counts.duckdb.wal");
    if wal_path.exists() {
        std::fs::remove_file(&wal_path)?;
    }

    log::info!("Creating DuckDB count database...");

    let duck = duckdb::Connection::open(&db_path)?;

    // Create raw incidents table for aggregation
    duck.execute_batch(
        "CREATE TABLE incidents (
            id BIGINT PRIMARY KEY,
            source_id INTEGER NOT NULL,
            subcategory VARCHAR NOT NULL,
            severity INTEGER NOT NULL,
            longitude DOUBLE NOT NULL,
            latitude DOUBLE NOT NULL,
            occurred_at VARCHAR,
            arrest_made INTEGER,
            category VARCHAR NOT NULL,
            state_fips VARCHAR,
            county_geoid VARCHAR,
            place_geoid VARCHAR,
            tract_geoid VARCHAR,
            neighborhood_id VARCHAR
        )",
    )?;

    // Drop the connection before the async loop; we'll reopen per batch
    drop(duck);

    let total_count =
        populate_duckdb_incidents(db, args, source_ids, &db_path, geo_index, progress).await?;

    // Reopen for aggregation
    let duck = duckdb::Connection::open(&db_path)?;

    // Create pre-aggregated count summary table
    log::info!("Creating count_summary aggregation table...");
    duck.execute_batch(
        "CREATE TABLE count_summary AS
         SELECT
             CAST(FLOOR(longitude * 1000) AS INTEGER) AS cell_lng,
             CAST(FLOOR(latitude * 1000) AS INTEGER) AS cell_lat,
             source_id,
             subcategory,
             category,
             severity,
             CASE WHEN arrest_made = 1 THEN 1
                  WHEN arrest_made = 0 THEN 0
                  ELSE 2 END AS arrest,
             SUBSTRING(occurred_at, 1, 10) AS day,
             state_fips,
             county_geoid,
             place_geoid,
             tract_geoid,
             neighborhood_id,
             COUNT(*) AS cnt,
             SUM(longitude) AS sum_lng,
             SUM(latitude) AS sum_lat
         FROM incidents
         GROUP BY ALL
         ORDER BY cell_lng, cell_lat",
    )?;

    // Drop the raw incidents table to save space
    duck.execute_batch("DROP TABLE incidents")?;

    // Create indexes on the summary table for fast filtering
    log::info!("Creating count_summary indexes...");
    duck.execute_batch(
        "CREATE INDEX idx_count_summary_cells ON count_summary (cell_lng, cell_lat)",
    )?;

    log::info!(
        "DuckDB count database generated: {} ({total_count} rows aggregated)",
        db_path.display()
    );
    Ok(())
}

/// H3 resolutions to pre-compute during generation.
///
/// These cover zoom levels 8-18 per the `config/hexbins.json` mapping.
/// Resolution 4 (~22km edge) through 9 (~200m edge).
const H3_RESOLUTIONS: &[u8] = &[4, 5, 6, 7, 8, 9];

/// Batch size for H3 generation (larger than the default for throughput).
const H3_BATCH_SIZE: i64 = 50_000;

/// Generates a `DuckDB` database with pre-aggregated H3 hexbin counts.
///
/// Creates `h3.duckdb` with an `h3_counts` table indexed by H3 cell,
/// resolution, category, severity, arrest status, and day. Uses a staging
/// table approach for performance: incidents are bulk-inserted with
/// pre-computed H3 cell indices as extra columns, then a single SQL
/// aggregation produces the final table.
///
/// # Errors
///
/// Returns an error if the `PostGIS` export, `DuckDB` creation, or
/// aggregation fails.
#[allow(clippy::too_many_lines)]
async fn generate_h3_db(
    db: &dyn Database,
    args: &GenerateArgs,
    source_ids: &[i32],
    dir: &Path,
    geo_index: &crate::spatial::SpatialIndex,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<(), Box<dyn std::error::Error>> {
    use h3o::{LatLng, Resolution};

    let db_path = dir.join("h3.duckdb");

    // Remove any existing file so we start fresh
    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
    }
    let wal_path = dir.join("h3.duckdb.wal");
    if wal_path.exists() {
        std::fs::remove_file(&wal_path)?;
    }

    log::info!("Creating H3 hexbin DuckDB database...");

    // Pre-resolve H3 Resolution objects (avoids repeated try_from in hot loop)
    let resolutions: Vec<Resolution> = H3_RESOLUTIONS
        .iter()
        .filter_map(|&r| Resolution::try_from(r).ok())
        .collect();

    {
        let duck = duckdb::Connection::open(&db_path)?;

        // Create staging table: one row per incident with H3 indices as columns.
        // This avoids string cloning per resolution and lets DuckDB aggregate.
        duck.execute_batch(
            "CREATE TABLE h3_staging (
                source_id INTEGER NOT NULL,
                category VARCHAR NOT NULL,
                subcategory VARCHAR NOT NULL,
                severity TINYINT NOT NULL,
                arrest TINYINT NOT NULL,
                day VARCHAR NOT NULL,
                lng DOUBLE NOT NULL,
                lat DOUBLE NOT NULL,
                h3_r4 BIGINT NOT NULL,
                h3_r5 BIGINT NOT NULL,
                h3_r6 BIGINT NOT NULL,
                h3_r7 BIGINT NOT NULL,
                h3_r8 BIGINT NOT NULL,
                h3_r9 BIGINT NOT NULL,
                state_fips VARCHAR,
                county_geoid VARCHAR,
                place_geoid VARCHAR,
                tract_geoid VARCHAR,
                neighborhood_id VARCHAR
            )",
        )?;
    }

    // Populate staging table from PostGIS
    let mut last_id: i64 = 0;
    let mut total_count: u64 = 0;
    let mut remaining = args.limit;

    loop {
        #[allow(clippy::cast_sign_loss)]
        let batch_limit = match remaining {
            Some(0) => break,
            Some(r) => i64::try_from(r.min(H3_BATCH_SIZE as u64))?,
            None => H3_BATCH_SIZE,
        };

        let (query, params) = build_batch_query(last_id, batch_limit, source_ids);
        let rows = db.query_raw_params(&query, &params).await?;

        if rows.is_empty() {
            break;
        }

        #[allow(clippy::cast_possible_truncation)]
        let batch_len = rows.len() as u64;

        // Open DuckDB per batch (avoids !Send across .await)
        {
            let duck = duckdb::Connection::open(&db_path)?;
            duck.execute_batch("BEGIN TRANSACTION")?;

            let mut stmt = duck.prepare(
                "INSERT INTO h3_staging (source_id, category, subcategory, severity, arrest, day, lng, lat,
                    h3_r4, h3_r5, h3_r6, h3_r7, h3_r8, h3_r9,
                    state_fips, county_geoid, place_geoid, tract_geoid, neighborhood_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;

            for row in &rows {
                let id: i64 = row.to_value("id").unwrap_or(0);
                let source_id: i32 = row.to_value("source_id").unwrap_or(0);
                let lng: f64 = row.to_value("longitude").unwrap_or(0.0);
                let lat: f64 = row.to_value("latitude").unwrap_or(0.0);
                let category: String = row.to_value("category").unwrap_or_default();
                let subcategory: String = row.to_value("subcategory").unwrap_or_default();
                let severity: i32 = row.to_value("severity").unwrap_or(1);
                let arrest_made: Option<bool> = row.to_value("arrest_made").unwrap_or(None);
                let occurred_at_naive: Option<chrono::NaiveDateTime> =
                    row.to_value("occurred_at").unwrap_or(None);
                let day = occurred_at_naive
                    .map(|naive| naive.format("%Y-%m-%d").to_string())
                    .unwrap_or_default();

                let arrest_int: i32 = match arrest_made {
                    Some(true) => 1,
                    Some(false) => 0,
                    None => 2,
                };

                // Boundary GEOIDs — computed via Rust spatial index
                let tract_geoid = geo_index.lookup_tract(lng, lat).map(str::to_owned);
                let state_fips = tract_geoid
                    .as_deref()
                    .and_then(crate::spatial::SpatialIndex::derive_state_fips)
                    .map(str::to_owned);
                let county_geoid = tract_geoid
                    .as_deref()
                    .and_then(crate::spatial::SpatialIndex::derive_county_geoid)
                    .map(str::to_owned);
                let place_geoid = geo_index.lookup_place(lng, lat).map(str::to_owned);
                let neighborhood_id = tract_geoid
                    .as_deref()
                    .and_then(|g| geo_index.lookup_neighborhood(g))
                    .map(str::to_owned);

                let Ok(coord) = LatLng::new(lat, lng) else {
                    last_id = id;
                    continue;
                };

                // Compute all 6 H3 cell indices (nanoseconds each)
                let h3_cells: Vec<i64> = resolutions
                    .iter()
                    .map(|&res| {
                        #[allow(clippy::cast_possible_wrap)]
                        let idx = u64::from(coord.to_cell(res)) as i64;
                        idx
                    })
                    .collect();

                stmt.execute(duckdb::params![
                    source_id,
                    category,
                    subcategory,
                    severity,
                    arrest_int,
                    day,
                    lng,
                    lat,
                    h3_cells[0],
                    h3_cells[1],
                    h3_cells[2],
                    h3_cells[3],
                    h3_cells[4],
                    h3_cells[5],
                    state_fips,
                    county_geoid,
                    place_geoid,
                    tract_geoid,
                    neighborhood_id,
                ])?;

                last_id = id;
            }

            duck.execute_batch("COMMIT")?;
        }

        total_count += batch_len;
        if let Some(ref mut r) = remaining {
            *r = r.saturating_sub(batch_len);
        }

        progress.inc(batch_len);
        log::info!("Loaded {total_count} incidents into H3 staging table...");

        #[allow(clippy::cast_sign_loss)]
        let batch_limit_u64 = batch_limit as u64;
        if batch_len < batch_limit_u64 {
            break;
        }
    }

    // Aggregate staging table into final h3_counts using UNION ALL
    // across the 6 resolution columns. One SQL statement, DuckDB handles it
    // in a single vectorized pass.
    let duck = duckdb::Connection::open(&db_path)?;

    log::info!("Aggregating H3 counts from staging table...");
    duck.execute_batch(
        "CREATE TABLE h3_counts AS
         WITH unpivoted AS (
             SELECT h3_r4 AS h3_index, 4 AS resolution, source_id, category, subcategory, severity, arrest, day, lng, lat, state_fips, county_geoid, place_geoid, tract_geoid, neighborhood_id FROM h3_staging
             UNION ALL
             SELECT h3_r5, 5, source_id, category, subcategory, severity, arrest, day, lng, lat, state_fips, county_geoid, place_geoid, tract_geoid, neighborhood_id FROM h3_staging
             UNION ALL
             SELECT h3_r6, 6, source_id, category, subcategory, severity, arrest, day, lng, lat, state_fips, county_geoid, place_geoid, tract_geoid, neighborhood_id FROM h3_staging
             UNION ALL
             SELECT h3_r7, 7, source_id, category, subcategory, severity, arrest, day, lng, lat, state_fips, county_geoid, place_geoid, tract_geoid, neighborhood_id FROM h3_staging
             UNION ALL
             SELECT h3_r8, 8, source_id, category, subcategory, severity, arrest, day, lng, lat, state_fips, county_geoid, place_geoid, tract_geoid, neighborhood_id FROM h3_staging
             UNION ALL
             SELECT h3_r9, 9, source_id, category, subcategory, severity, arrest, day, lng, lat, state_fips, county_geoid, place_geoid, tract_geoid, neighborhood_id FROM h3_staging
         )
         SELECT
             CAST(h3_index AS UBIGINT) AS h3_index,
             CAST(resolution AS TINYINT) AS resolution,
             source_id,
             category,
             subcategory,
             CAST(severity AS TINYINT) AS severity,
             CAST(arrest AS TINYINT) AS arrest,
             day,
             state_fips,
             county_geoid,
             place_geoid,
             tract_geoid,
             neighborhood_id,
             CAST(COUNT(*) AS INTEGER) AS cnt,
             SUM(lng) AS sum_lng,
             SUM(lat) AS sum_lat
         FROM unpivoted
         GROUP BY h3_index, resolution, source_id, category, subcategory, severity, arrest, day, state_fips, county_geoid, place_geoid, tract_geoid, neighborhood_id
         ORDER BY resolution, h3_index",
    )?;

    // Drop staging table to reclaim space
    duck.execute_batch("DROP TABLE h3_staging")?;

    // Create indexes for fast viewport queries
    log::info!("Creating H3 indexes...");
    duck.execute_batch("CREATE INDEX idx_h3_counts_res_cell ON h3_counts (resolution, h3_index)")?;

    // Pre-compute hex boundary vertices for every distinct H3 cell.
    // This avoids per-request trigonometric computation at runtime.
    log::info!("Pre-computing H3 boundary vertices...");
    duck.execute_batch(
        "CREATE TABLE h3_boundaries (
             h3_index UBIGINT PRIMARY KEY,
             v0_lng DOUBLE NOT NULL, v0_lat DOUBLE NOT NULL,
             v1_lng DOUBLE NOT NULL, v1_lat DOUBLE NOT NULL,
             v2_lng DOUBLE NOT NULL, v2_lat DOUBLE NOT NULL,
             v3_lng DOUBLE NOT NULL, v3_lat DOUBLE NOT NULL,
             v4_lng DOUBLE NOT NULL, v4_lat DOUBLE NOT NULL,
             v5_lng DOUBLE NOT NULL, v5_lat DOUBLE NOT NULL
         )",
    )?;

    {
        let mut boundary_stmt = duck
            .prepare("INSERT INTO h3_boundaries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;

        let mut distinct_stmt =
            duck.prepare("SELECT DISTINCT CAST(h3_index AS BIGINT) FROM h3_counts")?;
        let mut rows = distinct_stmt.query([])?;

        let mut boundary_count: u64 = 0;
        while let Some(row) = rows.next()? {
            let h3_raw: i64 = row.get(0)?;
            #[allow(clippy::cast_sign_loss)]
            let h3_u64 = h3_raw as u64;
            let Some(cell) = h3o::CellIndex::try_from(h3_u64).ok() else {
                continue;
            };

            let boundary = cell.boundary();
            let verts: Vec<_> = boundary.iter().collect();

            // Hexagons have 6 vertices; pentagons have 5 (extremely rare).
            // Pad with the last vertex if fewer than 6.
            let v = |i: usize| -> (f64, f64) {
                if i < verts.len() {
                    (verts[i].lng(), verts[i].lat())
                } else {
                    let last = &verts[verts.len() - 1];
                    (last.lng(), last.lat())
                }
            };

            let (v0_lng, v0_lat) = v(0);
            let (v1_lng, v1_lat) = v(1);
            let (v2_lng, v2_lat) = v(2);
            let (v3_lng, v3_lat) = v(3);
            let (v4_lng, v4_lat) = v(4);
            let (v5_lng, v5_lat) = v(5);

            #[allow(clippy::cast_possible_wrap)]
            boundary_stmt.execute(duckdb::params![
                h3_raw, v0_lng, v0_lat, v1_lng, v1_lat, v2_lng, v2_lat, v3_lng, v3_lat, v4_lng,
                v4_lat, v5_lng, v5_lat,
            ])?;

            boundary_count += 1;
        }

        log::info!("Pre-computed boundaries for {boundary_count} distinct H3 cells");
    }

    log::info!(
        "H3 DuckDB database generated: {} ({total_count} incidents indexed)",
        db_path.display()
    );
    Ok(())
}

/// Populates the `DuckDB` incidents table from `PostGIS` using keyset
/// pagination.
///
/// Opens and closes the `DuckDB` connection per batch to avoid holding a
/// non-`Send` reference across `.await` points.
///
/// Returns the total number of rows inserted.
///
/// # Errors
///
/// Returns an error if the `PostGIS` query or `DuckDB` insert fails.
async fn populate_duckdb_incidents(
    db: &dyn Database,
    args: &GenerateArgs,
    source_ids: &[i32],
    duck_path: &Path,
    geo_index: &crate::spatial::SpatialIndex,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut last_id: i64 = 0;
    let mut total_count: u64 = 0;
    let mut remaining = args.limit;

    loop {
        #[allow(clippy::cast_sign_loss)]
        let batch_limit = match remaining {
            Some(0) => break,
            Some(r) => i64::try_from(r.min(BATCH_SIZE as u64))?,
            None => BATCH_SIZE,
        };

        let (query, params) = build_batch_query(last_id, batch_limit, source_ids);
        let rows = db.query_raw_params(&query, &params).await?;

        if rows.is_empty() {
            break;
        }

        #[allow(clippy::cast_possible_truncation)]
        let batch_len = rows.len() as u64;

        // Open DuckDB connection for this batch only (avoids !Send across await)
        {
            let duck = duckdb::Connection::open(duck_path)?;
            duck.execute_batch("BEGIN TRANSACTION")?;

            let mut stmt = duck.prepare(
                "INSERT INTO incidents (id, source_id, subcategory, severity, longitude, latitude,
                    occurred_at, arrest_made, category,
                    state_fips, county_geoid, place_geoid, tract_geoid, neighborhood_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;

            for row in &rows {
                last_id = insert_duckdb_row(&mut stmt, row, geo_index)?;
            }

            duck.execute_batch("COMMIT")?;
        }

        total_count += batch_len;
        if let Some(ref mut r) = remaining {
            *r = r.saturating_sub(batch_len);
        }

        progress.inc(batch_len);
        log::info!("Inserted {total_count} rows into DuckDB...");

        #[allow(clippy::cast_sign_loss)]
        let batch_limit_u64 = batch_limit as u64;
        if batch_len < batch_limit_u64 {
            break;
        }
    }

    Ok(total_count)
}

/// Inserts a single `PostGIS` incident row into the `DuckDB` incidents table.
///
/// Returns the row's primary key ID for keyset pagination tracking.
///
/// # Errors
///
/// Returns an error if the row extraction or `DuckDB` insert fails.
fn insert_duckdb_row(
    stmt: &mut duckdb::Statement<'_>,
    row: &switchy_database::Row,
    geo_index: &crate::spatial::SpatialIndex,
) -> Result<i64, Box<dyn std::error::Error>> {
    let id: i64 = row.to_value("id").unwrap_or(0);
    let source_id: i32 = row.to_value("source_id").unwrap_or(0);
    let subcategory: String = row.to_value("subcategory").unwrap_or_default();
    let category: String = row.to_value("category").unwrap_or_default();
    let severity: i32 = row.to_value("severity").unwrap_or(1);
    let lng: f64 = row.to_value("longitude").unwrap_or(0.0);
    let lat: f64 = row.to_value("latitude").unwrap_or(0.0);
    let arrest_made: Option<bool> = row.to_value("arrest_made").unwrap_or(None);

    let occurred_at_naive: Option<chrono::NaiveDateTime> =
        row.to_value("occurred_at").unwrap_or(None);
    let occurred_at_str: Option<String> =
        occurred_at_naive.map(|naive| naive.format("%Y-%m-%d %H:%M:%S").to_string());

    let arrest_int: Option<i32> = arrest_made.map(i32::from);

    // Boundary GEOIDs — computed via Rust spatial index
    let tract_geoid = geo_index.lookup_tract(lng, lat).map(str::to_owned);
    let state_fips = tract_geoid
        .as_deref()
        .and_then(crate::spatial::SpatialIndex::derive_state_fips)
        .map(str::to_owned);
    let county_geoid = tract_geoid
        .as_deref()
        .and_then(crate::spatial::SpatialIndex::derive_county_geoid)
        .map(str::to_owned);
    let place_geoid = geo_index.lookup_place(lng, lat).map(str::to_owned);
    let neighborhood_id = tract_geoid
        .as_deref()
        .and_then(|g| geo_index.lookup_neighborhood(g))
        .map(str::to_owned);

    stmt.execute(duckdb::params![
        id,
        source_id,
        subcategory,
        severity,
        lng,
        lat,
        occurred_at_str,
        arrest_int,
        category,
        state_fips,
        county_geoid,
        place_geoid,
        tract_geoid,
        neighborhood_id,
    ])?;

    Ok(id)
}

/// Generates a `metadata.json` file containing server startup context that
/// would otherwise require `PostGIS` at runtime.
///
/// This includes:
/// - `cities`: distinct `(city, state)` pairs from the dataset
/// - `minDate` / `maxDate`: the earliest and latest `occurred_at` timestamps
///
/// The server loads this file at boot to populate the AI agent context
/// without needing a live `PostGIS` connection.
///
/// # Errors
///
/// Returns an error if the database query or file write fails.
async fn generate_metadata(
    db: &dyn Database,
    source_ids: &[i32],
    dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Querying available cities...");

    let source_filter = if source_ids.is_empty() {
        String::new()
    } else {
        let ids: Vec<String> = source_ids
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        format!(" AND source_id IN ({})", ids.join(", "))
    };

    let cities_query = format!(
        "SELECT DISTINCT city, state FROM crime_incidents
         WHERE city IS NOT NULL AND city != ''{source_filter}
         ORDER BY state, city"
    );
    let city_rows = db.query_raw_params(&cities_query, &[]).await?;

    let cities: Vec<serde_json::Value> = city_rows
        .iter()
        .map(|row| {
            let city: String = row.to_value("city").unwrap_or_default();
            let state: String = row.to_value("state").unwrap_or_default();
            serde_json::json!([city, state])
        })
        .collect();

    log::info!("Found {} distinct cities", cities.len());

    log::info!("Querying date range...");
    let date_query = format!(
        "SELECT MIN(occurred_at)::text as min_date, MAX(occurred_at)::text as max_date
         FROM crime_incidents
         WHERE has_coordinates = TRUE{source_filter}"
    );
    let date_rows = db.query_raw_params(&date_query, &[]).await?;

    let min_date: Option<String> = date_rows
        .first()
        .and_then(|r| r.to_value("min_date").unwrap_or(None));
    let max_date: Option<String> = date_rows
        .first()
        .and_then(|r| r.to_value("max_date").unwrap_or(None));

    // Query source metadata for the /api/sources endpoint
    log::info!("Querying source metadata...");
    let sources_query = if source_ids.is_empty() {
        "SELECT id, name, source_type, record_count, coverage_area, portal_url
         FROM crime_sources ORDER BY name"
            .to_string()
    } else {
        let ids: Vec<String> = source_ids
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        format!(
            "SELECT id, name, source_type, record_count, coverage_area, portal_url
             FROM crime_sources WHERE id IN ({}) ORDER BY name",
            ids.join(", ")
        )
    };
    let source_rows = db.query_raw_params(&sources_query, &[]).await?;

    let sources: Vec<serde_json::Value> = source_rows
        .iter()
        .map(|row| {
            let id: i32 = row.to_value("id").unwrap_or(0);
            let name: String = row.to_value("name").unwrap_or_default();
            let source_type: String = row.to_value("source_type").unwrap_or_default();
            let record_count: i64 = row.to_value("record_count").unwrap_or(0);
            let coverage_area: String = row.to_value("coverage_area").unwrap_or_default();
            let portal_url: Option<String> = row.to_value("portal_url").unwrap_or(None);
            serde_json::json!({
                "id": id,
                "name": name,
                "sourceType": source_type,
                "recordCount": record_count,
                "coverageArea": coverage_area,
                "portalUrl": portal_url,
            })
        })
        .collect();

    log::info!("Found {} sources", sources.len());

    let metadata = serde_json::json!({
        "cities": cities,
        "minDate": min_date,
        "maxDate": max_date,
        "sources": sources,
    });

    let path = dir.join("metadata.json");
    let tmp_path = dir.join("metadata.json.tmp");
    let contents = serde_json::to_string_pretty(&metadata)?;
    std::fs::write(&tmp_path, contents)?;
    std::fs::rename(&tmp_path, &path)?;

    log::info!("Server metadata generated: {}", path.display());
    Ok(())
}

// ============================================================
// Analytics DuckDB generation
// ============================================================

/// Generates a `DuckDB` database for AI analytics tool queries at runtime.
///
/// Creates `analytics.duckdb` with:
/// - `incidents` table: denormalized incident rows with pre-resolved
///   city, state, category, subcategory text columns
/// - `census_tracts` table: tract metadata for `rank_areas` tool
/// - `neighborhoods` / `tract_neighborhoods` tables: neighborhood mapping
/// - `census_places` table: place metadata for `search_locations` tool
/// - `crime_categories` table: category ID-to-name mapping
///
/// This replaces all runtime `PostGIS` queries from the AI analytics tools.
///
/// # Errors
///
/// Returns an error if the `PostGIS` export or `DuckDB` creation fails.
#[allow(clippy::too_many_lines)]
async fn generate_analytics_db(
    db: &dyn Database,
    args: &GenerateArgs,
    source_ids: &[i32],
    dir: &Path,
    geo_index: &crate::spatial::SpatialIndex,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = dir.join("analytics.duckdb");

    // Remove existing files
    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
    }
    let wal_path = dir.join("analytics.duckdb.wal");
    if wal_path.exists() {
        std::fs::remove_file(&wal_path)?;
    }

    log::info!("Creating analytics DuckDB database...");

    {
        let duck = duckdb::Connection::open(&db_path)?;

        // Create denormalized incidents table
        duck.execute_batch(
            "CREATE TABLE incidents (
                occurred_at TIMESTAMP,
                city VARCHAR,
                state VARCHAR,
                category VARCHAR NOT NULL,
                subcategory VARCHAR NOT NULL,
                severity INTEGER NOT NULL,
                arrest_made BOOLEAN,
                parent_category_id INTEGER,
                category_id INTEGER,
                source_id INTEGER NOT NULL,
                census_tract_geoid VARCHAR,
                census_place_geoid VARCHAR,
                neighborhood_id VARCHAR
            )",
        )?;
    }

    // Populate incidents from PostGIS using keyset pagination
    let mut last_id: i64 = 0;
    let mut total_count: u64 = 0;
    let mut remaining = args.limit;

    loop {
        #[allow(clippy::cast_sign_loss)]
        let batch_limit = match remaining {
            Some(0) => break,
            Some(r) => i64::try_from(r.min(BATCH_SIZE as u64))?,
            None => BATCH_SIZE,
        };

        let (query, params) = build_batch_query(last_id, batch_limit, source_ids);
        let rows = db.query_raw_params(&query, &params).await?;

        if rows.is_empty() {
            break;
        }

        #[allow(clippy::cast_possible_truncation)]
        let batch_len = rows.len() as u64;

        {
            let duck = duckdb::Connection::open(&db_path)?;
            duck.execute_batch("BEGIN TRANSACTION")?;

            let mut stmt = duck.prepare(
                "INSERT INTO incidents (occurred_at, city, state, category, subcategory,
                    severity, arrest_made, parent_category_id, category_id, source_id,
                    census_tract_geoid, census_place_geoid, neighborhood_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;

            for row in &rows {
                let id: i64 = row.to_value("id").unwrap_or(0);
                let source_id: i32 = row.to_value("source_id").unwrap_or(0);
                let subcategory: String = row.to_value("subcategory").unwrap_or_default();
                let category: String = row.to_value("category").unwrap_or_default();
                let severity: i32 = row.to_value("severity").unwrap_or(1);
                let lng: f64 = row.to_value("longitude").unwrap_or(0.0);
                let lat: f64 = row.to_value("latitude").unwrap_or(0.0);
                let arrest_made: Option<bool> = row.to_value("arrest_made").unwrap_or(None);
                let city: String = row.to_value("city").unwrap_or_default();
                let state: String = row.to_value("state").unwrap_or_default();

                let occurred_at_naive: Option<chrono::NaiveDateTime> =
                    row.to_value("occurred_at").unwrap_or(None);
                let occurred_at_str: Option<String> =
                    occurred_at_naive.map(|naive| naive.format("%Y-%m-%d %H:%M:%S").to_string());

                // Look up category IDs from the PostGIS batch query results
                // The batch query JOINs crime_categories, but we need the IDs for
                // the parent_category_id subquery pattern. We'll store them for
                // reference table lookups. For now, we can reconstruct from the
                // category/subcategory names.
                let parent_category_id: Option<i32> = None;
                let category_id: Option<i32> = None;

                // Boundary GEOIDs
                let tract_geoid = geo_index.lookup_tract(lng, lat).map(str::to_owned);
                let place_geoid = geo_index.lookup_place(lng, lat).map(str::to_owned);
                let neighborhood_id = tract_geoid
                    .as_deref()
                    .and_then(|g| geo_index.lookup_neighborhood(g))
                    .map(str::to_owned);

                stmt.execute(duckdb::params![
                    occurred_at_str,
                    city,
                    state,
                    category,
                    subcategory,
                    severity,
                    arrest_made,
                    parent_category_id,
                    category_id,
                    source_id,
                    tract_geoid,
                    place_geoid,
                    neighborhood_id,
                ])?;

                last_id = id;
            }

            duck.execute_batch("COMMIT")?;
        }

        total_count += batch_len;
        if let Some(ref mut r) = remaining {
            *r = r.saturating_sub(batch_len);
        }

        progress.inc(batch_len);
        log::info!("Inserted {total_count} rows into analytics DB...");

        #[allow(clippy::cast_sign_loss)]
        let batch_limit_u64 = batch_limit as u64;
        if batch_len < batch_limit_u64 {
            break;
        }
    }

    // Now populate reference tables
    let duck = duckdb::Connection::open(&db_path)?;

    // Create indexes on the incidents table
    log::info!("Creating analytics indexes...");
    duck.execute_batch(
        "CREATE INDEX idx_analytics_city ON incidents (city);
         CREATE INDEX idx_analytics_state ON incidents (state);
         CREATE INDEX idx_analytics_occurred_at ON incidents (occurred_at);
         CREATE INDEX idx_analytics_category ON incidents (category);
         CREATE INDEX idx_analytics_place_geoid ON incidents (census_place_geoid);
         CREATE INDEX idx_analytics_tract_geoid ON incidents (census_tract_geoid);
         CREATE INDEX idx_analytics_neighborhood_id ON incidents (neighborhood_id)",
    )?;

    // ── Census tracts reference table ──
    log::info!("Populating census_tracts reference table...");
    let tract_rows = db
        .query_raw_params(
            "SELECT geoid, name, state_abbr, county_name, population, land_area_sq_mi
             FROM census_tracts ORDER BY geoid",
            &[],
        )
        .await?;

    duck.execute_batch(
        "CREATE TABLE census_tracts (
            geoid VARCHAR PRIMARY KEY,
            name VARCHAR,
            state_abbr VARCHAR,
            county_name VARCHAR,
            population INTEGER,
            land_area_sq_mi DOUBLE
        )",
    )?;

    {
        let mut stmt = duck.prepare(
            "INSERT INTO census_tracts (geoid, name, state_abbr, county_name, population, land_area_sq_mi)
             VALUES (?, ?, ?, ?, ?, ?)",
        )?;

        for row in &tract_rows {
            let geoid: String = row.to_value("geoid").unwrap_or_default();
            let name: String = row.to_value("name").unwrap_or_default();
            let state_abbr: Option<String> = row.to_value("state_abbr").unwrap_or(None);
            let county_name: Option<String> = row.to_value("county_name").unwrap_or(None);
            let population: Option<i32> = row.to_value("population").unwrap_or(None);
            let land_area: Option<f64> = row.to_value("land_area_sq_mi").unwrap_or(None);
            stmt.execute(duckdb::params![
                geoid,
                name,
                state_abbr,
                county_name,
                population,
                land_area
            ])?;
        }
    }
    log::info!("Inserted {} census tracts", tract_rows.len());

    // ── Neighborhoods reference table ──
    log::info!("Populating neighborhoods reference table...");
    let nbhd_rows = db
        .query_raw_params(
            "SELECT id, name, city, state FROM neighborhoods ORDER BY id",
            &[],
        )
        .await?;

    duck.execute_batch(
        "CREATE TABLE neighborhoods (
            id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL
        )",
    )?;

    {
        let mut stmt = duck.prepare("INSERT INTO neighborhoods (id, name) VALUES (?, ?)")?;

        for row in &nbhd_rows {
            let id: i32 = row.to_value("id").unwrap_or(0);
            let name: String = row.to_value("name").unwrap_or_default();
            let nbhd_id = format!("nbhd-{id}");
            stmt.execute(duckdb::params![nbhd_id, name])?;
        }
    }
    log::info!("Inserted {} neighborhoods", nbhd_rows.len());

    // ── Tract-neighborhood mapping table ──
    log::info!("Populating tract_neighborhoods reference table...");
    let tn_rows = db
        .query_raw_params(
            "SELECT geoid, neighborhood_id FROM tract_neighborhoods ORDER BY geoid",
            &[],
        )
        .await?;

    duck.execute_batch(
        "CREATE TABLE tract_neighborhoods (
            geoid VARCHAR NOT NULL,
            neighborhood_id VARCHAR NOT NULL
        )",
    )?;

    {
        let mut stmt =
            duck.prepare("INSERT INTO tract_neighborhoods (geoid, neighborhood_id) VALUES (?, ?)")?;

        for row in &tn_rows {
            let geoid: String = row.to_value("geoid").unwrap_or_default();
            let nbhd_id: i32 = row.to_value("neighborhood_id").unwrap_or(0);
            let nbhd_id_str = format!("nbhd-{nbhd_id}");
            stmt.execute(duckdb::params![geoid, nbhd_id_str])?;
        }
    }
    log::info!("Inserted {} tract-neighborhood mappings", tn_rows.len());

    // ── Census places reference table ──
    log::info!("Populating census_places reference table...");
    let place_rows = db
        .query_raw_params(
            "SELECT geoid, name, full_name, state_abbr, place_type, population, land_area_sq_mi
             FROM census_places ORDER BY geoid",
            &[],
        )
        .await?;

    duck.execute_batch(
        "CREATE TABLE census_places (
            geoid VARCHAR PRIMARY KEY,
            name VARCHAR,
            full_name VARCHAR,
            state_abbr VARCHAR,
            place_type VARCHAR,
            population INTEGER,
            land_area_sq_mi DOUBLE
        )",
    )?;

    {
        let mut stmt = duck.prepare(
            "INSERT INTO census_places (geoid, name, full_name, state_abbr, place_type, population, land_area_sq_mi)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )?;

        for row in &place_rows {
            let geoid: String = row.to_value("geoid").unwrap_or_default();
            let name: String = row.to_value("name").unwrap_or_default();
            let full_name: String = row.to_value("full_name").unwrap_or_default();
            let state_abbr: Option<String> = row.to_value("state_abbr").unwrap_or(None);
            let place_type: String = row.to_value("place_type").unwrap_or_default();
            let population: Option<i32> = row.to_value("population").unwrap_or(None);
            let land_area: Option<f64> = row.to_value("land_area_sq_mi").unwrap_or(None);
            stmt.execute(duckdb::params![
                geoid, name, full_name, state_abbr, place_type, population, land_area
            ])?;
        }
    }
    log::info!("Inserted {} census places", place_rows.len());

    // ── Crime categories reference table ──
    log::info!("Populating crime_categories reference table...");
    let cat_rows = db
        .query_raw_params(
            "SELECT id, name, parent_id, severity FROM crime_categories ORDER BY id",
            &[],
        )
        .await?;

    duck.execute_batch(
        "CREATE TABLE crime_categories (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            parent_id INTEGER,
            severity INTEGER
        )",
    )?;

    {
        let mut stmt = duck.prepare(
            "INSERT INTO crime_categories (id, name, parent_id, severity) VALUES (?, ?, ?, ?)",
        )?;

        for row in &cat_rows {
            let id: i32 = row.to_value("id").unwrap_or(0);
            let name: String = row.to_value("name").unwrap_or_default();
            let parent_id: Option<i32> = row.to_value("parent_id").unwrap_or(None);
            let severity: Option<i32> = row.to_value("severity").unwrap_or(None);
            stmt.execute(duckdb::params![id, name, parent_id, severity])?;
        }
    }
    log::info!("Inserted {} crime categories", cat_rows.len());

    log::info!(
        "Analytics DuckDB database generated: {} ({total_count} incident rows + reference tables)",
        db_path.display()
    );
    Ok(())
}

// ============================================================
// Boundaries search SQLite generation
// ============================================================

/// Generates a `SQLite` database for boundary name lookups at runtime.
///
/// Creates `boundaries.db` with a single `boundaries` table containing
/// name/geoid metadata for all boundary types (states, counties, places,
/// tracts, neighborhoods). Used by `GET /api/boundaries/search` to
/// support type-ahead boundary filtering without `PostGIS`.
///
/// # Errors
///
/// Returns an error if the `PostGIS` query or `SQLite` write fails.
#[allow(clippy::too_many_lines)]
async fn generate_boundaries_db(
    db: &dyn Database,
    dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let db_path = dir.join("boundaries.db");

    // Remove existing file
    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
    }

    log::info!("Creating boundaries search SQLite database...");
    let sqlite = init_sqlite_rusqlite(Some(&db_path))?;

    sqlite
        .exec_raw(
            "CREATE TABLE boundaries (
                type TEXT NOT NULL,
                geoid TEXT NOT NULL,
                name TEXT NOT NULL,
                full_name TEXT,
                state_abbr TEXT,
                population INTEGER,
                PRIMARY KEY (type, geoid)
            )",
        )
        .await?;

    // States
    let rows = db
        .query_raw_params(
            "SELECT fips, name, abbr, population FROM census_states ORDER BY fips",
            &[],
        )
        .await?;
    let txn = sqlite.begin_transaction().await?;
    for row in &rows {
        let fips: String = row.to_value("fips").unwrap_or_default();
        let name: String = row.to_value("name").unwrap_or_default();
        let abbr: String = row.to_value("abbr").unwrap_or_default();
        let population: Option<i64> = row.to_value("population").unwrap_or(None);
        txn.exec_raw_params(
            "INSERT INTO boundaries (type, geoid, name, full_name, state_abbr, population)
             VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                DatabaseValue::String("state".to_string()),
                DatabaseValue::String(fips),
                DatabaseValue::String(name.clone()),
                DatabaseValue::String(name),
                DatabaseValue::String(abbr),
                population.map_or(DatabaseValue::Null, DatabaseValue::Int64),
            ],
        )
        .await?;
    }
    txn.commit().await?;
    log::info!("Inserted {} state boundaries", rows.len());

    // Counties
    let rows = db
        .query_raw_params(
            "SELECT geoid, name, full_name, state_abbr, population
             FROM census_counties ORDER BY geoid",
            &[],
        )
        .await?;
    let txn = sqlite.begin_transaction().await?;
    for row in &rows {
        let geoid: String = row.to_value("geoid").unwrap_or_default();
        let name: String = row.to_value("name").unwrap_or_default();
        let full_name: String = row.to_value("full_name").unwrap_or_default();
        let state_abbr: Option<String> = row.to_value("state_abbr").unwrap_or(None);
        let population: Option<i32> = row.to_value("population").unwrap_or(None);
        txn.exec_raw_params(
            "INSERT INTO boundaries (type, geoid, name, full_name, state_abbr, population)
             VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                DatabaseValue::String("county".to_string()),
                DatabaseValue::String(geoid),
                DatabaseValue::String(name),
                DatabaseValue::String(full_name),
                state_abbr.map_or(DatabaseValue::Null, DatabaseValue::String),
                population.map_or(DatabaseValue::Null, DatabaseValue::Int32),
            ],
        )
        .await?;
    }
    txn.commit().await?;
    log::info!("Inserted {} county boundaries", rows.len());

    // Places
    let rows = db
        .query_raw_params(
            "SELECT geoid, name, full_name, state_abbr, population
             FROM census_places ORDER BY geoid",
            &[],
        )
        .await?;
    let txn = sqlite.begin_transaction().await?;
    for row in &rows {
        let geoid: String = row.to_value("geoid").unwrap_or_default();
        let name: String = row.to_value("name").unwrap_or_default();
        let full_name: String = row.to_value("full_name").unwrap_or_default();
        let state_abbr: Option<String> = row.to_value("state_abbr").unwrap_or(None);
        let population: Option<i32> = row.to_value("population").unwrap_or(None);
        txn.exec_raw_params(
            "INSERT INTO boundaries (type, geoid, name, full_name, state_abbr, population)
             VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                DatabaseValue::String("place".to_string()),
                DatabaseValue::String(geoid),
                DatabaseValue::String(name),
                DatabaseValue::String(full_name),
                state_abbr.map_or(DatabaseValue::Null, DatabaseValue::String),
                population.map_or(DatabaseValue::Null, DatabaseValue::Int32),
            ],
        )
        .await?;
    }
    txn.commit().await?;
    log::info!("Inserted {} place boundaries", rows.len());

    // Tracts
    let rows = db
        .query_raw_params(
            "SELECT geoid, name, state_abbr, county_name, population
             FROM census_tracts ORDER BY geoid",
            &[],
        )
        .await?;
    let txn = sqlite.begin_transaction().await?;
    for row in &rows {
        let geoid: String = row.to_value("geoid").unwrap_or_default();
        let name: String = row.to_value("name").unwrap_or_default();
        let state_abbr: Option<String> = row.to_value("state_abbr").unwrap_or(None);
        let county_name: Option<String> = row.to_value("county_name").unwrap_or(None);
        let population: Option<i32> = row.to_value("population").unwrap_or(None);
        let full_name = match (&county_name, &state_abbr) {
            (Some(c), Some(s)) => format!("Tract {name}, {c}, {s}"),
            (Some(c), None) => format!("Tract {name}, {c}"),
            (None, Some(s)) => format!("Tract {name}, {s}"),
            (None, None) => format!("Tract {name}"),
        };
        txn.exec_raw_params(
            "INSERT INTO boundaries (type, geoid, name, full_name, state_abbr, population)
             VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                DatabaseValue::String("tract".to_string()),
                DatabaseValue::String(geoid),
                DatabaseValue::String(name),
                DatabaseValue::String(full_name),
                state_abbr.map_or(DatabaseValue::Null, DatabaseValue::String),
                population.map_or(DatabaseValue::Null, DatabaseValue::Int32),
            ],
        )
        .await?;
    }
    txn.commit().await?;
    log::info!("Inserted {} tract boundaries", rows.len());

    // Neighborhoods
    let rows = db
        .query_raw_params(
            "SELECT id, name, city, state FROM neighborhoods ORDER BY id",
            &[],
        )
        .await?;
    let txn = sqlite.begin_transaction().await?;
    for row in &rows {
        let id: i32 = row.to_value("id").unwrap_or(0);
        let name: String = row.to_value("name").unwrap_or_default();
        let city: String = row.to_value("city").unwrap_or_default();
        let state: String = row.to_value("state").unwrap_or_default();
        let geoid = format!("nbhd-{id}");
        let full_name = format!("{name}, {city}, {state}");
        txn.exec_raw_params(
            "INSERT INTO boundaries (type, geoid, name, full_name, state_abbr, population)
             VALUES ($1, $2, $3, $4, $5, $6)",
            &[
                DatabaseValue::String("neighborhood".to_string()),
                DatabaseValue::String(geoid),
                DatabaseValue::String(name),
                DatabaseValue::String(full_name),
                DatabaseValue::String(state),
                DatabaseValue::Null,
            ],
        )
        .await?;
    }
    txn.commit().await?;
    log::info!("Inserted {} neighborhood boundaries", rows.len());

    // Create search index
    sqlite
        .exec_raw("CREATE INDEX idx_boundaries_name ON boundaries(type, name COLLATE NOCASE)")
        .await?;

    sqlite.exec_raw("ANALYZE").await?;

    log::info!(
        "Boundaries search database generated: {}",
        db_path.display()
    );
    Ok(())
}

// ============================================================
// Boundary PMTiles generation
// ============================================================

/// Boundary layer names for tippecanoe's `--named-layer` parameter.
const BOUNDARY_LAYERS: &[(&str, &str)] = &[
    ("states", "states.geojsonseq"),
    ("counties", "counties.geojsonseq"),
    ("places", "places.geojsonseq"),
    ("tracts", "tracts.geojsonseq"),
    ("neighborhoods", "neighborhoods.geojsonseq"),
];

/// Generates `boundaries.pmtiles` containing administrative boundary
/// polygons from `PostGIS`.
///
/// Exports 5 `GeoJSONSeq` files (states, counties, places, tracts,
/// neighborhoods), then runs tippecanoe with multiple named layers
/// to produce a single `PMTiles` archive.
///
/// # Errors
///
/// Returns an error if any export or tippecanoe invocation fails.
async fn generate_boundaries_pmtiles(
    db: &dyn Database,
    dir: &Path,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Exporting boundary layers to GeoJSONSeq...");

    export_boundary_layer(db, dir, "states", progress).await?;
    export_boundary_layer(db, dir, "counties", progress).await?;
    export_boundary_layer(db, dir, "places", progress).await?;
    export_boundary_layer(db, dir, "tracts", progress).await?;
    export_boundary_layer(db, dir, "neighborhoods", progress).await?;

    log::info!("Running tippecanoe to generate boundaries PMTiles...");

    let output_path = dir.join("boundaries.pmtiles");
    let mut cmd = Command::new("tippecanoe");
    cmd.args([
        "-o",
        &output_path.to_string_lossy(),
        "--force",
        "--no-feature-limit",
        "--no-tile-size-limit",
        "--minimum-zoom=0",
        "--maximum-zoom=14",
        "--coalesce-densest-as-needed",
        "--detect-shared-borders",
    ]);

    // Add each layer as a named-layer with its GeoJSONSeq file
    for &(layer_name, filename) in BOUNDARY_LAYERS {
        let layer_path = dir.join(filename);
        if layer_path.exists() {
            let arg = format!(
                "--named-layer={layer_name}:{}",
                layer_path.to_string_lossy()
            );
            cmd.arg(arg);
        }
    }

    let status = cmd.status()?;
    if !status.success() {
        return Err("tippecanoe failed for boundaries".into());
    }

    // Clean up intermediate GeoJSONSeq files
    for &(_, filename) in BOUNDARY_LAYERS {
        let path = dir.join(filename);
        if path.exists()
            && let Err(e) = std::fs::remove_file(&path)
        {
            log::warn!("Failed to remove {}: {e}", path.display());
        }
    }

    log::info!("Boundaries PMTiles generated: {}", output_path.display());
    Ok(())
}

/// Exports a single boundary layer from `PostGIS` as `GeoJSONSeq`.
///
/// Each feature is a polygon/multipolygon with name/identifier properties.
#[allow(clippy::too_many_lines)]
async fn export_boundary_layer(
    db: &dyn Database,
    dir: &Path,
    layer: &str,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<(), Box<dyn std::error::Error>> {
    let filename = format!("{layer}.geojsonseq");
    let output_path = dir.join(&filename);
    let file = std::fs::File::create(&output_path)?;
    let mut writer = BufWriter::new(file);

    let query = match layer {
        "states" => {
            "SELECT fips, name, abbr, population,
                    land_area_sq_mi,
                    ST_AsGeoJSON(boundary::geometry) as geojson
             FROM census_states
             WHERE boundary IS NOT NULL
             ORDER BY fips"
        }
        "counties" => {
            "SELECT geoid, name, full_name, state_fips, state_abbr,
                    county_fips, population, land_area_sq_mi,
                    ST_AsGeoJSON(boundary::geometry) as geojson
             FROM census_counties
             WHERE boundary IS NOT NULL
             ORDER BY geoid"
        }
        "places" => {
            "SELECT geoid, name, full_name, state_fips, state_abbr,
                    place_type, population, land_area_sq_mi,
                    ST_AsGeoJSON(boundary::geometry) as geojson
             FROM census_places
             WHERE boundary IS NOT NULL
             ORDER BY geoid"
        }
        "tracts" => {
            "SELECT geoid, name, state_fips, county_fips, state_abbr,
                    county_name, population, land_area_sq_mi,
                    ST_AsGeoJSON(boundary::geometry) as geojson
             FROM census_tracts
             WHERE boundary IS NOT NULL
             ORDER BY geoid"
        }
        "neighborhoods" => {
            "SELECT id, name, city, state,
                    ST_AsGeoJSON(boundary::geometry) as geojson
             FROM neighborhoods
             WHERE boundary IS NOT NULL
             ORDER BY id"
        }
        _ => return Err(format!("Unknown boundary layer: {layer}").into()),
    };

    let rows = db.query_raw_params(query, &[]).await?;

    let mut count = 0u64;

    for row in &rows {
        let geojson_str: String = row.to_value("geojson").unwrap_or_default();
        if geojson_str.is_empty() {
            continue;
        }

        let geometry: serde_json::Value = serde_json::from_str(&geojson_str)?;

        let properties = match layer {
            "states" => {
                let fips: String = row.to_value("fips").unwrap_or_default();
                let name: String = row.to_value("name").unwrap_or_default();
                let abbr: String = row.to_value("abbr").unwrap_or_default();
                let population: Option<i64> = row.to_value("population").unwrap_or(None);
                serde_json::json!({
                    "name": name,
                    "abbr": abbr,
                    "fips": fips,
                    "population": population,
                })
            }
            "counties" => {
                let geoid: String = row.to_value("geoid").unwrap_or_default();
                let name: String = row.to_value("name").unwrap_or_default();
                let full_name: String = row.to_value("full_name").unwrap_or_default();
                let state_abbr: Option<String> = row.to_value("state_abbr").unwrap_or(None);
                let population: Option<i32> = row.to_value("population").unwrap_or(None);
                serde_json::json!({
                    "name": name,
                    "full_name": full_name,
                    "geoid": geoid,
                    "state": state_abbr,
                    "population": population,
                })
            }
            "places" => {
                let geoid: String = row.to_value("geoid").unwrap_or_default();
                let name: String = row.to_value("name").unwrap_or_default();
                let full_name: String = row.to_value("full_name").unwrap_or_default();
                let state_abbr: Option<String> = row.to_value("state_abbr").unwrap_or(None);
                let place_type: String = row.to_value("place_type").unwrap_or_default();
                let population: Option<i32> = row.to_value("population").unwrap_or(None);
                serde_json::json!({
                    "name": name,
                    "full_name": full_name,
                    "geoid": geoid,
                    "state": state_abbr,
                    "type": place_type,
                    "population": population,
                })
            }
            "tracts" => {
                let geoid: String = row.to_value("geoid").unwrap_or_default();
                let name: String = row.to_value("name").unwrap_or_default();
                let state_abbr: Option<String> = row.to_value("state_abbr").unwrap_or(None);
                let county_name: Option<String> = row.to_value("county_name").unwrap_or(None);
                let population: Option<i32> = row.to_value("population").unwrap_or(None);
                serde_json::json!({
                    "name": name,
                    "geoid": geoid,
                    "state": state_abbr,
                    "county": county_name,
                    "population": population,
                })
            }
            "neighborhoods" => {
                let id: i64 = row.to_value("id").unwrap_or(0);
                let name: String = row.to_value("name").unwrap_or_default();
                let city: String = row.to_value("city").unwrap_or_default();
                let state: String = row.to_value("state").unwrap_or_default();
                serde_json::json!({
                    "nbhd_id": format!("nbhd-{id}"),
                    "name": name,
                    "city": city,
                    "state": state,
                })
            }
            _ => serde_json::json!({}),
        };

        let feature = serde_json::json!({
            "type": "Feature",
            "geometry": geometry,
            "properties": properties,
        });

        serde_json::to_writer(&mut writer, &feature)?;
        writer.write_all(b"\n")?;
        count += 1;
    }

    writer.flush()?;
    progress.inc(count);
    log::info!("Exported {count} {layer} boundary features to {filename}");
    Ok(())
}

/// Exports all incidents from `PostGIS` as newline-delimited `GeoJSON`,
/// using keyset pagination and streaming writes to keep memory constant.
#[allow(clippy::too_many_lines)]
async fn export_geojsonseq(
    db: &dyn Database,
    output_path: &Path,
    limit: Option<u64>,
    source_ids: &[i32],
    geo_index: &crate::spatial::SpatialIndex,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::create(output_path)?;
    let mut writer = BufWriter::new(file);

    let mut last_id: i64 = 0;
    let mut total_count: u64 = 0;
    let mut remaining = limit;

    loop {
        #[allow(clippy::cast_sign_loss)]
        let batch_limit = match remaining {
            Some(0) => break,
            Some(r) => i64::try_from(r.min(BATCH_SIZE as u64))?,
            None => BATCH_SIZE,
        };

        let (query, params) = build_batch_query(last_id, batch_limit, source_ids);

        let rows = db.query_raw_params(&query, &params).await?;

        if rows.is_empty() {
            break;
        }

        #[allow(clippy::cast_possible_truncation)]
        let batch_len = rows.len() as u64;

        for row in &rows {
            let id: i64 = row.to_value("id").unwrap_or(0);
            let source_id: i32 = row.to_value("source_id").unwrap_or(0);
            let source_name: String = row.to_value("source_name").unwrap_or_default();
            let lng: f64 = row.to_value("longitude").unwrap_or(0.0);
            let lat: f64 = row.to_value("latitude").unwrap_or(0.0);
            let source_incident_id: String = row.to_value("source_incident_id").unwrap_or_default();
            let subcategory: String = row.to_value("subcategory").unwrap_or_default();
            let category: String = row.to_value("category").unwrap_or_default();
            let severity: i32 = row.to_value("severity").unwrap_or(1);
            let city: String = row.to_value("city").unwrap_or_default();
            let state: String = row.to_value("state").unwrap_or_default();
            let arrest_made: Option<bool> = row.to_value("arrest_made").unwrap_or(None);
            let description: Option<String> = row.to_value("description").unwrap_or(None);
            let block_address: Option<String> = row.to_value("block_address").unwrap_or(None);

            let occurred_at_naive: Option<chrono::NaiveDateTime> =
                row.to_value("occurred_at").unwrap_or(None);
            let occurred_at: Option<String> = occurred_at_naive.map(|naive| {
                chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive, chrono::Utc)
                    .to_rfc3339()
            });

            // Derive boundary GEOIDs from spatial index
            let tract_geoid = geo_index.lookup_tract(lng, lat).map(str::to_owned);
            let state_fips = tract_geoid
                .as_deref()
                .and_then(crate::spatial::SpatialIndex::derive_state_fips)
                .map(str::to_owned);
            let county_geoid = tract_geoid
                .as_deref()
                .and_then(crate::spatial::SpatialIndex::derive_county_geoid)
                .map(str::to_owned);
            let place_geoid = geo_index.lookup_place(lng, lat).map(str::to_owned);
            let neighborhood_id = tract_geoid
                .as_deref()
                .and_then(|g| geo_index.lookup_neighborhood(g))
                .map(str::to_owned);

            let feature = serde_json::json!({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lng, lat]
                },
                "properties": {
                    "id": id,
                    "src": source_id,
                    "src_name": source_name,
                    "sid": source_incident_id,
                    "subcategory": subcategory,
                    "category": category,
                    "severity": severity,
                    "city": city,
                    "state": state,
                    "arrest": arrest_made,
                    "date": occurred_at,
                    "desc": description,
                    "addr": block_address,
                    "state_fips": state_fips,
                    "county_geoid": county_geoid,
                    "place_geoid": place_geoid,
                    "tract_geoid": tract_geoid,
                    "neighborhood_id": neighborhood_id,
                }
            });

            serde_json::to_writer(&mut writer, &feature)?;
            writer.write_all(b"\n")?;

            last_id = id;
        }

        total_count += batch_len;

        if let Some(ref mut r) = remaining {
            *r = r.saturating_sub(batch_len);
        }

        progress.inc(batch_len);
        log::info!("Exported {total_count} features so far...");

        #[allow(clippy::cast_sign_loss)]
        let batch_limit_u64 = batch_limit as u64;
        if batch_len < batch_limit_u64 {
            break;
        }
    }

    writer.flush()?;
    log::info!(
        "Exported {total_count} features to {}",
        output_path.display()
    );
    Ok(())
}
