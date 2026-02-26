#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Library for generating `PMTiles`, sidebar `SQLite`, and count `DuckDB`
//! databases from per-source `DuckDB` crime incident data.
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
//! Iterates per-source `DuckDB` files with keyset pagination and streaming
//! writes to keep memory usage constant regardless of dataset size.

pub mod interactive;
pub mod merge;
pub mod spatial;

use std::collections::BTreeMap;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use crime_map_source::progress::ProgressCallback;
use crime_map_source::registry::all_sources;
use serde::{Deserialize, Serialize};

/// Number of rows to fetch per database query batch.
const BATCH_SIZE: i64 = 10_000;

/// Current manifest schema version. Bump this when the manifest format
/// changes in a backward-incompatible way.
const MANIFEST_VERSION: u32 = 2;

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

/// Opens an output `DuckDB` database with a `2GB` memory limit.
///
/// All generated `DuckDB` files (counts, H3, analytics) should use this
/// helper instead of raw `duckdb::Connection::open()` so they get a
/// consistent memory budget suitable for large sources like Philadelphia
/// (~3.5M records).
///
/// # Errors
///
/// Returns `duckdb::Error` if the connection or configuration fails.
fn open_output_duckdb(path: &Path) -> Result<duckdb::Connection, duckdb::Error> {
    let duck = duckdb::Connection::open(path)?;
    duck.execute_batch("SET memory_limit = '2GB'; SET threads = 4;")?;
    Ok(duck)
}

/// Per-source fingerprint capturing the data state at generation time.
///
/// Since source `DuckDB` files are insert-only (`ON CONFLICT DO NOTHING`),
/// the combination of `record_count`, `last_synced_at`, and
/// `max_occurred_at` is a reliable change indicator.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SourceFingerprint {
    source_id: String,
    name: String,
    record_count: i64,
    last_synced_at: Option<String>,
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
#[allow(clippy::too_many_lines, clippy::future_not_send)]
pub async fn run_with_cache(
    args: &GenerateArgs,
    source_ids: &[String],
    dir: &Path,
    requested_outputs: &[&str],
    progress: Option<Arc<dyn ProgressCallback>>,
) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Querying source fingerprints...");
    let fingerprints = query_fingerprints(source_ids)?;

    // Count the actual exportable records (must match the export WHERE clause)
    let total_records = count_exportable_records(source_ids)?;
    log::info!(
        "Found {} sources, {total_records} exportable records",
        fingerprints.len()
    );

    // Validate that all records have been spatially enriched
    if total_records > 0 {
        validate_enrichment(source_ids)?;
    }

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
    // NOTE: Spatial enrichment now happens at ingest time (`cargo ingest enrich`).
    // The spatial index is no longer loaded here for per-incident lookups.
    // It is still needed for boundary generation (PMTiles, DB).

    // Open boundaries DuckDB for boundary outputs
    let needs_boundaries = needs.get(OUTPUT_BOUNDARIES_PMTILES) == Some(&true)
        || needs.get(OUTPUT_BOUNDARIES_DB) == Some(&true)
        || needs.get(OUTPUT_METADATA) == Some(&true);

    let boundaries_conn = if needs_boundaries {
        Some(crime_map_database::boundaries_db::open_default()?)
    } else {
        None
    };

    // Run each output that needs it
    if needs.get(OUTPUT_INCIDENTS_PMTILES) == Some(&true) {
        progress.set_message("Generating PMTiles...".to_string());
        progress.set_total(total_records);
        progress.set_position(0);
        generate_pmtiles(args, source_ids, dir, &progress)?;
        record_output(manifest, OUTPUT_INCIDENTS_PMTILES);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_INCIDENTS_DB) == Some(&true) {
        progress.set_message("Generating sidebar DB...".to_string());
        progress.set_total(total_records);
        progress.set_position(0);
        generate_sidebar_db(args, source_ids, dir, &progress).await?;
        record_output(manifest, OUTPUT_INCIDENTS_DB);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_COUNT_DB) == Some(&true) {
        progress.set_message("Generating count DB...".to_string());
        progress.set_total(total_records);
        progress.set_position(0);
        generate_count_db(args, source_ids, dir, &progress)?;
        record_output(manifest, OUTPUT_COUNT_DB);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_H3_DB) == Some(&true) {
        progress.set_message("Generating H3 hexbin DB...".to_string());
        progress.set_total(total_records);
        progress.set_position(0);
        generate_h3_db(args, source_ids, dir, &progress)?;
        record_output(manifest, OUTPUT_H3_DB);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_METADATA) == Some(&true) {
        progress.set_message("Generating server metadata...".to_string());
        progress.set_total(0);
        progress.set_position(0);
        generate_metadata(
            source_ids,
            boundaries_conn
                .as_ref()
                .expect("boundaries connection required"),
            dir,
        )?;
        record_output(manifest, OUTPUT_METADATA);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_BOUNDARIES_PMTILES) == Some(&true) {
        progress.set_message("Generating boundaries PMTiles...".to_string());
        progress.set_total(0);
        progress.set_position(0);
        generate_boundaries_pmtiles(
            boundaries_conn
                .as_ref()
                .expect("boundaries connection required"),
            dir,
            &progress,
        )?;
        record_output(manifest, OUTPUT_BOUNDARIES_PMTILES);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_BOUNDARIES_DB) == Some(&true) {
        progress.set_message("Generating boundaries search DB...".to_string());
        progress.set_total(0);
        progress.set_position(0);
        generate_boundaries_db(
            boundaries_conn
                .as_ref()
                .expect("boundaries connection required"),
            dir,
        )
        .await?;
        record_output(manifest, OUTPUT_BOUNDARIES_DB);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_ANALYTICS_DB) == Some(&true) {
        progress.set_message("Generating analytics DB...".to_string());
        progress.set_total(total_records);
        progress.set_position(0);
        generate_analytics_db(
            args,
            source_ids,
            boundaries_conn
                .as_ref()
                .expect("boundaries connection required"),
            dir,
            &progress,
        )?;
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

/// Queries per-source `DuckDB` `_meta` tables for fingerprints used to
/// detect data changes.
///
/// Returns one [`SourceFingerprint`] per source, ordered by source ID.
///
/// # Errors
///
/// Returns an error if any source database cannot be opened or queried.
fn query_fingerprints(
    source_ids: &[String],
) -> Result<Vec<SourceFingerprint>, Box<dyn std::error::Error>> {
    let mut fingerprints = Vec::with_capacity(source_ids.len());

    for sid in source_ids {
        let path = crime_map_database::paths::source_db_path(sid);
        if !path.exists() {
            log::warn!(
                "Source DuckDB not found: {} — skipping fingerprint",
                path.display()
            );
            continue;
        }

        let conn = crime_map_database::source_db::open_by_id(sid)?;
        let name =
            crime_map_database::source_db::get_meta(&conn, "source_name")?.unwrap_or_default();
        let record_count = crime_map_database::source_db::get_record_count(&conn)?;
        let last_synced_at = crime_map_database::source_db::get_meta(&conn, "last_synced_at")?;

        #[allow(clippy::cast_possible_wrap)]
        fingerprints.push(SourceFingerprint {
            source_id: sid.clone(),
            name,
            record_count: record_count as i64,
            last_synced_at,
        });
    }

    Ok(fingerprints)
}

/// Counts incidents with coordinates across all source `DuckDB` files.
///
/// Uses the same `has_coordinates = TRUE` + coordinate range filter as
/// the progress bar total matches the real feature count.
///
/// # Errors
///
/// Returns an error if any source database cannot be opened or queried.
fn count_exportable_records(source_ids: &[String]) -> Result<u64, Box<dyn std::error::Error>> {
    let mut total: u64 = 0;

    for sid in source_ids {
        let path = crime_map_database::paths::source_db_path(sid);
        if !path.exists() {
            continue;
        }

        let conn = crime_map_database::source_db::open_by_id(sid)?;
        let mut stmt =
            conn.prepare("SELECT COUNT(*) FROM incidents WHERE has_coordinates = TRUE AND longitude BETWEEN -180 AND 180 AND latitude BETWEEN -90 AND 90")?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        #[allow(clippy::cast_sign_loss)]
        {
            total += count as u64;
        }
    }

    Ok(total)
}

/// Validates that all exportable records in the given sources have been
/// spatially enriched (i.e., `enriched = TRUE`).
///
/// Returns an error listing un-enriched sources if any are found.
/// This ensures the `cargo ingest enrich` step was run before generation.
///
/// # Errors
///
/// Returns an error if any source has un-enriched records or if
/// database queries fail.
fn validate_enrichment(source_ids: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let mut unenriched: Vec<(String, u64)> = Vec::new();

    for sid in source_ids {
        let path = crime_map_database::paths::source_db_path(sid);
        if !path.exists() {
            continue;
        }

        let conn = crime_map_database::source_db::open_by_id(sid)?;
        let mut stmt = conn.prepare(
            "SELECT COUNT(*) FROM incidents
             WHERE has_coordinates = TRUE
               AND enriched = FALSE
               AND longitude BETWEEN -180 AND 180
               AND latitude BETWEEN -90 AND 90",
        )?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;

        #[allow(clippy::cast_sign_loss)]
        if count > 0 {
            unenriched.push((sid.clone(), count as u64));
        }
    }

    if unenriched.is_empty() {
        return Ok(());
    }

    let mut msg = String::from(
        "Found un-enriched records. Run `cargo ingest enrich` before generation.\n\
         Un-enriched sources:\n",
    );
    for (sid, count) in &unenriched {
        use std::fmt::Write;
        writeln!(msg, "  - {sid}: {count} record(s)").unwrap();
    }
    msg.push_str("Hint: cargo ingest enrich --sources ");
    msg.push_str(
        &unenriched
            .iter()
            .map(|(s, _)| s.as_str())
            .collect::<Vec<_>>()
            .join(","),
    );

    Err(msg.into())
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

/// Resolves `--sources` and/or `--states` filters to source short IDs.
///
/// When `--sources` is provided, validates each short ID against the TOML
/// registry and filesystem.
///
/// When `--states` is provided, maps FIPS codes to state abbreviations
/// and filters the registry by the `state` field on each source.
///
/// If both are provided, their results are unioned (deduplicated).
///
/// Returns an empty `Vec` if neither flag was provided (meaning: export
/// all sources that have `DuckDB` files on disk).
///
/// # Errors
///
/// Returns an error if a provided source ID does not match any configured
/// source.
pub fn resolve_source_ids(args: &GenerateArgs) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    if args.sources.is_none() && args.states.is_none() {
        // No filter: discover all source DuckDB files on disk
        let ids = crime_map_database::source_db::discover_source_ids();
        if ids.is_empty() {
            return Err("No source DuckDB files found in data/sources/".into());
        }
        log::info!("Discovered {} source DuckDB files", ids.len());
        return Ok(ids);
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

    // Validate each short ID exists in the registry
    let mut result = Vec::with_capacity(short_ids.len());
    for short_id in &short_ids {
        if registry.iter().any(|s| s.id() == short_id.as_str()) {
            let path = crime_map_database::paths::source_db_path(short_id);
            if path.exists() {
                result.push(short_id.clone());
                log::info!("Resolved source '{short_id}' -> {}", path.display());
            } else {
                log::warn!(
                    "Source '{short_id}' is in the registry but has no DuckDB file — skipping",
                );
            }
        } else {
            return Err(format!("Unknown source ID: {short_id}").into());
        }
    }

    if result.is_empty() {
        return Err("None of the requested sources have DuckDB files on disk".into());
    }

    Ok(result)
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

// ============================================================
// Per-source DuckDB row iteration helpers
// ============================================================

/// A decoded incident row from a source `DuckDB` file.
#[allow(dead_code)]
struct IncidentRow {
    source_incident_id: String,
    source_id: String,
    source_name: String,
    category: String,
    parent_category: String,
    severity: i32,
    longitude: f64,
    latitude: f64,
    occurred_at: Option<String>,
    description: Option<String>,
    block_address: Option<String>,
    city: String,
    state: String,
    arrest_made: Option<bool>,
    domestic: Option<bool>,
    location_type: Option<String>,
    // Pre-computed spatial attribution (populated by `cargo ingest enrich`)
    census_tract_geoid: Option<String>,
    census_place_geoid: Option<String>,
    state_fips: Option<String>,
    county_geoid: Option<String>,
    neighborhood_id: Option<String>,
}

/// Iterates over incidents from a single source `DuckDB` with keyset
/// pagination. Calls `callback` for each row. Respects `limit` and
/// `remaining` count.
///
/// Returns the number of rows processed.
///
/// # Errors
///
/// Returns an error if the source database cannot be opened or queried.
fn iterate_source_incidents<F>(
    source_id: &str,
    source_name: &str,
    limit: &mut Option<u64>,
    callback: &mut F,
) -> Result<u64, Box<dyn std::error::Error>>
where
    F: FnMut(&IncidentRow) -> Result<(), Box<dyn std::error::Error>>,
{
    let conn = crime_map_database::source_db::open_by_id(source_id)?;
    let mut last_rowid: i64 = 0;
    let mut count: u64 = 0;

    loop {
        if *limit == Some(0) {
            break;
        }

        #[allow(clippy::cast_sign_loss)]
        let batch_limit = match *limit {
            Some(r) => i64::try_from(r.min(BATCH_SIZE as u64))?,
            None => BATCH_SIZE,
        };

        let mut stmt = conn.prepare(
            "SELECT rowid,
                    source_incident_id, category, parent_category, severity,
                    longitude, latitude, occurred_at::TEXT as occurred_at_text,
                    description, block_address,
                    city, state, arrest_made, domestic, location_type,
                    census_tract_geoid, census_place_geoid, state_fips,
                    county_geoid, neighborhood_id
             FROM incidents
             WHERE has_coordinates = TRUE
               AND longitude BETWEEN -180 AND 180
               AND latitude BETWEEN -90 AND 90
               AND rowid > ?
             ORDER BY rowid ASC
             LIMIT ?",
        )?;

        let mut rows = stmt.query(duckdb::params![last_rowid, batch_limit])?;

        let mut batch_len: u64 = 0;
        while let Some(row) = rows.next()? {
            let rowid: i64 = row.get(0)?;
            last_rowid = rowid;

            let incident = IncidentRow {
                source_incident_id: row.get(1)?,
                source_id: source_id.to_string(),
                source_name: source_name.to_string(),
                category: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                parent_category: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                severity: row.get::<_, Option<i16>>(4)?.unwrap_or(1).into(),
                longitude: row.get(5)?,
                latitude: row.get(6)?,
                occurred_at: row.get(7)?,
                description: row.get(8)?,
                block_address: row.get(9)?,
                city: row.get::<_, Option<String>>(10)?.unwrap_or_default(),
                state: row.get::<_, Option<String>>(11)?.unwrap_or_default(),
                arrest_made: row.get(12)?,
                domestic: row.get(13)?,
                location_type: row.get(14)?,
                census_tract_geoid: row.get(15)?,
                census_place_geoid: row.get(16)?,
                state_fips: row.get(17)?,
                county_geoid: row.get(18)?,
                neighborhood_id: row.get(19)?,
            };

            callback(&incident)?;
            batch_len += 1;
        }

        if batch_len == 0 {
            break;
        }

        count += batch_len;
        if let Some(ref mut r) = *limit {
            *r = r.saturating_sub(batch_len);
        }

        #[allow(clippy::cast_sign_loss)]
        let batch_limit_u64 = batch_limit as u64;
        if batch_len < batch_limit_u64 {
            break;
        }
    }

    Ok(count)
}

/// Resolves the human-readable source name for a source ID.
///
/// Reads from the `_meta` table in the source's `DuckDB` file, or falls
/// back to the TOML registry name.
fn resolve_source_name(source_id: &str) -> String {
    if let Ok(conn) = crime_map_database::source_db::open_by_id(source_id)
        && let Ok(Some(name)) = crime_map_database::source_db::get_meta(&conn, "source_name")
    {
        return name;
    }

    // Fall back to the TOML registry
    let registry = all_sources();
    registry
        .iter()
        .find(|s| s.id() == source_id)
        .map_or_else(|| source_id.to_string(), |s| s.name().to_string())
}

// ============================================================
// PMTiles generation
// ============================================================

/// Exports incidents as `GeoJSONSeq` and generates `PMTiles` via tippecanoe.
fn generate_pmtiles(
    args: &GenerateArgs,
    source_ids: &[String],
    dir: &Path,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<(), Box<dyn std::error::Error>> {
    let geojsonseq_path = dir.join("incidents.geojsonseq");

    log::info!("Exporting incidents to GeoJSONSeq...");
    export_geojsonseq(&geojsonseq_path, args.limit, source_ids, progress)?;

    // Skip tippecanoe if no features were exported (empty GeoJSONSeq).
    // tippecanoe crashes with "Did not read any valid geometries" on empty input.
    let file_size = std::fs::metadata(&geojsonseq_path)
        .map(|m| m.len())
        .unwrap_or(0);
    if file_size == 0 {
        log::warn!("No incident features to tile; skipping PMTiles generation");
        std::fs::remove_file(&geojsonseq_path).ok();
        return Ok(());
    }

    log::info!("Running tippecanoe to generate PMTiles...");

    let output_path = dir.join("incidents.pmtiles");

    let mut cmd = Command::new("tippecanoe");
    cmd.args([
        "-o",
        &*output_path.to_string_lossy(),
        "--force",
        "--no-feature-limit",
        "--no-tile-size-limit",
        "--minimum-zoom=0",
        "--maximum-zoom=14",
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        "--layer=incidents",
        &*geojsonseq_path.to_string_lossy(),
    ]);

    if std::env::var("CI").is_ok() {
        cmd.arg("--quiet");
    }

    let status = cmd.status()?;

    if !status.success() {
        return Err("tippecanoe failed".into());
    }

    log::info!("PMTiles generated: {}", output_path.display());
    Ok(())
}

/// Exports all incidents from source `DuckDB` files as newline-delimited
/// `GeoJSON`, iterating per-source with keyset pagination and streaming
/// writes to keep memory constant.
fn export_geojsonseq(
    output_path: &Path,
    limit: Option<u64>,
    source_ids: &[String],
    progress: &Arc<dyn ProgressCallback>,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::create(output_path)?;
    let mut writer = BufWriter::new(file);
    let mut total_count: u64 = 0;
    let mut remaining = limit;

    for sid in source_ids {
        if remaining == Some(0) {
            break;
        }

        let source_name = resolve_source_name(sid);
        let source_count =
            iterate_source_incidents(sid, &source_name, &mut remaining, &mut |incident| {
                // Read pre-computed spatial attribution from source DuckDB
                let tract_geoid = incident.census_tract_geoid.clone();
                let state_fips = incident.state_fips.clone();
                let county_geoid = incident.county_geoid.clone();
                let place_geoid = incident.census_place_geoid.clone();
                let neighborhood_id = incident.neighborhood_id.clone();

                let feature = serde_json::json!({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [incident.longitude, incident.latitude]
                    },
                    "properties": {
                        "sid": incident.source_incident_id,
                        "src": incident.source_id,
                        "src_name": incident.source_name,
                        "subcategory": incident.category,
                        "category": incident.parent_category,
                        "severity": incident.severity,
                        "city": incident.city,
                        "state": incident.state,
                        "arrest": incident.arrest_made,
                        "date": incident.occurred_at,
                        "desc": incident.description,
                        "addr": incident.block_address,
                        "state_fips": state_fips,
                        "county_geoid": county_geoid,
                        "place_geoid": place_geoid,
                        "tract_geoid": tract_geoid,
                        "neighborhood_id": neighborhood_id,
                    }
                });

                serde_json::to_writer(&mut writer, &feature)?;
                writer.write_all(b"\n")?;
                Ok(())
            })?;

        total_count += source_count;
        progress.inc(source_count);
        log::info!("Exported {source_count} features from source '{sid}' (total: {total_count})");
    }

    writer.flush()?;
    log::info!(
        "Exported {total_count} features to {}",
        output_path.display()
    );
    Ok(())
}

// ============================================================
// Sidebar SQLite generation
// ============================================================

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
/// Returns an error if the source `DuckDB` export, `SQLite` creation, or
/// index population fails.
#[allow(clippy::too_many_lines, clippy::future_not_send)]
async fn generate_sidebar_db(
    args: &GenerateArgs,
    source_ids: &[String],
    dir: &Path,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<(), Box<dyn std::error::Error>> {
    use switchy_database::DatabaseValue;

    let db_path = dir.join("incidents.db");

    // Remove any existing file so we start fresh
    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
    }

    log::info!("Creating sidebar SQLite database...");

    let sqlite = switchy_database_connection::init_sqlite_rusqlite(Some(&db_path))
        .map_err(|e| format!("Failed to open sidebar SQLite: {e}"))?;

    // WAL mode + generous busy timeout to avoid "database is locked" errors
    // when the connection pool uses multiple connections.
    sqlite.exec_raw("PRAGMA journal_mode=WAL").await?;
    sqlite.exec_raw("PRAGMA busy_timeout=5000").await?;

    // Create schema
    sqlite
        .exec_raw(
            "CREATE TABLE incidents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT NOT NULL,
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
        .await
        .map_err(|e| format!("Failed to create incidents table: {e}"))?;

    sqlite
        .exec_raw(
            "CREATE VIRTUAL TABLE incidents_rtree USING rtree(
                id, min_lng, max_lng, min_lat, max_lat
            )",
        )
        .await
        .map_err(|e| format!("Failed to create incidents_rtree: {e}"))?;

    // Populate from per-source DuckDB files
    let mut total_count: u64 = 0;
    let mut remaining = args.limit;

    for sid in source_ids {
        if remaining == Some(0) {
            break;
        }

        let source_name = resolve_source_name(sid);

        let source_count = {
            // We need to batch-insert into SQLite. Collect into a Vec per batch.
            let conn = crime_map_database::source_db::open_by_id(sid)?;
            let mut last_rowid: i64 = 0;
            let mut source_total: u64 = 0;

            loop {
                if remaining == Some(0) {
                    break;
                }

                #[allow(clippy::cast_sign_loss)]
                let batch_limit = match remaining {
                    Some(r) => i64::try_from(r.min(BATCH_SIZE as u64))?,
                    None => BATCH_SIZE,
                };

                // Collect batch from DuckDB in a separate scope so non-Send
                // DuckDB types are dropped before any .await points.
                let batch: Vec<IncidentRow> = {
                    let mut stmt = conn.prepare(
                        "SELECT rowid,
                                source_incident_id, category, parent_category, severity,
                                longitude, latitude, occurred_at::TEXT as occurred_at_text,
                                description, block_address,
                                city, state, arrest_made, domestic, location_type,
                                census_tract_geoid, census_place_geoid, state_fips,
                                county_geoid, neighborhood_id
                         FROM incidents
                         WHERE has_coordinates = TRUE
                   AND longitude BETWEEN -180 AND 180
                   AND latitude BETWEEN -90 AND 90
                   AND rowid > ?
                         ORDER BY rowid ASC
                         LIMIT ?",
                    )?;

                    let mut rows = stmt.query(duckdb::params![last_rowid, batch_limit])?;

                    let mut batch: Vec<IncidentRow> = Vec::new();
                    while let Some(row) = rows.next()? {
                        let rowid: i64 = row.get(0)?;
                        last_rowid = rowid;

                        batch.push(IncidentRow {
                            source_incident_id: row.get(1)?,
                            source_id: sid.clone(),
                            source_name: source_name.clone(),
                            category: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                            parent_category: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                            severity: row.get::<_, Option<i16>>(4)?.unwrap_or(1).into(),
                            longitude: row.get(5)?,
                            latitude: row.get(6)?,
                            occurred_at: row.get(7)?,
                            description: row.get(8)?,
                            block_address: row.get(9)?,
                            city: row.get::<_, Option<String>>(10)?.unwrap_or_default(),
                            state: row.get::<_, Option<String>>(11)?.unwrap_or_default(),
                            arrest_made: row.get(12)?,
                            domestic: row.get(13)?,
                            location_type: row.get(14)?,
                            census_tract_geoid: row.get(15)?,
                            census_place_geoid: row.get(16)?,
                            state_fips: row.get(17)?,
                            county_geoid: row.get(18)?,
                            neighborhood_id: row.get(19)?,
                        });
                    }
                    batch
                };

                if batch.is_empty() {
                    break;
                }

                #[allow(clippy::cast_possible_truncation)]
                let batch_len = batch.len() as u64;

                // Insert batch into SQLite within a transaction.
                // begin_transaction() pins all operations to one pooled
                // connection, avoiding "database is locked" errors.
                let tx = sqlite
                    .begin_transaction()
                    .await
                    .map_err(|e| format!("Failed to begin transaction: {e}"))?;

                for incident in &batch {
                    let tract_geoid = incident.census_tract_geoid.clone();
                    let state_fips = incident.state_fips.clone();
                    let county_geoid = incident.county_geoid.clone();
                    let place_geoid = incident.census_place_geoid.clone();
                    let neighborhood_id = incident.neighborhood_id.clone();

                    let arrest_int = incident.arrest_made.map(i32::from);

                    tx
                        .exec_raw_params(
                            "INSERT INTO incidents (source_id, source_name, source_incident_id,
                                subcategory, category,
                                severity, longitude, latitude, occurred_at, description,
                                block_address, city, state, arrest_made, location_type,
                                state_fips, county_geoid, place_geoid, tract_geoid, neighborhood_id)
                             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)",
                            &[
                                DatabaseValue::String(incident.source_id.clone()),
                                DatabaseValue::String(incident.source_name.clone()),
                                DatabaseValue::String(incident.source_incident_id.clone()),
                                DatabaseValue::String(incident.category.clone()),
                                DatabaseValue::String(incident.parent_category.clone()),
                                DatabaseValue::Int32(incident.severity),
                                DatabaseValue::Real64(incident.longitude),
                                DatabaseValue::Real64(incident.latitude),
                                incident.occurred_at.as_ref().map_or(DatabaseValue::Null, |s| DatabaseValue::String(s.clone())),
                                incident.description.as_ref().map_or(DatabaseValue::Null, |s| DatabaseValue::String(s.clone())),
                                incident.block_address.as_ref().map_or(DatabaseValue::Null, |s| DatabaseValue::String(s.clone())),
                                DatabaseValue::String(incident.city.clone()),
                                DatabaseValue::String(incident.state.clone()),
                                arrest_int.map_or(DatabaseValue::Null, DatabaseValue::Int32),
                                incident.location_type.as_ref().map_or(DatabaseValue::Null, |s| DatabaseValue::String(s.clone())),
                                state_fips.map_or(DatabaseValue::Null, DatabaseValue::String),
                                county_geoid.map_or(DatabaseValue::Null, DatabaseValue::String),
                                place_geoid.map_or(DatabaseValue::Null, DatabaseValue::String),
                                tract_geoid.map_or(DatabaseValue::Null, DatabaseValue::String),
                                neighborhood_id.map_or(DatabaseValue::Null, DatabaseValue::String),
                            ],
                        )
                        .await
                        .map_err(|e| format!("Failed to insert incident: {e}"))?;
                }

                tx.commit()
                    .await
                    .map_err(|e| format!("Failed to commit transaction: {e}"))?;

                source_total += batch_len;
                if let Some(ref mut r) = remaining {
                    *r = r.saturating_sub(batch_len);
                }

                progress.inc(batch_len);

                #[allow(clippy::cast_sign_loss)]
                let batch_limit_u64 = batch_limit as u64;
                if batch_len < batch_limit_u64 {
                    break;
                }
            }

            source_total
        };

        total_count += source_count;
        log::info!("Inserted {source_count} rows from source '{sid}' into sidebar DB...");
    }

    // Populate R-tree from incidents table
    log::info!("Populating R-tree spatial index...");
    sqlite
        .exec_raw(
            "INSERT INTO incidents_rtree (id, min_lng, max_lng, min_lat, max_lat)
             SELECT id, longitude, longitude, latitude, latitude FROM incidents",
        )
        .await
        .map_err(|e| format!("Failed to populate R-tree: {e}"))?;

    // Create date index for feature queries
    log::info!("Creating indexes...");
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_occurred_at ON incidents(occurred_at DESC)")
        .await
        .map_err(|e| format!("Failed to create index: {e}"))?;
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_source_id ON incidents(source_id)")
        .await
        .map_err(|e| format!("Failed to create index: {e}"))?;
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_state_fips ON incidents(state_fips)")
        .await
        .map_err(|e| format!("Failed to create index: {e}"))?;
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_county_geoid ON incidents(county_geoid)")
        .await
        .map_err(|e| format!("Failed to create index: {e}"))?;
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_place_geoid ON incidents(place_geoid)")
        .await
        .map_err(|e| format!("Failed to create index: {e}"))?;
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_tract_geoid ON incidents(tract_geoid)")
        .await
        .map_err(|e| format!("Failed to create index: {e}"))?;
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_neighborhood_id ON incidents(neighborhood_id)")
        .await
        .map_err(|e| format!("Failed to create index: {e}"))?;
    sqlite
        .exec_raw("ANALYZE")
        .await
        .map_err(|e| format!("Failed to run ANALYZE: {e}"))?;

    log::info!(
        "Sidebar SQLite database generated: {} ({total_count} rows)",
        db_path.display()
    );
    Ok(())
}

// ============================================================
// Count DuckDB generation
// ============================================================

/// Generates a `DuckDB` database with a pre-aggregated `count_summary` table
/// for fast count queries.
///
/// Creates `counts.duckdb` with:
/// - A raw `incidents` table populated from source `DuckDB` files
/// - A `count_summary` table aggregated by spatial cell, subcategory, severity,
///   arrest status, and day
///
/// At runtime, count queries become a simple `SUM(cnt)` over the summary table
/// filtered by cell coordinates, completing in under 10ms for any bounding box.
///
/// # Errors
///
/// Returns an error if the source `DuckDB` export, output `DuckDB` creation,
/// or aggregation fails.
fn generate_count_db(
    args: &GenerateArgs,
    source_ids: &[String],
    dir: &Path,
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

    {
        let duck = open_output_duckdb(&db_path)?;

        // Create raw incidents table for aggregation
        duck.execute_batch(
            "CREATE TABLE incidents (
                source_id VARCHAR NOT NULL,
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
    }

    let total_count = populate_duckdb_incidents(args, source_ids, &db_path, progress)?;

    // Reopen for aggregation
    let duck = open_output_duckdb(&db_path)?;

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

/// Populates the `DuckDB` incidents table from source `DuckDB` files.
///
/// Iterates each source, reads incidents, computes boundary GEOIDs via
/// the spatial index, and inserts into the output `DuckDB`.
///
/// Returns the total number of rows inserted.
///
/// # Errors
///
/// Returns an error if any source or output database operation fails.
#[allow(clippy::too_many_lines)]
fn populate_duckdb_incidents(
    args: &GenerateArgs,
    source_ids: &[String],
    duck_path: &Path,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut total_count: u64 = 0;
    let mut remaining = args.limit;

    for sid in source_ids {
        if remaining == Some(0) {
            break;
        }

        let source_name = resolve_source_name(sid);

        // Iterate source DuckDB and insert into output DuckDB in batches
        let conn = crime_map_database::source_db::open_by_id(sid)?;
        let mut last_rowid: i64 = 0;
        let mut source_total: u64 = 0;

        loop {
            if remaining == Some(0) {
                break;
            }

            #[allow(clippy::cast_sign_loss)]
            let batch_limit = match remaining {
                Some(r) => i64::try_from(r.min(BATCH_SIZE as u64))?,
                None => BATCH_SIZE,
            };

            let mut stmt = conn.prepare(
                "SELECT rowid,
                        source_incident_id, category, parent_category, severity,
                        longitude, latitude, occurred_at::TEXT as occurred_at_text,
                        description, block_address,
                        city, state, arrest_made, domestic, location_type,
                        census_tract_geoid, census_place_geoid, state_fips,
                        county_geoid, neighborhood_id
                 FROM incidents
                 WHERE has_coordinates = TRUE
                   AND longitude BETWEEN -180 AND 180
                   AND latitude BETWEEN -90 AND 90
                   AND rowid > ?
                 ORDER BY rowid ASC
                 LIMIT ?",
            )?;

            let mut rows = stmt.query(duckdb::params![last_rowid, batch_limit])?;

            // Collect batch in memory
            let mut batch: Vec<IncidentRow> = Vec::new();
            while let Some(row) = rows.next()? {
                let rowid: i64 = row.get(0)?;
                last_rowid = rowid;

                batch.push(IncidentRow {
                    source_incident_id: row.get(1)?,
                    source_id: sid.clone(),
                    source_name: source_name.clone(),
                    category: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                    parent_category: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                    severity: row.get::<_, Option<i16>>(4)?.unwrap_or(1).into(),
                    longitude: row.get(5)?,
                    latitude: row.get(6)?,
                    occurred_at: row.get(7)?,
                    description: row.get(8)?,
                    block_address: row.get(9)?,
                    city: row.get::<_, Option<String>>(10)?.unwrap_or_default(),
                    state: row.get::<_, Option<String>>(11)?.unwrap_or_default(),
                    arrest_made: row.get(12)?,
                    domestic: row.get(13)?,
                    location_type: row.get(14)?,
                    census_tract_geoid: row.get(15)?,
                    census_place_geoid: row.get(16)?,
                    state_fips: row.get(17)?,
                    county_geoid: row.get(18)?,
                    neighborhood_id: row.get(19)?,
                });
            }

            if batch.is_empty() {
                break;
            }

            #[allow(clippy::cast_possible_truncation)]
            let batch_len = batch.len() as u64;

            // Open output DuckDB per batch (avoids holding non-Send across await points)
            {
                let duck = open_output_duckdb(duck_path)?;
                duck.execute_batch("BEGIN TRANSACTION")?;

                let mut insert_stmt = duck.prepare(
                    "INSERT INTO incidents (source_id, subcategory, severity, longitude, latitude,
                        occurred_at, arrest_made, category,
                        state_fips, county_geoid, place_geoid, tract_geoid, neighborhood_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )?;

                for incident in &batch {
                    let tract_geoid = incident.census_tract_geoid.clone();
                    let state_fips = incident.state_fips.clone();
                    let county_geoid = incident.county_geoid.clone();
                    let place_geoid = incident.census_place_geoid.clone();
                    let neighborhood_id = incident.neighborhood_id.clone();

                    let arrest_int: Option<i32> = incident.arrest_made.map(i32::from);

                    insert_stmt.execute(duckdb::params![
                        incident.source_id,
                        incident.category,
                        incident.severity,
                        incident.longitude,
                        incident.latitude,
                        incident.occurred_at,
                        arrest_int,
                        incident.parent_category,
                        state_fips,
                        county_geoid,
                        place_geoid,
                        tract_geoid,
                        neighborhood_id,
                    ])?;
                }

                duck.execute_batch("COMMIT")?;
            }

            source_total += batch_len;
            if let Some(ref mut r) = remaining {
                *r = r.saturating_sub(batch_len);
            }

            progress.inc(batch_len);

            #[allow(clippy::cast_sign_loss)]
            let batch_limit_u64 = batch_limit as u64;
            if batch_len < batch_limit_u64 {
                break;
            }
        }

        total_count += source_total;
        log::info!("Inserted {source_total} rows from source '{sid}' into DuckDB...");
    }

    log::info!("Inserted {total_count} total rows into DuckDB");
    Ok(total_count)
}

// ============================================================
// H3 hexbin DuckDB generation
// ============================================================

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
/// Returns an error if the source `DuckDB` export, output `DuckDB`
/// creation, or aggregation fails.
#[allow(clippy::too_many_lines)]
fn generate_h3_db(
    args: &GenerateArgs,
    source_ids: &[String],
    dir: &Path,
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
        let duck = open_output_duckdb(&db_path)?;

        // Create staging table: one row per incident with H3 indices as columns.
        duck.execute_batch(
            "CREATE TABLE h3_staging (
                source_id VARCHAR NOT NULL,
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

    // Populate staging table from per-source DuckDB files
    let mut total_count: u64 = 0;
    let mut remaining = args.limit;

    for sid in source_ids {
        if remaining == Some(0) {
            break;
        }

        let source_name = resolve_source_name(sid);

        let conn = crime_map_database::source_db::open_by_id(sid)?;
        let mut last_rowid: i64 = 0;
        let mut source_total: u64 = 0;

        loop {
            if remaining == Some(0) {
                break;
            }

            #[allow(clippy::cast_sign_loss)]
            let batch_limit = match remaining {
                Some(r) => i64::try_from(r.min(H3_BATCH_SIZE as u64))?,
                None => H3_BATCH_SIZE,
            };

            let mut stmt = conn.prepare(
                "SELECT rowid,
                        source_incident_id, category, parent_category, severity,
                        longitude, latitude, occurred_at::TEXT as occurred_at_text,
                        description, block_address,
                        city, state, arrest_made, domestic, location_type,
                        census_tract_geoid, census_place_geoid, state_fips,
                        county_geoid, neighborhood_id
                 FROM incidents
                 WHERE has_coordinates = TRUE
                   AND longitude BETWEEN -180 AND 180
                   AND latitude BETWEEN -90 AND 90
                   AND rowid > ?
                 ORDER BY rowid ASC
                 LIMIT ?",
            )?;

            let mut rows = stmt.query(duckdb::params![last_rowid, batch_limit])?;

            let mut batch: Vec<IncidentRow> = Vec::new();
            while let Some(row) = rows.next()? {
                let rowid: i64 = row.get(0)?;
                last_rowid = rowid;

                batch.push(IncidentRow {
                    source_incident_id: row.get(1)?,
                    source_id: sid.clone(),
                    source_name: source_name.clone(),
                    category: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                    parent_category: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                    severity: row.get::<_, Option<i16>>(4)?.unwrap_or(1).into(),
                    longitude: row.get(5)?,
                    latitude: row.get(6)?,
                    occurred_at: row.get(7)?,
                    description: row.get(8)?,
                    block_address: row.get(9)?,
                    city: row.get::<_, Option<String>>(10)?.unwrap_or_default(),
                    state: row.get::<_, Option<String>>(11)?.unwrap_or_default(),
                    arrest_made: row.get(12)?,
                    domestic: row.get(13)?,
                    location_type: row.get(14)?,
                    census_tract_geoid: row.get(15)?,
                    census_place_geoid: row.get(16)?,
                    state_fips: row.get(17)?,
                    county_geoid: row.get(18)?,
                    neighborhood_id: row.get(19)?,
                });
            }

            if batch.is_empty() {
                break;
            }

            #[allow(clippy::cast_possible_truncation)]
            let batch_len = batch.len() as u64;

            // Insert into H3 staging in the output DuckDB
            {
                let duck = open_output_duckdb(&db_path)?;
                duck.execute_batch("BEGIN TRANSACTION")?;

                let mut insert_stmt = duck.prepare(
                    "INSERT INTO h3_staging (source_id, category, subcategory, severity, arrest, day, lng, lat,
                        h3_r4, h3_r5, h3_r6, h3_r7, h3_r8, h3_r9,
                        state_fips, county_geoid, place_geoid, tract_geoid, neighborhood_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )?;

                for incident in &batch {
                    let arrest_int: i32 = match incident.arrest_made {
                        Some(true) => 1,
                        Some(false) => 0,
                        None => 2,
                    };

                    let day = incident
                        .occurred_at
                        .as_deref()
                        .and_then(|s| s.get(..10))
                        .unwrap_or("");

                    // Boundary GEOIDs
                    let tract_geoid = incident.census_tract_geoid.clone();
                    let state_fips = incident.state_fips.clone();
                    let county_geoid = incident.county_geoid.clone();
                    let place_geoid = incident.census_place_geoid.clone();
                    let neighborhood_id = incident.neighborhood_id.clone();

                    let Ok(coord) = LatLng::new(incident.latitude, incident.longitude) else {
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

                    insert_stmt.execute(duckdb::params![
                        incident.source_id,
                        incident.parent_category,
                        incident.category,
                        incident.severity,
                        arrest_int,
                        day,
                        incident.longitude,
                        incident.latitude,
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
                }

                duck.execute_batch("COMMIT")?;
            }

            source_total += batch_len;
            if let Some(ref mut r) = remaining {
                *r = r.saturating_sub(batch_len);
            }

            progress.inc(batch_len);

            #[allow(clippy::cast_sign_loss)]
            let batch_limit_u64 = batch_limit as u64;
            if batch_len < batch_limit_u64 {
                break;
            }
        }

        total_count += source_total;
        log::info!("Loaded {source_total} incidents from source '{sid}' into H3 staging table...");
    }

    // Aggregate staging table into final h3_counts using UNION ALL
    // across the 6 resolution columns.
    let duck = open_output_duckdb(&db_path)?;

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

// ============================================================
// Metadata JSON generation
// ============================================================

/// Generates a `metadata.json` file containing server startup context.
///
/// This includes:
/// - `cities`: distinct `(city, state)` pairs from the dataset
/// - `minDate` / `maxDate`: the earliest and latest `occurred_at` timestamps
/// - `sources`: source metadata from the TOML registry
///
/// The server loads this file at boot to populate the AI agent context
/// without needing a live database connection.
///
/// # Errors
///
/// Returns an error if the database query or file write fails.
fn generate_metadata(
    source_ids: &[String],
    boundaries_conn: &duckdb::Connection,
    dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Querying available cities...");

    let mut all_cities: std::collections::BTreeSet<(String, String)> =
        std::collections::BTreeSet::new();
    let mut min_date: Option<String> = None;
    let mut max_date: Option<String> = None;

    let registry = all_sources();
    let mut sources: Vec<serde_json::Value> = Vec::new();

    for sid in source_ids {
        let path = crime_map_database::paths::source_db_path(sid);
        if !path.exists() {
            continue;
        }

        let conn = crime_map_database::source_db::open_by_id(sid)?;

        // Collect distinct cities
        let mut stmt = conn.prepare(
            "SELECT DISTINCT city, state FROM incidents
             WHERE city IS NOT NULL AND city != ''",
        )?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let city: String = row.get::<_, Option<String>>(0)?.unwrap_or_default();
            let state: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
            if !city.is_empty() {
                all_cities.insert((city, state));
            }
        }

        // Collect date range
        let mut stmt = conn.prepare(
            "SELECT MIN(occurred_at)::TEXT as min_d, MAX(occurred_at)::TEXT as max_d
              FROM incidents WHERE has_coordinates = TRUE
                AND longitude BETWEEN -180 AND 180
                AND latitude BETWEEN -90 AND 90
                AND occurred_at IS NOT NULL",
        )?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let src_min: Option<String> = row.get(0)?;
            let src_max: Option<String> = row.get(1)?;

            if let Some(d) = src_min {
                min_date = Some(match min_date {
                    Some(ref cur) if cur.as_str() <= d.as_str() => cur.clone(),
                    _ => d,
                });
            }
            if let Some(d) = src_max {
                max_date = Some(match max_date {
                    Some(ref cur) if cur.as_str() >= d.as_str() => cur.clone(),
                    _ => d,
                });
            }
        }

        // Build source metadata from registry + _meta
        let source_name =
            crime_map_database::source_db::get_meta(&conn, "source_name")?.unwrap_or_default();
        let record_count = crime_map_database::source_db::get_record_count(&conn)?;

        // Find registry entry for additional metadata
        let def = registry.iter().find(|s| s.id() == sid.as_str());

        let portal_url = def.and_then(crime_map_source::source_def::SourceDefinition::portal_url);
        let city = def.map_or(String::new(), |d| d.city.clone());
        let state = def.map_or(String::new(), |d| d.state.clone());

        sources.push(serde_json::json!({
            "id": sid,
            "name": source_name,
            "recordCount": record_count,
            "city": city,
            "state": state,
            "portalUrl": portal_url,
        }));
    }

    let cities: Vec<serde_json::Value> = all_cities
        .iter()
        .map(|(city, state)| serde_json::json!([city, state]))
        .collect();

    log::info!("Found {} distinct cities", cities.len());

    // Also query boundary summary counts from the boundaries DuckDB
    // for the /api/sources endpoint context
    let _ = boundaries_conn; // used for boundary outputs, not needed for metadata beyond sources

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
/// - `crime_categories` table: distinct category/subcategory/severity from data
///
/// This replaces all runtime `PostGIS` queries from the AI analytics tools.
///
/// # Errors
///
/// Returns an error if the source `DuckDB` export or output `DuckDB`
/// creation fails.
#[allow(clippy::too_many_lines)]
fn generate_analytics_db(
    args: &GenerateArgs,
    source_ids: &[String],
    boundaries_conn: &duckdb::Connection,
    dir: &Path,
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
        let duck = open_output_duckdb(&db_path)?;

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
                source_id VARCHAR NOT NULL,
                census_tract_geoid VARCHAR,
                census_place_geoid VARCHAR,
                neighborhood_id VARCHAR
            )",
        )?;
    }

    // Populate incidents from per-source DuckDB files
    let mut total_count: u64 = 0;
    let mut remaining = args.limit;

    for sid in source_ids {
        if remaining == Some(0) {
            break;
        }

        let source_name = resolve_source_name(sid);

        let conn = crime_map_database::source_db::open_by_id(sid)?;
        let mut last_rowid: i64 = 0;
        let mut source_total: u64 = 0;

        loop {
            if remaining == Some(0) {
                break;
            }

            #[allow(clippy::cast_sign_loss)]
            let batch_limit = match remaining {
                Some(r) => i64::try_from(r.min(BATCH_SIZE as u64))?,
                None => BATCH_SIZE,
            };

            let mut stmt = conn.prepare(
                "SELECT rowid,
                        source_incident_id, category, parent_category, severity,
                        longitude, latitude, occurred_at::TEXT as occurred_at_text,
                        description, block_address,
                        city, state, arrest_made, domestic, location_type,
                        census_tract_geoid, census_place_geoid, state_fips,
                        county_geoid, neighborhood_id
                 FROM incidents
                 WHERE has_coordinates = TRUE
                   AND longitude BETWEEN -180 AND 180
                   AND latitude BETWEEN -90 AND 90
                   AND rowid > ?
                 ORDER BY rowid ASC
                 LIMIT ?",
            )?;

            let mut rows = stmt.query(duckdb::params![last_rowid, batch_limit])?;

            let mut batch: Vec<IncidentRow> = Vec::new();
            while let Some(row) = rows.next()? {
                let rowid: i64 = row.get(0)?;
                last_rowid = rowid;

                batch.push(IncidentRow {
                    source_incident_id: row.get(1)?,
                    source_id: sid.clone(),
                    source_name: source_name.clone(),
                    category: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                    parent_category: row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                    severity: row.get::<_, Option<i16>>(4)?.unwrap_or(1).into(),
                    longitude: row.get(5)?,
                    latitude: row.get(6)?,
                    occurred_at: row.get(7)?,
                    description: row.get(8)?,
                    block_address: row.get(9)?,
                    city: row.get::<_, Option<String>>(10)?.unwrap_or_default(),
                    state: row.get::<_, Option<String>>(11)?.unwrap_or_default(),
                    arrest_made: row.get(12)?,
                    domestic: row.get(13)?,
                    location_type: row.get(14)?,
                    census_tract_geoid: row.get(15)?,
                    census_place_geoid: row.get(16)?,
                    state_fips: row.get(17)?,
                    county_geoid: row.get(18)?,
                    neighborhood_id: row.get(19)?,
                });
            }

            if batch.is_empty() {
                break;
            }

            #[allow(clippy::cast_possible_truncation)]
            let batch_len = batch.len() as u64;

            {
                let duck = open_output_duckdb(&db_path)?;
                duck.execute_batch("BEGIN TRANSACTION")?;

                let mut insert_stmt = duck.prepare(
                    "INSERT INTO incidents (occurred_at, city, state, category, subcategory,
                        severity, arrest_made, parent_category_id, category_id, source_id,
                        census_tract_geoid, census_place_geoid, neighborhood_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )?;

                for incident in &batch {
                    // Read pre-computed spatial attribution
                    let tract_geoid = incident.census_tract_geoid.clone();
                    let place_geoid = incident.census_place_geoid.clone();
                    let neighborhood_id = incident.neighborhood_id.clone();

                    let parent_category_id: Option<i32> = None;
                    let category_id: Option<i32> = None;

                    insert_stmt.execute(duckdb::params![
                        incident.occurred_at,
                        incident.city,
                        incident.state,
                        incident.parent_category,
                        incident.category,
                        incident.severity,
                        incident.arrest_made,
                        parent_category_id,
                        category_id,
                        incident.source_id,
                        tract_geoid,
                        place_geoid,
                        neighborhood_id,
                    ])?;
                }

                duck.execute_batch("COMMIT")?;
            }

            source_total += batch_len;
            if let Some(ref mut r) = remaining {
                *r = r.saturating_sub(batch_len);
            }

            progress.inc(batch_len);

            #[allow(clippy::cast_sign_loss)]
            let batch_limit_u64 = batch_limit as u64;
            if batch_len < batch_limit_u64 {
                break;
            }
        }

        total_count += source_total;
        log::info!("Inserted {source_total} rows from source '{sid}' into analytics DB...");
    }

    // Now populate reference tables from the boundaries DuckDB
    let duck = open_output_duckdb(&db_path)?;

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
        let mut src_stmt = boundaries_conn.prepare(
            "SELECT geoid, name, state_abbr, county_name, population, land_area_sq_mi
             FROM census_tracts ORDER BY geoid",
        )?;
        let mut src_rows = src_stmt.query([])?;

        let mut dst_stmt = duck.prepare(
            "INSERT INTO census_tracts (geoid, name, state_abbr, county_name, population, land_area_sq_mi)
             VALUES (?, ?, ?, ?, ?, ?)",
        )?;

        let mut count = 0u64;
        while let Some(row) = src_rows.next()? {
            let geoid: String = row.get(0)?;
            let name: Option<String> = row.get(1)?;
            let state_abbr: Option<String> = row.get(2)?;
            let county_name: Option<String> = row.get(3)?;
            let population: Option<i32> = row.get(4)?;
            let land_area: Option<f64> = row.get(5)?;
            dst_stmt.execute(duckdb::params![
                geoid,
                name,
                state_abbr,
                county_name,
                population,
                land_area
            ])?;
            count += 1;
        }
        log::info!("Inserted {count} census tracts");
    }

    // ── Neighborhoods reference table ──
    log::info!("Populating neighborhoods reference table...");
    duck.execute_batch(
        "CREATE TABLE neighborhoods (
            id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL
        )",
    )?;

    {
        let mut src_stmt =
            boundaries_conn.prepare("SELECT id, name FROM neighborhoods ORDER BY id")?;
        let mut src_rows = src_stmt.query([])?;

        let mut dst_stmt = duck.prepare("INSERT INTO neighborhoods (id, name) VALUES (?, ?)")?;

        let mut count = 0u64;
        while let Some(row) = src_rows.next()? {
            let id: i32 = row.get(0)?;
            let name: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
            let nbhd_id = format!("nbhd-{id}");
            dst_stmt.execute(duckdb::params![nbhd_id, name])?;
            count += 1;
        }
        log::info!("Inserted {count} neighborhoods");
    }

    // ── Tract-neighborhood mapping table ──
    log::info!("Populating tract_neighborhoods reference table...");
    duck.execute_batch(
        "CREATE TABLE tract_neighborhoods (
            geoid VARCHAR NOT NULL,
            neighborhood_id VARCHAR NOT NULL
        )",
    )?;

    {
        let mut src_stmt = boundaries_conn
            .prepare("SELECT geoid, neighborhood_id FROM tract_neighborhoods ORDER BY geoid")?;
        let mut src_rows = src_stmt.query([])?;

        let mut dst_stmt =
            duck.prepare("INSERT INTO tract_neighborhoods (geoid, neighborhood_id) VALUES (?, ?)")?;

        let mut count = 0u64;
        while let Some(row) = src_rows.next()? {
            let geoid: String = row.get(0)?;
            let nbhd_id: i32 = row.get(1)?;
            let nbhd_id_str = format!("nbhd-{nbhd_id}");
            dst_stmt.execute(duckdb::params![geoid, nbhd_id_str])?;
            count += 1;
        }
        log::info!("Inserted {count} tract-neighborhood mappings");
    }

    // ── Census places reference table ──
    log::info!("Populating census_places reference table...");
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
        let mut src_stmt = boundaries_conn.prepare(
            "SELECT geoid, name, full_name, state_abbr, place_type, population, land_area_sq_mi
             FROM census_places ORDER BY geoid",
        )?;
        let mut src_rows = src_stmt.query([])?;

        let mut dst_stmt = duck.prepare(
            "INSERT INTO census_places (geoid, name, full_name, state_abbr, place_type, population, land_area_sq_mi)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )?;

        let mut count = 0u64;
        while let Some(row) = src_rows.next()? {
            let geoid: String = row.get(0)?;
            let name: Option<String> = row.get(1)?;
            let full_name: Option<String> = row.get(2)?;
            let state_abbr: Option<String> = row.get(3)?;
            let place_type: Option<String> = row.get(4)?;
            let population: Option<i32> = row.get(5)?;
            let land_area: Option<f64> = row.get(6)?;
            dst_stmt.execute(duckdb::params![
                geoid, name, full_name, state_abbr, place_type, population, land_area
            ])?;
            count += 1;
        }
        log::info!("Inserted {count} census places");
    }

    // ── Crime categories reference table (derived from data) ──
    log::info!("Populating crime_categories reference table...");
    duck.execute_batch(
        "CREATE TABLE crime_categories (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            parent_id INTEGER,
            severity INTEGER
        )",
    )?;

    // Build categories from the distinct (subcategory, parent_category, severity)
    // tuples in the incidents table
    duck.execute_batch(
        "INSERT INTO crime_categories (id, name, parent_id, severity)
         WITH parents AS (
             SELECT DISTINCT category AS name
             FROM incidents
         ),
         numbered_parents AS (
             SELECT ROW_NUMBER() OVER (ORDER BY name) AS id, name
             FROM parents
         ),
         children AS (
             SELECT DISTINCT subcategory AS name, category AS parent_name, severity
             FROM incidents
         ),
         numbered_children AS (
             SELECT
                 (SELECT MAX(id) FROM numbered_parents) + ROW_NUMBER() OVER (ORDER BY c.name) AS id,
                 c.name,
                 np.id AS parent_id,
                 c.severity
             FROM children c
             JOIN numbered_parents np ON np.name = c.parent_name
         )
         SELECT id, name, NULL AS parent_id, NULL AS severity FROM numbered_parents
         UNION ALL
         SELECT id, name, parent_id, severity FROM numbered_children",
    )?;

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
/// support type-ahead boundary filtering without a live database.
///
/// # Errors
///
/// Returns an error if the boundaries `DuckDB` query or `SQLite` write fails.
#[allow(clippy::too_many_lines, clippy::future_not_send)]
async fn generate_boundaries_db(
    boundaries_conn: &duckdb::Connection,
    dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    use switchy_database::DatabaseValue;

    let db_path = dir.join("boundaries.db");

    // Remove existing file
    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
    }

    log::info!("Creating boundaries search SQLite database...");
    let sqlite = switchy_database_connection::init_sqlite_rusqlite(Some(&db_path))
        .map_err(|e| format!("Failed to open boundaries SQLite: {e}"))?;

    // WAL mode + generous busy timeout to avoid "database is locked" errors.
    sqlite.exec_raw("PRAGMA journal_mode=WAL").await?;
    sqlite.exec_raw("PRAGMA busy_timeout=5000").await?;

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
        .await
        .map_err(|e| format!("Failed to create boundaries table: {e}"))?;

    // States
    {
        let mut src_stmt = boundaries_conn
            .prepare("SELECT fips, name, abbr, population FROM census_states ORDER BY fips")?;
        let mut src_rows = src_stmt.query([])?;

        let tx = sqlite
            .begin_transaction()
            .await
            .map_err(|e| format!("Failed to begin transaction: {e}"))?;

        let mut count = 0u64;
        while let Some(row) = src_rows.next()? {
            let fips: String = row.get(0)?;
            let name: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
            let abbr: String = row.get::<_, Option<String>>(2)?.unwrap_or_default();
            let population: Option<i64> = row.get(3)?;
            tx.exec_raw_params(
                "INSERT INTO boundaries (type, geoid, name, full_name, state_abbr, population)
                 VALUES ('state', $1, $2, $3, $4, $5)",
                &[
                    DatabaseValue::String(fips),
                    DatabaseValue::String(name.clone()),
                    DatabaseValue::String(name),
                    DatabaseValue::String(abbr),
                    population.map_or(DatabaseValue::Null, DatabaseValue::Int64),
                ],
            )
            .await
            .map_err(|e| format!("Failed to insert state boundary: {e}"))?;
            count += 1;
        }
        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {e}"))?;
        log::info!("Inserted {count} state boundaries");
    }

    // Counties
    {
        let mut src_stmt = boundaries_conn.prepare(
            "SELECT geoid, name, full_name, state_abbr, population
             FROM census_counties ORDER BY geoid",
        )?;
        let mut src_rows = src_stmt.query([])?;

        let tx = sqlite
            .begin_transaction()
            .await
            .map_err(|e| format!("Failed to begin transaction: {e}"))?;

        let mut count = 0u64;
        while let Some(row) = src_rows.next()? {
            let geoid: String = row.get(0)?;
            let name: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
            let full_name: String = row.get::<_, Option<String>>(2)?.unwrap_or_default();
            let state_abbr: Option<String> = row.get(3)?;
            let population: Option<i32> = row.get(4)?;
            tx.exec_raw_params(
                "INSERT INTO boundaries (type, geoid, name, full_name, state_abbr, population)
                 VALUES ('county', $1, $2, $3, $4, $5)",
                &[
                    DatabaseValue::String(geoid),
                    DatabaseValue::String(name),
                    DatabaseValue::String(full_name),
                    state_abbr.map_or(DatabaseValue::Null, DatabaseValue::String),
                    population.map_or(DatabaseValue::Null, DatabaseValue::Int32),
                ],
            )
            .await
            .map_err(|e| format!("Failed to insert county boundary: {e}"))?;
            count += 1;
        }
        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {e}"))?;
        log::info!("Inserted {count} county boundaries");
    }

    // Places
    {
        let mut src_stmt = boundaries_conn.prepare(
            "SELECT geoid, name, full_name, state_abbr, population
             FROM census_places ORDER BY geoid",
        )?;
        let mut src_rows = src_stmt.query([])?;

        let tx = sqlite
            .begin_transaction()
            .await
            .map_err(|e| format!("Failed to begin transaction: {e}"))?;

        let mut count = 0u64;
        while let Some(row) = src_rows.next()? {
            let geoid: String = row.get(0)?;
            let name: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
            let full_name: String = row.get::<_, Option<String>>(2)?.unwrap_or_default();
            let state_abbr: Option<String> = row.get(3)?;
            let population: Option<i32> = row.get(4)?;
            tx.exec_raw_params(
                "INSERT INTO boundaries (type, geoid, name, full_name, state_abbr, population)
                 VALUES ('place', $1, $2, $3, $4, $5)",
                &[
                    DatabaseValue::String(geoid),
                    DatabaseValue::String(name),
                    DatabaseValue::String(full_name),
                    state_abbr.map_or(DatabaseValue::Null, DatabaseValue::String),
                    population.map_or(DatabaseValue::Null, DatabaseValue::Int32),
                ],
            )
            .await
            .map_err(|e| format!("Failed to insert place boundary: {e}"))?;
            count += 1;
        }
        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {e}"))?;
        log::info!("Inserted {count} place boundaries");
    }

    // Tracts
    {
        let mut src_stmt = boundaries_conn.prepare(
            "SELECT geoid, name, state_abbr, county_name, population
             FROM census_tracts ORDER BY geoid",
        )?;
        let mut src_rows = src_stmt.query([])?;

        let tx = sqlite
            .begin_transaction()
            .await
            .map_err(|e| format!("Failed to begin transaction: {e}"))?;

        let mut count = 0u64;
        while let Some(row) = src_rows.next()? {
            let geoid: String = row.get(0)?;
            let name: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
            let state_abbr: Option<String> = row.get(2)?;
            let county_name: Option<String> = row.get(3)?;
            let population: Option<i32> = row.get(4)?;
            let full_name = match (&county_name, &state_abbr) {
                (Some(c), Some(s)) => format!("Tract {name}, {c}, {s}"),
                (Some(c), None) => format!("Tract {name}, {c}"),
                (None, Some(s)) => format!("Tract {name}, {s}"),
                (None, None) => format!("Tract {name}"),
            };
            tx.exec_raw_params(
                "INSERT INTO boundaries (type, geoid, name, full_name, state_abbr, population)
                 VALUES ('tract', $1, $2, $3, $4, $5)",
                &[
                    DatabaseValue::String(geoid),
                    DatabaseValue::String(name),
                    DatabaseValue::String(full_name),
                    state_abbr.map_or(DatabaseValue::Null, DatabaseValue::String),
                    population.map_or(DatabaseValue::Null, DatabaseValue::Int32),
                ],
            )
            .await
            .map_err(|e| format!("Failed to insert tract boundary: {e}"))?;
            count += 1;
        }
        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {e}"))?;
        log::info!("Inserted {count} tract boundaries");
    }

    // Neighborhoods
    {
        let mut src_stmt = boundaries_conn
            .prepare("SELECT id, name, city, state FROM neighborhoods ORDER BY id")?;
        let mut src_rows = src_stmt.query([])?;

        let tx = sqlite
            .begin_transaction()
            .await
            .map_err(|e| format!("Failed to begin transaction: {e}"))?;

        let mut count = 0u64;
        while let Some(row) = src_rows.next()? {
            let id: i32 = row.get(0)?;
            let name: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
            let city: String = row.get::<_, Option<String>>(2)?.unwrap_or_default();
            let state: String = row.get::<_, Option<String>>(3)?.unwrap_or_default();
            let geoid = format!("nbhd-{id}");
            let full_name = format!("{name}, {city}, {state}");
            tx.exec_raw_params(
                "INSERT INTO boundaries (type, geoid, name, full_name, state_abbr, population)
                 VALUES ('neighborhood', $1, $2, $3, $4, NULL)",
                &[
                    DatabaseValue::String(geoid),
                    DatabaseValue::String(name),
                    DatabaseValue::String(full_name),
                    DatabaseValue::String(state),
                ],
            )
            .await
            .map_err(|e| format!("Failed to insert neighborhood boundary: {e}"))?;
            count += 1;
        }
        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {e}"))?;
        log::info!("Inserted {count} neighborhood boundaries");
    }

    // Create search index
    sqlite
        .exec_raw("CREATE INDEX idx_boundaries_name ON boundaries(type, name COLLATE NOCASE)")
        .await
        .map_err(|e| format!("Failed to create index: {e}"))?;
    sqlite
        .exec_raw("ANALYZE")
        .await
        .map_err(|e| format!("Failed to run ANALYZE: {e}"))?;

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
/// polygons from the boundaries `DuckDB`.
///
/// Exports 5 `GeoJSONSeq` files (states, counties, places, tracts,
/// neighborhoods), then runs tippecanoe with multiple named layers
/// to produce a single `PMTiles` archive.
///
/// # Errors
///
/// Returns an error if any export or tippecanoe invocation fails.
fn generate_boundaries_pmtiles(
    boundaries_conn: &duckdb::Connection,
    dir: &Path,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<(), Box<dyn std::error::Error>> {
    log::info!("Exporting boundary layers to GeoJSONSeq...");

    export_boundary_layer(boundaries_conn, dir, "states", progress)?;
    export_boundary_layer(boundaries_conn, dir, "counties", progress)?;
    export_boundary_layer(boundaries_conn, dir, "places", progress)?;
    export_boundary_layer(boundaries_conn, dir, "tracts", progress)?;
    export_boundary_layer(boundaries_conn, dir, "neighborhoods", progress)?;

    log::info!("Running tippecanoe to generate boundaries PMTiles...");

    let output_path = dir.join("boundaries.pmtiles");
    let mut cmd = Command::new("tippecanoe");
    cmd.args([
        "-o",
        &*output_path.to_string_lossy(),
        "--force",
        "--no-feature-limit",
        "--no-tile-size-limit",
        "--minimum-zoom=0",
        "--maximum-zoom=14",
        "--coalesce-densest-as-needed",
        "--detect-shared-borders",
    ]);

    if std::env::var("CI").is_ok() {
        cmd.arg("--quiet");
    }

    // Add each layer as a named-layer with its GeoJSONSeq file
    let mut has_layers = false;
    for &(layer_name, filename) in BOUNDARY_LAYERS {
        let layer_path = dir.join(filename);
        let non_empty = layer_path.exists()
            && std::fs::metadata(&layer_path)
                .map(|m| m.len() > 0)
                .unwrap_or(false);
        if non_empty {
            let arg = format!(
                "--named-layer={layer_name}:{}",
                layer_path.to_string_lossy()
            );
            cmd.arg(arg);
            has_layers = true;
        }
    }

    // Skip tippecanoe if all layer files are empty
    if !has_layers {
        log::warn!("No boundary features to tile; skipping boundaries PMTiles generation");
        // Clean up empty layer files
        for &(_, filename) in BOUNDARY_LAYERS {
            let path = dir.join(filename);
            std::fs::remove_file(&path).ok();
        }
        return Ok(());
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

/// Exports a single boundary layer from the boundaries `DuckDB` as
/// `GeoJSONSeq`.
///
/// Each feature is a polygon/multipolygon with name/identifier properties.
#[allow(clippy::too_many_lines)]
fn export_boundary_layer(
    boundaries_conn: &duckdb::Connection,
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
                    boundary_geojson as geojson
             FROM census_states
             WHERE boundary_geojson IS NOT NULL
             ORDER BY fips"
        }
        "counties" => {
            "SELECT geoid, name, full_name, state_fips, state_abbr,
                    county_fips, population, land_area_sq_mi,
                    boundary_geojson as geojson
             FROM census_counties
             WHERE boundary_geojson IS NOT NULL
             ORDER BY geoid"
        }
        "places" => {
            "SELECT geoid, name, full_name, state_fips, state_abbr,
                    place_type, population, land_area_sq_mi,
                    boundary_geojson as geojson
             FROM census_places
             WHERE boundary_geojson IS NOT NULL
             ORDER BY geoid"
        }
        "tracts" => {
            "SELECT geoid, name, state_fips, county_fips, state_abbr,
                    county_name, population, land_area_sq_mi,
                    boundary_geojson as geojson
             FROM census_tracts
             WHERE boundary_geojson IS NOT NULL
             ORDER BY geoid"
        }
        "neighborhoods" => {
            "SELECT id, name, city, state,
                    boundary_geojson as geojson
             FROM neighborhoods
             WHERE boundary_geojson IS NOT NULL
             ORDER BY id"
        }
        _ => return Err(format!("Unknown boundary layer: {layer}").into()),
    };

    // Check row count first to avoid DuckDB type-inference edge cases on
    // empty tables (can trigger InvalidColumnType errors in the Rust crate).
    let count_query = match layer {
        "states" => "SELECT COUNT(*) FROM census_states WHERE boundary_geojson IS NOT NULL",
        "counties" => "SELECT COUNT(*) FROM census_counties WHERE boundary_geojson IS NOT NULL",
        "places" => "SELECT COUNT(*) FROM census_places WHERE boundary_geojson IS NOT NULL",
        "tracts" => "SELECT COUNT(*) FROM census_tracts WHERE boundary_geojson IS NOT NULL",
        "neighborhoods" => "SELECT COUNT(*) FROM neighborhoods WHERE boundary_geojson IS NOT NULL",
        _ => return Err(format!("Unknown boundary layer: {layer}").into()),
    };
    let total: u64 = boundaries_conn.query_row(count_query, [], |row| row.get(0))?;

    if total == 0 {
        progress.inc(0);
        log::info!("Exported 0 {layer} boundary features to {filename} (table empty)");
        return Ok(());
    }

    let mut stmt = boundaries_conn.prepare(query)?;
    let mut rows = stmt.query([])?;

    let mut count = 0u64;

    while let Some(row) = rows.next()? {
        let geojson_str: String = match layer {
            "states" => row.get::<_, Option<String>>(5)?.unwrap_or_default(),
            "counties" | "places" | "tracts" => {
                row.get::<_, Option<String>>(8)?.unwrap_or_default()
            }
            "neighborhoods" => row.get::<_, Option<String>>(4)?.unwrap_or_default(),
            _ => String::new(),
        };

        if geojson_str.is_empty() {
            continue;
        }

        let geometry: serde_json::Value = serde_json::from_str(&geojson_str)?;

        let properties = match layer {
            "states" => {
                let fips: String = row.get(0)?;
                let name: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
                let abbr: String = row.get::<_, Option<String>>(2)?.unwrap_or_default();
                let population: Option<i64> = row.get(3)?;
                let land_area: Option<f64> = row.get(4)?;
                serde_json::json!({
                    "name": name,
                    "abbr": abbr,
                    "fips": fips,
                    "population": population,
                    "land_area_sq_mi": land_area,
                })
            }
            "counties" => {
                let geoid: String = row.get(0)?;
                let name: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
                let full_name: String = row.get::<_, Option<String>>(2)?.unwrap_or_default();
                let state_abbr: Option<String> = row.get(4)?;
                let population: Option<i32> = row.get(6)?;
                let land_area: Option<f64> = row.get(7)?;
                serde_json::json!({
                    "name": name,
                    "full_name": full_name,
                    "geoid": geoid,
                    "state": state_abbr,
                    "population": population,
                    "land_area_sq_mi": land_area,
                })
            }
            "places" => {
                let geoid: String = row.get(0)?;
                let name: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
                let full_name: String = row.get::<_, Option<String>>(2)?.unwrap_or_default();
                let state_abbr: Option<String> = row.get(4)?;
                let place_type: String = row.get::<_, Option<String>>(5)?.unwrap_or_default();
                let population: Option<i32> = row.get(6)?;
                let land_area: Option<f64> = row.get(7)?;
                serde_json::json!({
                    "name": name,
                    "full_name": full_name,
                    "geoid": geoid,
                    "state": state_abbr,
                    "type": place_type,
                    "population": population,
                    "land_area_sq_mi": land_area,
                })
            }
            "tracts" => {
                let geoid: String = row.get(0)?;
                let name: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
                let state_abbr: Option<String> = row.get(4)?;
                let county_name: Option<String> = row.get(5)?;
                let population: Option<i32> = row.get(6)?;
                let land_area: Option<f64> = row.get(7)?;
                serde_json::json!({
                    "name": name,
                    "geoid": geoid,
                    "state": state_abbr,
                    "county": county_name,
                    "population": population,
                    "land_area_sq_mi": land_area,
                })
            }
            "neighborhoods" => {
                let id: i64 = row.get(0)?;
                let name: String = row.get::<_, Option<String>>(1)?.unwrap_or_default();
                let city: String = row.get::<_, Option<String>>(2)?.unwrap_or_default();
                let state: String = row.get::<_, Option<String>>(3)?.unwrap_or_default();
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
