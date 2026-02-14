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

    // Run each output that needs it
    if needs.get(OUTPUT_INCIDENTS_PMTILES) == Some(&true) {
        progress.set_message("Generating PMTiles...".to_string());
        progress.set_total(total_records);
        progress.set_position(0);
        generate_pmtiles(db, args, source_ids, dir, &progress).await?;
        record_output(manifest, OUTPUT_INCIDENTS_PMTILES);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_INCIDENTS_DB) == Some(&true) {
        progress.set_message("Generating sidebar DB...".to_string());
        progress.set_total(total_records);
        progress.set_position(0);
        generate_sidebar_db(db, args, source_ids, dir, &progress).await?;
        record_output(manifest, OUTPUT_INCIDENTS_DB);
        save_manifest(dir, manifest)?;
    }

    if needs.get(OUTPUT_COUNT_DB) == Some(&true) {
        progress.set_message("Generating count DB...".to_string());
        progress.set_total(total_records);
        progress.set_position(0);
        generate_count_db(db, args, source_ids, dir, &progress).await?;
        record_output(manifest, OUTPUT_COUNT_DB);
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

/// Resolves `--sources` short IDs (e.g., "chicago") to database integer
/// `source_id` values by looking up each source's human-readable name in
/// `crime_sources`.
///
/// Returns an empty `Vec` if `--sources` was not provided (meaning: export
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
    let Some(ref sources_str) = args.sources else {
        return Ok(Vec::new());
    };

    let requested: Vec<&str> = sources_str.split(',').map(str::trim).collect();
    let registry = all_sources();

    let mut source_ids = Vec::with_capacity(requested.len());
    for short_id in &requested {
        let Some(def) = registry.iter().find(|s| s.id() == *short_id) else {
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
    progress: &Arc<dyn ProgressCallback>,
) -> Result<(), Box<dyn std::error::Error>> {
    let geojsonseq_path = dir.join("incidents.geojsonseq");

    log::info!("Exporting incidents to GeoJSONSeq...");
    export_geojsonseq(db, &geojsonseq_path, args.limit, source_ids, progress).await?;

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
) -> Result<i64, Box<dyn std::error::Error>> {
    let id: i64 = row.to_value("id").unwrap_or(0);
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

    let occurred_at_naive: chrono::NaiveDateTime = row.to_value("occurred_at").unwrap_or_default();
    let occurred_at =
        chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(occurred_at_naive, chrono::Utc)
            .to_rfc3339();

    let arrest_int = arrest_made.map(|b| DatabaseValue::Int32(i32::from(b)));

    txn.exec_raw_params(
        "INSERT INTO incidents (id, source_incident_id, subcategory, category,
            severity, longitude, latitude, occurred_at, description,
            block_address, city, state, arrest_made, location_type)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
        &[
            DatabaseValue::Int64(id),
            DatabaseValue::String(source_incident_id),
            DatabaseValue::String(subcategory),
            DatabaseValue::String(category),
            DatabaseValue::Int32(severity),
            DatabaseValue::Real64(lng),
            DatabaseValue::Real64(lat),
            DatabaseValue::String(occurred_at),
            description.map_or(DatabaseValue::Null, DatabaseValue::String),
            block_address.map_or(DatabaseValue::Null, DatabaseValue::String),
            DatabaseValue::String(city),
            DatabaseValue::String(state),
            arrest_int.unwrap_or(DatabaseValue::Null),
            location_type.map_or(DatabaseValue::Null, DatabaseValue::String),
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
async fn generate_sidebar_db(
    db: &dyn Database,
    args: &GenerateArgs,
    source_ids: &[i32],
    dir: &Path,
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
                source_incident_id TEXT,
                subcategory TEXT NOT NULL,
                category TEXT NOT NULL,
                severity INTEGER NOT NULL,
                longitude REAL NOT NULL,
                latitude REAL NOT NULL,
                occurred_at TEXT NOT NULL,
                description TEXT,
                block_address TEXT,
                city TEXT,
                state TEXT,
                arrest_made INTEGER,
                location_type TEXT
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
            last_id = insert_sidebar_row(txn.as_ref(), row).await?;
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
        "SELECT i.id, i.source_incident_id,
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
            subcategory VARCHAR NOT NULL,
            severity INTEGER NOT NULL,
            longitude DOUBLE NOT NULL,
            latitude DOUBLE NOT NULL,
            occurred_at VARCHAR NOT NULL,
            arrest_made INTEGER,
            category VARCHAR NOT NULL
        )",
    )?;

    // Drop the connection before the async loop; we'll reopen per batch
    drop(duck);

    let total_count = populate_duckdb_incidents(db, args, source_ids, &db_path, progress).await?;

    // Reopen for aggregation
    let duck = duckdb::Connection::open(&db_path)?;

    // Create pre-aggregated count summary table
    log::info!("Creating count_summary aggregation table...");
    duck.execute_batch(
        "CREATE TABLE count_summary AS
         SELECT
             CAST(FLOOR(longitude * 1000) AS INTEGER) AS cell_lng,
             CAST(FLOOR(latitude * 1000) AS INTEGER) AS cell_lat,
             subcategory,
             category,
             severity,
             CASE WHEN arrest_made = 1 THEN 1
                  WHEN arrest_made = 0 THEN 0
                  ELSE 2 END AS arrest,
             SUBSTRING(occurred_at, 1, 10) AS day,
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
                "INSERT INTO incidents (id, subcategory, severity, longitude, latitude,
                    occurred_at, arrest_made, category)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )?;

            for row in &rows {
                last_id = insert_duckdb_row(&mut stmt, row)?;
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
) -> Result<i64, Box<dyn std::error::Error>> {
    let id: i64 = row.to_value("id").unwrap_or(0);
    let subcategory: String = row.to_value("subcategory").unwrap_or_default();
    let category: String = row.to_value("category").unwrap_or_default();
    let severity: i32 = row.to_value("severity").unwrap_or(1);
    let lng: f64 = row.to_value("longitude").unwrap_or(0.0);
    let lat: f64 = row.to_value("latitude").unwrap_or(0.0);
    let arrest_made: Option<bool> = row.to_value("arrest_made").unwrap_or(None);

    let occurred_at_naive: chrono::NaiveDateTime = row.to_value("occurred_at").unwrap_or_default();
    let occurred_at_str = occurred_at_naive.format("%Y-%m-%d %H:%M:%S").to_string();

    let arrest_int: Option<i32> = arrest_made.map(i32::from);

    stmt.execute(duckdb::params![
        id,
        subcategory,
        severity,
        lng,
        lat,
        occurred_at_str,
        arrest_int,
        category,
    ])?;

    Ok(id)
}

/// Exports all incidents from `PostGIS` as newline-delimited `GeoJSON`,
/// using keyset pagination and streaming writes to keep memory constant.
async fn export_geojsonseq(
    db: &dyn Database,
    output_path: &Path,
    limit: Option<u64>,
    source_ids: &[i32],
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

            let occurred_at_naive: chrono::NaiveDateTime =
                row.to_value("occurred_at").unwrap_or_default();
            let occurred_at = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                occurred_at_naive,
                chrono::Utc,
            )
            .to_rfc3339();

            let feature = serde_json::json!({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lng, lat]
                },
                "properties": {
                    "id": id,
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
