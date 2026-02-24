//! Merge partitioned generation artifacts into unified output files.
//!
//! Each partition directory is expected to contain a subset of the standard
//! generation outputs (produced by `cargo generate all --sources=X
//! --output-dir=<partition>`). This module combines them into a single set
//! of artifacts suitable for serving.
//!
//! ## Merge strategies
//!
//! | Artifact | Strategy |
//! |----------|----------|
//! | `incidents.pmtiles` | `tile-join` from the tippecanoe suite |
//! | `incidents.db` | SQLite `ATTACH` + `INSERT` with auto-assigned IDs, R-tree rebuild |
//! | `counts.duckdb` | DuckDB `ATTACH` + `INSERT INTO ... SELECT` (UNION ALL) |
//! | `h3.duckdb` | DuckDB `ATTACH` + `INSERT INTO ... SELECT`, deduplicate `h3_boundaries` |
//! | `analytics.duckdb` | DuckDB `ATTACH` + UNION ALL incidents, copy reference tables from first partition |
//! | `metadata.json` | JSON merge: union cities, union sources, MIN/MAX dates |
//! | `boundaries.pmtiles` | Copy from `--boundaries-dir` |
//! | `boundaries.db` | Copy from `--boundaries-dir` |

use std::path::{Path, PathBuf};
use std::process::Command;

use switchy_database_connection::init_sqlite_rusqlite;

/// Runs the full merge pipeline.
///
/// # Errors
///
/// Returns an error if any merge step fails (missing files, subprocess
/// errors, database errors, etc.).
pub async fn run(
    partition_dirs: &[PathBuf],
    boundaries_dir: Option<&Path>,
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    log::info!(
        "Merging {} partitions into {}",
        partition_dirs.len(),
        output_dir.display()
    );

    // Validate that at least one partition exists
    if partition_dirs.is_empty() {
        return Err("No partition directories provided".into());
    }

    for dir in partition_dirs {
        if !dir.exists() {
            return Err(format!("Partition directory does not exist: {}", dir.display()).into());
        }
    }

    // Merge each artifact type
    merge_pmtiles(partition_dirs, output_dir)?;
    merge_sidebar_db(partition_dirs, output_dir).await?;
    merge_count_db(partition_dirs, output_dir)?;
    merge_h3_db(partition_dirs, output_dir)?;
    merge_analytics_db(partition_dirs, output_dir)?;
    merge_metadata(partition_dirs, output_dir)?;

    // Copy boundary artifacts if provided
    if let Some(bdir) = boundaries_dir {
        copy_boundary_artifacts(bdir, output_dir)?;
    } else {
        log::info!("No --boundaries-dir provided; skipping boundary artifact copy");
    }

    log::info!("Merge complete: {}", output_dir.display());
    Ok(())
}

// ============================================================
// PMTiles merge via tile-join
// ============================================================

/// Merges `incidents.pmtiles` from all partitions using `tile-join`.
fn merge_pmtiles(
    partition_dirs: &[PathBuf],
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let inputs: Vec<PathBuf> = partition_dirs
        .iter()
        .map(|d| d.join("incidents.pmtiles"))
        .filter(|p| p.exists())
        .collect();

    if inputs.is_empty() {
        log::warn!("No incidents.pmtiles files found in any partition; skipping PMTiles merge");
        return Ok(());
    }

    let output_path = output_dir.join("incidents.pmtiles");
    log::info!("Merging {} PMTiles files via tile-join...", inputs.len());

    let mut cmd = Command::new("tile-join");
    cmd.arg("-f")
        .arg("--no-tile-size-limit")
        .arg("-o")
        .arg(&output_path);

    if std::env::var("CI").is_ok() {
        cmd.arg("--quiet");
    }

    for input in &inputs {
        cmd.arg(input);
    }

    let status = cmd
        .status()
        .map_err(|e| format!("Failed to run tile-join (is tippecanoe installed?): {e}"))?;

    if !status.success() {
        return Err(format!("tile-join exited with status {status}").into());
    }

    log::info!("PMTiles merge complete: {}", output_path.display());
    Ok(())
}

// ============================================================
// SQLite incidents.db merge
// ============================================================

/// Merges `incidents.db` from all partitions into a single `SQLite` database.
///
/// Uses `ATTACH DATABASE` to read from each partition and inserts rows
/// with auto-assigned IDs (the partition-local IDs are discarded). The
/// R-tree spatial index and all secondary indexes are rebuilt after all
/// data is inserted.
#[allow(clippy::too_many_lines)]
async fn merge_sidebar_db(
    partition_dirs: &[PathBuf],
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let inputs: Vec<PathBuf> = partition_dirs
        .iter()
        .map(|d| d.join("incidents.db"))
        .filter(|p| p.exists())
        .collect();

    if inputs.is_empty() {
        log::warn!("No incidents.db files found in any partition; skipping sidebar merge");
        return Ok(());
    }

    let output_path = output_dir.join("incidents.db");
    if output_path.exists() {
        std::fs::remove_file(&output_path)?;
    }

    log::info!("Merging {} incidents.db files...", inputs.len());
    let sqlite = init_sqlite_rusqlite(Some(&output_path))?;

    // Create the schema (matches generate_sidebar_db in lib.rs)
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

    // Import from each partition
    for (i, input) in inputs.iter().enumerate() {
        let alias = format!("p{i}");
        let path_str = input.to_string_lossy();

        sqlite
            .exec_raw(&format!("ATTACH DATABASE '{path_str}' AS {alias}"))
            .await?;

        sqlite
            .exec_raw(&format!(
                "INSERT INTO incidents (
                    source_id, source_name, source_incident_id,
                    subcategory, category, severity,
                    longitude, latitude, occurred_at,
                    description, block_address, city, state,
                    arrest_made, location_type,
                    state_fips, county_geoid, place_geoid,
                    tract_geoid, neighborhood_id
                )
                SELECT
                    source_id, source_name, source_incident_id,
                    subcategory, category, severity,
                    longitude, latitude, occurred_at,
                    description, block_address, city, state,
                    arrest_made, location_type,
                    state_fips, county_geoid, place_geoid,
                    tract_geoid, neighborhood_id
                FROM {alias}.incidents"
            ))
            .await?;

        log::info!("  Partition {}: merged from {}", i + 1, input.display());

        sqlite.exec_raw(&format!("DETACH {alias}")).await?;
    }

    // Build R-tree spatial index
    log::info!("Building R-tree spatial index...");
    sqlite
        .exec_raw(
            "CREATE VIRTUAL TABLE incidents_rtree USING rtree(
                id, min_lng, max_lng, min_lat, max_lat
            )",
        )
        .await?;

    sqlite
        .exec_raw(
            "INSERT INTO incidents_rtree (id, min_lng, max_lng, min_lat, max_lat)
             SELECT id, longitude, longitude, latitude, latitude FROM incidents",
        )
        .await?;

    // Build secondary indexes
    log::info!("Building secondary indexes...");
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_occurred_at ON incidents(occurred_at DESC)")
        .await?;
    sqlite
        .exec_raw("CREATE INDEX idx_incidents_source_id ON incidents(source_id)")
        .await?;
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

    sqlite.exec_raw("ANALYZE").await?;

    log::info!("Sidebar merge complete: {}", output_path.display());
    Ok(())
}

// ============================================================
// DuckDB counts.duckdb merge
// ============================================================

/// Merges `counts.duckdb` from all partitions.
///
/// Each partition's `count_summary` table is already pre-aggregated by
/// `source_id`, so a simple `UNION ALL` produces correct results without
/// re-aggregation.
fn merge_count_db(
    partition_dirs: &[PathBuf],
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let inputs: Vec<PathBuf> = partition_dirs
        .iter()
        .map(|d| d.join("counts.duckdb"))
        .filter(|p| p.exists())
        .collect();

    if inputs.is_empty() {
        log::warn!("No counts.duckdb files found in any partition; skipping count merge");
        return Ok(());
    }

    let output_path = output_dir.join("counts.duckdb");
    // Remove existing file + WAL
    for ext in &["", ".wal"] {
        let p = output_dir.join(format!("counts.duckdb{ext}"));
        if p.exists() {
            std::fs::remove_file(&p)?;
        }
    }

    log::info!("Merging {} counts.duckdb files...", inputs.len());
    let duck = duckdb::Connection::open(&output_path)?;

    // Attach all partitions and build UNION ALL
    let mut union_parts = Vec::with_capacity(inputs.len());
    for (i, input) in inputs.iter().enumerate() {
        let alias = format!("p{i}");
        let path_str = input.to_string_lossy();
        duck.execute_batch(&format!("ATTACH '{path_str}' AS {alias} (READ_ONLY)"))?;
        union_parts.push(format!("SELECT * FROM {alias}.count_summary"));
    }

    let union_query = union_parts.join(" UNION ALL ");
    duck.execute_batch(&format!("CREATE TABLE count_summary AS {union_query}"))?;

    // Create index
    duck.execute_batch("CREATE INDEX idx_count_summary_cell ON count_summary(cell_lng, cell_lat)")?;

    // Detach all
    for i in 0..inputs.len() {
        duck.execute_batch(&format!("DETACH p{i}"))?;
    }

    log::info!("Count merge complete: {}", output_path.display());
    Ok(())
}

// ============================================================
// DuckDB h3.duckdb merge
// ============================================================

/// Merges `h3.duckdb` from all partitions.
///
/// `h3_counts` rows are unioned directly. `h3_boundaries` rows are
/// deduplicated by `(h3_index, resolution)` since hex cells near state
/// borders may appear in multiple partitions.
fn merge_h3_db(
    partition_dirs: &[PathBuf],
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let inputs: Vec<PathBuf> = partition_dirs
        .iter()
        .map(|d| d.join("h3.duckdb"))
        .filter(|p| p.exists())
        .collect();

    if inputs.is_empty() {
        log::warn!("No h3.duckdb files found in any partition; skipping H3 merge");
        return Ok(());
    }

    let output_path = output_dir.join("h3.duckdb");
    // Remove existing file + WAL
    for ext in &["", ".wal"] {
        let p = output_dir.join(format!("h3.duckdb{ext}"));
        if p.exists() {
            std::fs::remove_file(&p)?;
        }
    }

    log::info!("Merging {} h3.duckdb files...", inputs.len());
    let duck = duckdb::Connection::open(&output_path)?;

    // Attach all partitions
    for (i, input) in inputs.iter().enumerate() {
        let alias = format!("p{i}");
        let path_str = input.to_string_lossy();
        duck.execute_batch(&format!("ATTACH '{path_str}' AS {alias} (READ_ONLY)"))?;
    }

    // UNION ALL h3_counts
    let counts_union: Vec<String> = (0..inputs.len())
        .map(|i| format!("SELECT * FROM p{i}.h3_counts"))
        .collect();

    duck.execute_batch(&format!(
        "CREATE TABLE h3_counts AS {}",
        counts_union.join(" UNION ALL ")
    ))?;

    // Create index on h3_counts
    duck.execute_batch("CREATE INDEX idx_h3_counts ON h3_counts(resolution, h3_index)")?;

    // UNION ALL h3_boundaries with deduplication
    let boundaries_union: Vec<String> = (0..inputs.len())
        .map(|i| format!("SELECT * FROM p{i}.h3_boundaries"))
        .collect();

    duck.execute_batch(&format!(
        "CREATE TABLE h3_boundaries AS
         SELECT DISTINCT ON (h3_index, resolution) *
         FROM ({})
         ORDER BY h3_index, resolution",
        boundaries_union.join(" UNION ALL ")
    ))?;

    // Detach all
    for i in 0..inputs.len() {
        duck.execute_batch(&format!("DETACH p{i}"))?;
    }

    log::info!("H3 merge complete: {}", output_path.display());
    Ok(())
}

// ============================================================
// DuckDB analytics.duckdb merge
// ============================================================

/// Reference tables in `analytics.duckdb` that are identical across partitions.
///
/// These are populated from the shared `PostGIS` census/boundary data, so
/// every partition produces the same rows. We copy them from the first
/// partition that has the table.
const ANALYTICS_REFERENCE_TABLES: &[&str] = &[
    "census_tracts",
    "neighborhoods",
    "tract_neighborhoods",
    "census_places",
    "crime_categories",
];

/// Merges `analytics.duckdb` from all partitions.
///
/// The `incidents` table is unioned from all partitions (each partition
/// has a disjoint set of source data). Reference tables (`census_tracts`,
/// `neighborhoods`, etc.) are identical across partitions and are copied
/// from the first partition.
#[allow(clippy::too_many_lines)]
fn merge_analytics_db(
    partition_dirs: &[PathBuf],
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let inputs: Vec<PathBuf> = partition_dirs
        .iter()
        .map(|d| d.join("analytics.duckdb"))
        .filter(|p| p.exists())
        .collect();

    if inputs.is_empty() {
        log::warn!("No analytics.duckdb files found in any partition; skipping analytics merge");
        return Ok(());
    }

    let output_path = output_dir.join("analytics.duckdb");
    // Remove existing file + WAL
    for ext in &["", ".wal"] {
        let p = output_dir.join(format!("analytics.duckdb{ext}"));
        if p.exists() {
            std::fs::remove_file(&p)?;
        }
    }

    log::info!("Merging {} analytics.duckdb files...", inputs.len());
    let duck = duckdb::Connection::open(&output_path)?;

    // Attach all partitions
    for (i, input) in inputs.iter().enumerate() {
        let alias = format!("p{i}");
        let path_str = input.to_string_lossy();
        duck.execute_batch(&format!("ATTACH '{path_str}' AS {alias} (READ_ONLY)"))?;
    }

    // UNION ALL incidents from all partitions
    let incidents_union: Vec<String> = (0..inputs.len())
        .map(|i| format!("SELECT * FROM p{i}.incidents"))
        .collect();

    duck.execute_batch(&format!(
        "CREATE TABLE incidents AS {}",
        incidents_union.join(" UNION ALL ")
    ))?;

    log::info!("  Merged incidents from {} partitions", inputs.len());

    // Create indexes on the merged incidents table
    duck.execute_batch(
        "CREATE INDEX idx_analytics_city ON incidents (city);
         CREATE INDEX idx_analytics_state ON incidents (state);
         CREATE INDEX idx_analytics_occurred_at ON incidents (occurred_at);
         CREATE INDEX idx_analytics_category ON incidents (category);
         CREATE INDEX idx_analytics_place_geoid ON incidents (census_place_geoid);
         CREATE INDEX idx_analytics_tract_geoid ON incidents (census_tract_geoid);
         CREATE INDEX idx_analytics_neighborhood_id ON incidents (neighborhood_id)",
    )?;

    // Copy reference tables from the first partition (they're identical across all)
    for &table in ANALYTICS_REFERENCE_TABLES {
        duck.execute_batch(&format!("CREATE TABLE {table} AS SELECT * FROM p0.{table}"))?;
        log::info!("  Copied reference table: {table}");
    }

    // Detach all
    for i in 0..inputs.len() {
        duck.execute_batch(&format!("DETACH p{i}"))?;
    }

    log::info!("Analytics merge complete: {}", output_path.display());
    Ok(())
}

// ============================================================
// metadata.json merge
// ============================================================

/// Merges `metadata.json` from all partitions.
///
/// Unions city arrays (deduplicated and sorted), unions source arrays
/// (deduplicated by `id`), takes the MIN of all `minDate` values and the
/// MAX of all `maxDate` values.
fn merge_metadata(
    partition_dirs: &[PathBuf],
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let inputs: Vec<PathBuf> = partition_dirs
        .iter()
        .map(|d| d.join("metadata.json"))
        .filter(|p| p.exists())
        .collect();

    if inputs.is_empty() {
        log::warn!("No metadata.json files found in any partition; skipping metadata merge");
        return Ok(());
    }

    log::info!("Merging {} metadata.json files...", inputs.len());

    let mut all_cities: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut all_sources: std::collections::BTreeMap<i64, serde_json::Value> =
        std::collections::BTreeMap::new();
    let mut min_date: Option<String> = None;
    let mut max_date: Option<String> = None;

    for input in &inputs {
        let content = std::fs::read_to_string(input)?;
        let meta: serde_json::Value = serde_json::from_str(&content)?;

        // Collect cities
        if let Some(cities) = meta.get("cities").and_then(|c| c.as_array()) {
            for city in cities {
                all_cities.insert(city.to_string());
            }
        }

        // Collect sources (deduplicate by id)
        if let Some(sources) = meta.get("sources").and_then(|s| s.as_array()) {
            for source in sources {
                if let Some(id) = source.get("id").and_then(serde_json::Value::as_i64) {
                    all_sources.entry(id).or_insert_with(|| source.clone());
                }
            }
        }

        // Track min/max dates
        if let Some(d) = meta.get("minDate").and_then(|d| d.as_str()) {
            min_date = Some(match min_date {
                Some(ref cur) if cur.as_str() <= d => cur.clone(),
                _ => d.to_string(),
            });
        }
        if let Some(d) = meta.get("maxDate").and_then(|d| d.as_str()) {
            max_date = Some(match max_date {
                Some(ref cur) if cur.as_str() >= d => cur.clone(),
                _ => d.to_string(),
            });
        }
    }

    // Reconstruct city arrays from deduplicated set
    let cities: Vec<serde_json::Value> = all_cities
        .into_iter()
        .filter_map(|s| serde_json::from_str(&s).ok())
        .collect();

    // Collect sources sorted by id
    let sources: Vec<serde_json::Value> = all_sources.into_values().collect();

    let merged = serde_json::json!({
        "cities": cities,
        "minDate": min_date,
        "maxDate": max_date,
        "sources": sources,
    });

    let path = output_dir.join("metadata.json");
    let tmp = output_dir.join("metadata.json.tmp");
    std::fs::write(&tmp, serde_json::to_string_pretty(&merged)?)?;
    std::fs::rename(&tmp, &path)?;

    log::info!("Metadata merge complete: {}", path.display());
    Ok(())
}

// ============================================================
// Boundary artifact copy
// ============================================================

/// Copies pre-generated boundary artifacts (`boundaries.pmtiles` and
/// `boundaries.db`) from the boundaries directory into the output
/// directory.
fn copy_boundary_artifacts(
    boundaries_dir: &Path,
    output_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    for filename in &["boundaries.pmtiles", "boundaries.db"] {
        let src = boundaries_dir.join(filename);
        if src.exists() {
            let dst = output_dir.join(filename);
            std::fs::copy(&src, &dst)?;
            log::info!("Copied {} -> {}", src.display(), dst.display());
        } else {
            log::warn!("Boundary artifact not found: {} (skipping)", src.display());
        }
    }
    Ok(())
}
