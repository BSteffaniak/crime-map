//! Shared boundary data storage in `DuckDB`.
//!
//! Stores census tract, place, county, state, and neighborhood boundaries
//! with their `GeoJSON` geometry as plain TEXT (no `PostGIS` spatial types).
//! The boundaries `DuckDB` lives at `data/shared/boundaries.duckdb`.

use std::path::Path;

use duckdb::Connection;

use crate::DbError;

/// Opens (or creates) the boundaries `DuckDB` and ensures schema exists.
///
/// # Errors
///
/// Returns [`DbError`] if the connection or schema creation fails.
pub fn open(path: &Path) -> Result<Connection, DbError> {
    if let Some(parent) = path.parent() {
        crate::paths::ensure_dir(parent)?;
    }

    let conn = Connection::open(path)?;

    conn.execute_batch("SET threads = 4; SET memory_limit = '512MB';")?;

    create_schema(&conn)?;

    Ok(conn)
}

/// Opens the boundaries DB at the default path.
///
/// # Errors
///
/// Returns [`DbError`] if the connection or schema creation fails.
pub fn open_default() -> Result<Connection, DbError> {
    open(&crate::paths::boundaries_db_path())
}

fn create_schema(conn: &Connection) -> Result<(), DbError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS census_tracts (
            geoid TEXT PRIMARY KEY,
            name TEXT,
            state_fips TEXT,
            county_fips TEXT,
            state_abbr TEXT,
            county_name TEXT,
            boundary_geojson TEXT,
            land_area_sq_mi DOUBLE,
            population INTEGER,
            centroid_lon DOUBLE,
            centroid_lat DOUBLE
        );

        CREATE TABLE IF NOT EXISTS census_places (
            geoid TEXT PRIMARY KEY,
            name TEXT,
            full_name TEXT,
            state_fips TEXT,
            state_abbr TEXT,
            place_type TEXT,
            boundary_geojson TEXT,
            land_area_sq_mi DOUBLE,
            population INTEGER,
            centroid_lon DOUBLE,
            centroid_lat DOUBLE
        );

        CREATE TABLE IF NOT EXISTS census_counties (
            geoid TEXT PRIMARY KEY,
            name TEXT,
            full_name TEXT,
            state_fips TEXT,
            county_fips TEXT,
            state_abbr TEXT,
            boundary_geojson TEXT,
            land_area_sq_mi DOUBLE,
            population INTEGER,
            centroid_lon DOUBLE,
            centroid_lat DOUBLE
        );

        CREATE TABLE IF NOT EXISTS census_states (
            fips TEXT PRIMARY KEY,
            name TEXT,
            abbr TEXT,
            boundary_geojson TEXT,
            land_area_sq_mi DOUBLE,
            population BIGINT,
            centroid_lon DOUBLE,
            centroid_lat DOUBLE
        );

        CREATE TABLE IF NOT EXISTS neighborhoods (
            id INTEGER PRIMARY KEY,
            source_id TEXT NOT NULL,
            city TEXT,
            state TEXT,
            name TEXT,
            boundary_geojson TEXT
        );

        CREATE SEQUENCE IF NOT EXISTS neighborhoods_id_seq START 1;

        CREATE TABLE IF NOT EXISTS tract_neighborhoods (
            geoid TEXT NOT NULL,
            neighborhood_id INTEGER NOT NULL,
            PRIMARY KEY (geoid, neighborhood_id)
        );",
    )?;

    Ok(())
}

/// Tables in the boundaries schema that support upsert via `INSERT OR
/// REPLACE`.
const MERGE_TABLES: &[&str] = &[
    "census_states",
    "census_counties",
    "census_tracts",
    "census_places",
    "neighborhoods",
    "tract_neighborhoods",
];

/// Merges rows from a source boundaries `DuckDB` file into the target
/// connection.
///
/// For each table present in the source, rows are upserted into the
/// target using `INSERT OR REPLACE`. This is safe because each parallel
/// boundary partition writes to non-overlapping rows (different states,
/// different boundary types).
///
/// Returns the total number of rows merged across all tables.
///
/// # Errors
///
/// Returns [`DbError`] if ATTACH, INSERT, or DETACH fails.
pub fn merge_from(target: &Connection, source_path: &Path) -> Result<u64, DbError> {
    let path_str = source_path.display().to_string();
    let alias = "src_boundaries";

    target.execute_batch(&format!("ATTACH '{path_str}' AS {alias} (READ_ONLY);"))?;

    // The merge must handle large partition files (600MB+ for counties/tracts).
    // Raise the memory limit above the default 512MB and disable insertion-order
    // preservation to reduce peak memory during bulk INSERT ... SELECT.
    target.execute_batch("SET memory_limit = '4GB'; SET preserve_insertion_order = false;")?;

    let mut total = 0u64;

    for table in MERGE_TABLES {
        // Check if the table exists in the source and has rows.
        // DuckDB uses `table_catalog` for the attached database alias
        // and `table_schema` for the schema within that database (usually `main`).
        let has_table: bool = target
            .prepare(&format!(
                "SELECT COUNT(*) FROM information_schema.tables
                 WHERE table_catalog = '{alias}' AND table_name = '{table}'"
            ))?
            .query_row([], |row| row.get::<_, i64>(0))
            .map(|c| c > 0)
            .unwrap_or(false);

        if !has_table {
            log::debug!("  {table}: not found in source, skipping");
            continue;
        }

        let count: i64 = target
            .prepare(&format!("SELECT COUNT(*) FROM {alias}.{table}"))?
            .query_row([], |row| row.get(0))?;

        if count == 0 {
            log::debug!("  {table}: 0 rows in source, skipping");
            continue;
        }

        target.execute_batch(&format!(
            "INSERT OR REPLACE INTO {table} SELECT * FROM {alias}.{table};"
        ))?;

        #[allow(clippy::cast_sign_loss)]
        let count_u = count as u64;
        total += count_u;
        log::info!("  merged {count_u} rows from {table}");
    }

    target.execute_batch(&format!("DETACH {alias};"))?;

    Ok(total)
}
