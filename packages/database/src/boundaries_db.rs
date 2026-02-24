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
