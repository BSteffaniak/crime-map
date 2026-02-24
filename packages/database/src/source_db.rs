//! Per-source `DuckDB` incident storage.
//!
//! Each crime data source gets its own `DuckDB` file at
//! `data/sources/{source_id}.duckdb`. The file contains an `incidents`
//! table and a `_meta` table for tracking sync state.

use std::collections::BTreeMap;
use std::path::Path;

use crime_map_source_models::NormalizedIncident;
use duckdb::Connection;

use crate::DbError;

/// Number of rows per INSERT chunk (`DuckDB` handles large batches well).
const CHUNK_SIZE: usize = 5_000;

/// Opens (or creates) a per-source `DuckDB` database and ensures the
/// schema exists.
///
/// # Errors
///
/// Returns [`DbError`] if the connection or schema creation fails.
pub fn open(path: &Path) -> Result<Connection, DbError> {
    if let Some(parent) = path.parent() {
        crate::paths::ensure_dir(parent)?;
    }

    let conn = Connection::open(path)?;

    // Enable performance optimizations
    conn.execute_batch(
        "SET threads = 4;
         SET memory_limit = '512MB';",
    )?;

    create_schema(&conn)?;

    Ok(conn)
}

/// Opens a source DB by source ID using the default path.
///
/// # Errors
///
/// Returns [`DbError`] if the connection or schema creation fails.
pub fn open_by_id(source_id: &str) -> Result<Connection, DbError> {
    open(&crate::paths::source_db_path(source_id))
}

fn create_schema(conn: &Connection) -> Result<(), DbError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS incidents (
            source_incident_id TEXT NOT NULL PRIMARY KEY,
            category TEXT NOT NULL,
            parent_category TEXT NOT NULL,
            severity SMALLINT NOT NULL,
            longitude DOUBLE NOT NULL,
            latitude DOUBLE NOT NULL,
            occurred_at TIMESTAMPTZ,
            description TEXT,
            block_address TEXT,
            city TEXT,
            state TEXT,
            arrest_made BOOLEAN,
            domestic BOOLEAN,
            location_type TEXT,
            has_coordinates BOOLEAN NOT NULL DEFAULT TRUE,
            geocoded BOOLEAN NOT NULL DEFAULT FALSE,
            census_place_geoid TEXT,
            census_tract_geoid TEXT
        );

        CREATE TABLE IF NOT EXISTS _meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );",
    )?;

    Ok(())
}

/// Inserts a batch of normalized incidents into the source `DuckDB`.
///
/// Uses multi-row INSERT with ON CONFLICT to upsert. Category names
/// are stored denormalized (no integer FK lookup needed).
///
/// Returns the number of rows affected.
///
/// # Errors
///
/// Returns [`DbError`] if any database operation fails.
#[allow(clippy::too_many_lines)]
pub fn insert_incidents(
    conn: &Connection,
    incidents: &[NormalizedIncident],
) -> Result<u64, DbError> {
    if incidents.is_empty() {
        return Ok(0);
    }

    let mut total_inserted = 0u64;

    // Deduplicate within the batch: keep last occurrence of each source_incident_id
    let mut last_seen: BTreeMap<&str, usize> = BTreeMap::new();
    for (i, incident) in incidents.iter().enumerate() {
        last_seen.insert(&incident.source_incident_id, i);
    }
    let deduped: Vec<&NormalizedIncident> = incidents
        .iter()
        .enumerate()
        .filter(|(i, inc)| last_seen.get(inc.source_incident_id.as_str()) == Some(i))
        .map(|(_, inc)| inc)
        .collect();

    if deduped.len() < incidents.len() {
        log::info!(
            "Deduplicated INSERT batch: {} -> {} rows ({} duplicates removed)",
            incidents.len(),
            deduped.len(),
            incidents.len() - deduped.len(),
        );
    }

    for chunk in deduped.chunks(CHUNK_SIZE) {
        let mut sql = String::from(
            "INSERT INTO incidents (
                source_incident_id, category, parent_category, severity,
                longitude, latitude, occurred_at, description, block_address,
                city, state, arrest_made, domestic, location_type,
                has_coordinates, geocoded
            ) VALUES ",
        );

        for (i, _) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        }

        sql.push_str(
            " ON CONFLICT (source_incident_id) DO UPDATE SET
                category = EXCLUDED.category,
                parent_category = EXCLUDED.parent_category,
                severity = EXCLUDED.severity,
                longitude = EXCLUDED.longitude,
                latitude = EXCLUDED.latitude,
                occurred_at = EXCLUDED.occurred_at,
                description = EXCLUDED.description,
                block_address = EXCLUDED.block_address,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                arrest_made = EXCLUDED.arrest_made,
                domestic = EXCLUDED.domestic,
                location_type = EXCLUDED.location_type,
                has_coordinates = EXCLUDED.has_coordinates,
                geocoded = EXCLUDED.geocoded",
        );

        let mut stmt = conn.prepare(&sql)?;
        let mut param_idx = 1usize;

        for incident in chunk {
            let has_coordinates = incident.longitude.is_some() && incident.latitude.is_some();
            let subcategory_name = incident.subcategory.as_ref();
            let parent_category = incident.subcategory.category();
            let parent_category_name = parent_category.as_ref();
            let severity = incident.subcategory.severity().value();

            stmt.raw_bind_parameter(param_idx, &incident.source_incident_id)?;
            stmt.raw_bind_parameter(param_idx + 1, subcategory_name)?;
            stmt.raw_bind_parameter(param_idx + 2, parent_category_name)?;
            stmt.raw_bind_parameter(param_idx + 3, i16::from(severity))?;
            stmt.raw_bind_parameter(param_idx + 4, incident.longitude.unwrap_or(0.0))?;
            stmt.raw_bind_parameter(param_idx + 5, incident.latitude.unwrap_or(0.0))?;

            // occurred_at
            if let Some(ref dt) = incident.occurred_at {
                stmt.raw_bind_parameter(param_idx + 6, dt.format("%Y-%m-%d %H:%M:%S").to_string())?;
            } else {
                stmt.raw_bind_parameter(param_idx + 6, Option::<&str>::None)?;
            }

            // description
            stmt.raw_bind_parameter(param_idx + 7, incident.description.as_deref())?;
            // block_address
            stmt.raw_bind_parameter(param_idx + 8, incident.block_address.as_deref())?;
            // city
            stmt.raw_bind_parameter(param_idx + 9, &incident.city)?;
            // state
            stmt.raw_bind_parameter(param_idx + 10, &incident.state)?;
            // arrest_made
            stmt.raw_bind_parameter(param_idx + 11, incident.arrest_made)?;
            // domestic
            stmt.raw_bind_parameter(param_idx + 12, incident.domestic)?;
            // location_type
            stmt.raw_bind_parameter(param_idx + 13, incident.location_type.as_deref())?;
            // has_coordinates
            stmt.raw_bind_parameter(param_idx + 14, has_coordinates)?;
            // geocoded
            stmt.raw_bind_parameter(param_idx + 15, incident.geocoded)?;

            param_idx += 16;
        }

        let rows = stmt.raw_execute()?;
        total_inserted += u64::try_from(rows).unwrap_or(0);
    }

    Ok(total_inserted)
}

/// Returns the number of incidents stored for this source.
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub fn get_record_count(conn: &Connection) -> Result<u64, DbError> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM incidents")?;
    let count: i64 = stmt.query_row([], |row| row.get(0))?;
    #[allow(clippy::cast_sign_loss)]
    Ok(count as u64)
}

/// Returns the maximum `occurred_at` timestamp, or `None` if no
/// incidents exist.
///
/// Tries multiple timestamp formats to handle `DuckDB`'s text cast
/// variations (with/without fractional seconds, with/without timezone).
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub fn get_max_occurred_at(
    conn: &Connection,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT MAX(occurred_at)::TEXT as max_ts FROM incidents WHERE occurred_at IS NOT NULL",
    )?;
    let result: Option<String> = stmt.query_row([], |row| row.get(0))?;

    Ok(result.and_then(|s| parse_timestamp(&s)))
}

/// Parses a `DuckDB` timestamp text representation into a UTC `DateTime`.
///
/// `DuckDB`'s `::TEXT` cast can produce several formats depending on the
/// stored precision:
/// - `2024-01-15 10:30:00` (no fractional seconds)
/// - `2024-01-15 10:30:00.123` (fractional seconds)
/// - `2024-01-15 10:30:00+00` (with timezone)
/// - `2024-01-15 10:30:00.123+00` (both)
///
/// This function tries them in order and returns the first successful parse.
fn parse_timestamp(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    use chrono::{DateTime, NaiveDateTime, Utc};

    // Try parsing as a full DateTime with timezone first
    if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%z") {
        return Some(dt.with_timezone(&Utc));
    }
    if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%z") {
        return Some(dt.with_timezone(&Utc));
    }

    // Fall back to naive (no timezone) — assume UTC
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc));
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc));
    }

    log::warn!("Failed to parse timestamp: {s:?}");
    None
}

/// Gets a metadata value from the `_meta` table.
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub fn get_meta(conn: &Connection, key: &str) -> Result<Option<String>, DbError> {
    let mut stmt = conn.prepare("SELECT value FROM _meta WHERE key = ?")?;
    let result = stmt.query_row([key], |row| row.get(0));
    match result {
        Ok(v) => Ok(Some(v)),
        Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(DbError::DuckDb(e)),
    }
}

/// Sets a metadata value in the `_meta` table.
///
/// # Errors
///
/// Returns [`DbError`] if the upsert fails.
pub fn set_meta(conn: &Connection, key: &str, value: &str) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO _meta (key, value) VALUES (?, ?)
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        duckdb::params![key, value],
    )?;
    Ok(())
}

/// Updates the sync metadata after a successful ingestion.
///
/// # Errors
///
/// Returns [`DbError`] if the metadata update fails.
pub fn update_sync_metadata(conn: &Connection, source_name: &str) -> Result<(), DbError> {
    let count = get_record_count(conn)?;
    let now = chrono::Utc::now().to_rfc3339();

    set_meta(conn, "source_name", source_name)?;
    set_meta(conn, "record_count", &count.to_string())?;
    set_meta(conn, "last_synced_at", &now)?;

    Ok(())
}

/// Returns whether this source has completed a full (non-limited) sync.
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub fn get_fully_synced(conn: &Connection) -> Result<bool, DbError> {
    get_meta(conn, "fully_synced").map(|v| v.as_deref() == Some("true"))
}

/// Sets whether this source has completed a full sync.
///
/// # Errors
///
/// Returns [`DbError`] if the update fails.
pub fn set_fully_synced(conn: &Connection, fully_synced: bool) -> Result<(), DbError> {
    set_meta(
        conn,
        "fully_synced",
        if fully_synced { "true" } else { "false" },
    )
}

/// Updates coordinates for geocoded incidents.
///
/// When `clear_attribution` is true, also clears census GEOIDs for
/// re-attribution.
///
/// # Errors
///
/// Returns [`DbError`] if the update fails.
pub fn batch_update_geocoded(
    conn: &Connection,
    updates: &[(String, f64, f64)],
    clear_attribution: bool,
) -> Result<u64, DbError> {
    if updates.is_empty() {
        return Ok(0);
    }

    let sql = if clear_attribution {
        "UPDATE incidents SET
            longitude = ?, latitude = ?,
            geocoded = TRUE,
            census_place_geoid = NULL,
            census_tract_geoid = NULL
         WHERE source_incident_id = ?"
    } else {
        "UPDATE incidents SET
            longitude = ?, latitude = ?,
            has_coordinates = TRUE,
            geocoded = TRUE
         WHERE source_incident_id = ?"
    };

    let mut stmt = conn.prepare(sql)?;
    let mut total = 0u64;

    for (incident_id, lng, lat) in updates {
        let rows = stmt.execute(duckdb::params![lng, lat, incident_id])?;
        total += u64::try_from(rows).unwrap_or(0);
    }

    Ok(total)
}

/// Marks incidents as `geocoded = TRUE` without changing coordinates.
///
/// # Errors
///
/// Returns [`DbError`] if the update fails.
pub fn batch_mark_geocoded(conn: &Connection, incident_ids: &[String]) -> Result<u64, DbError> {
    if incident_ids.is_empty() {
        return Ok(0);
    }

    let mut total = 0u64;

    for chunk in incident_ids.chunks(1000) {
        let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let sql = format!(
            "UPDATE incidents SET geocoded = TRUE WHERE source_incident_id IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql)?;

        for (i, id) in chunk.iter().enumerate() {
            stmt.raw_bind_parameter(i + 1, id)?;
        }

        let rows = stmt.raw_execute()?;
        total += u64::try_from(rows).unwrap_or(0);
    }

    Ok(total)
}

/// Updates census place and tract GEOIDs for a batch of incidents.
///
/// # Errors
///
/// Returns [`DbError`] if the update fails.
pub fn batch_update_attribution(
    conn: &Connection,
    updates: &[(String, Option<String>, Option<String>)],
) -> Result<u64, DbError> {
    if updates.is_empty() {
        return Ok(0);
    }

    let mut stmt = conn.prepare(
        "UPDATE incidents SET
            census_place_geoid = ?,
            census_tract_geoid = ?
         WHERE source_incident_id = ?",
    )?;

    let mut total = 0u64;

    for (incident_id, place_geoid, tract_geoid) in updates {
        let rows = stmt.execute(duckdb::params![
            place_geoid.as_deref(),
            tract_geoid.as_deref(),
            incident_id,
        ])?;
        total += u64::try_from(rows).unwrap_or(0);
    }

    Ok(total)
}

/// Discovers all source `DuckDB` files in the sources directory.
///
/// Returns a sorted list of source IDs (derived from filenames).
#[must_use]
pub fn discover_source_ids() -> Vec<String> {
    let dir = crate::paths::sources_dir();
    let mut ids = Vec::new();

    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("duckdb")
                && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
            {
                ids.push(stem.to_string());
            }
        }
    }

    ids.sort();
    ids
}
