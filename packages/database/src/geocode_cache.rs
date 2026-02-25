//! Geocoding result cache stored in `DuckDB`.
//!
//! Shared across all sources. Caches both successful geocodes
//! (with coordinates) and failed lookups (null coordinates) so
//! we don't re-query the same addresses.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use duckdb::Connection;

use crate::DbError;

/// A cached geocoding result: `(address_key, provider, lat, lng, matched_address)`.
pub type CacheEntry = (String, String, Option<f64>, Option<f64>, Option<String>);

/// Opens (or creates) the geocode cache `DuckDB`.
///
/// # Errors
///
/// Returns [`DbError`] if the connection or schema creation fails.
pub fn open(path: &Path) -> Result<Connection, DbError> {
    if let Some(parent) = path.parent() {
        crate::paths::ensure_dir(parent)?;
    }

    let conn = Connection::open(path)?;
    create_schema(&conn)?;
    Ok(conn)
}

/// Opens the geocode cache at the default path.
///
/// # Errors
///
/// Returns [`DbError`] if the connection or schema creation fails.
pub fn open_default() -> Result<Connection, DbError> {
    open(&crate::paths::geocode_cache_db_path())
}

fn create_schema(conn: &Connection) -> Result<(), DbError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS geocode_cache (
            address_key TEXT NOT NULL,
            provider TEXT NOT NULL,
            lat DOUBLE,
            lng DOUBLE,
            matched_address TEXT,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (address_key, provider)
        );",
    )?;
    Ok(())
}

/// Result of a geocode cache lookup: `(hits, tried_keys)`.
pub type CacheLookupResult = (BTreeMap<String, (f64, f64)>, BTreeSet<String>);

/// Looks up cached geocoding results for the given address keys.
///
/// Returns `(hits, tried)` where:
/// - `hits` maps `address_key` -> (lat, lng) for successful geocodes
/// - `tried` contains all `address_keys` that have any cache entry (hit or miss)
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub fn cache_lookup(
    conn: &Connection,
    address_keys: &[String],
) -> Result<CacheLookupResult, DbError> {
    let mut hits: BTreeMap<String, (f64, f64)> = BTreeMap::new();
    let mut tried: BTreeSet<String> = BTreeSet::new();

    if address_keys.is_empty() {
        return Ok((hits, tried));
    }

    for chunk in address_keys.chunks(1000) {
        let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let sql = format!(
            "SELECT address_key, lat, lng FROM geocode_cache WHERE address_key IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&sql)?;

        for (i, key) in chunk.iter().enumerate() {
            stmt.raw_bind_parameter(i + 1, key)?;
        }

        stmt.raw_execute()?;
        let mut rows = stmt.raw_query();
        while let Some(row) = rows.next()? {
            let key: String = row.get(0)?;
            let lat: Option<f64> = row.get(1)?;
            let lng: Option<f64> = row.get(2)?;

            tried.insert(key.clone());

            if let (Some(lat_v), Some(lng_v)) = (lat, lng) {
                hits.insert(key, (lat_v, lng_v));
            }
        }
    }

    Ok((hits, tried))
}

/// Inserts geocoding results (both hits and misses) into the cache.
///
/// # Errors
///
/// Returns [`DbError`] if the insert fails.
pub fn cache_insert(conn: &Connection, entries: &[CacheEntry]) -> Result<(), DbError> {
    if entries.is_empty() {
        return Ok(());
    }

    let mut stmt = conn.prepare(
        "INSERT INTO geocode_cache (address_key, provider, lat, lng, matched_address)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT (address_key, provider) DO NOTHING",
    )?;

    for (key, provider, lat, lng, matched) in entries {
        stmt.execute(duckdb::params![key, provider, lat, lng, matched.as_deref(),])?;
    }

    Ok(())
}
