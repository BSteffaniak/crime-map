//! Database query functions for crime data.
//!
//! Spatial queries use `query_raw_params()` with `PostGIS` functions.
//! Non-spatial queries use the typed `switchy_database` query builder.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

use crime_map_crime_models::{CrimeSeverity, CrimeSubcategory};
use crime_map_database_models::{IncidentQuery, IncidentRow};
use crime_map_source::progress::ProgressCallback;
use crime_map_source_models::NormalizedIncident;
use moosicbox_json_utils::database::ToValue as _;
use switchy_database::{Database, DatabaseValue};

use crate::DbError;

/// Inserts or retrieves the ID for a crime source.
///
/// # Errors
///
/// Returns [`DbError`] if the database operation fails.
pub async fn upsert_source(
    db: &dyn Database,
    name: &str,
    source_type: &str,
    api_url: Option<&str>,
    coverage_area: &str,
    portal_url: Option<&str>,
) -> Result<i32, DbError> {
    let rows = db
        .query_raw_params(
            "INSERT INTO crime_sources (name, source_type, api_url, coverage_area, portal_url)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (name) DO UPDATE SET
                 source_type = EXCLUDED.source_type,
                 api_url = EXCLUDED.api_url,
                 portal_url = EXCLUDED.portal_url
             RETURNING id",
            &[
                DatabaseValue::String(name.to_string()),
                DatabaseValue::String(source_type.to_string()),
                api_url.map_or(DatabaseValue::Null, |u| {
                    DatabaseValue::String(u.to_string())
                }),
                DatabaseValue::String(coverage_area.to_string()),
                portal_url.map_or(DatabaseValue::Null, |u| {
                    DatabaseValue::String(u.to_string())
                }),
            ],
        )
        .await?;

    let row = rows.first().ok_or_else(|| DbError::Conversion {
        message: "Failed to get source id from upsert".to_string(),
    })?;

    let id: i32 = row.to_value("id").map_err(|e| DbError::Conversion {
        message: format!("Failed to parse source id: {e}"),
    })?;

    Ok(id)
}

/// Looks up the database source ID for a source by its TOML `name` field.
///
/// The `name_fragment` is matched against the `name` column using
/// case-insensitive `LIKE %fragment%` matching.
///
/// # Errors
///
/// Returns [`DbError`] if the database operation fails or no matching source
/// is found.
pub async fn get_source_id_by_name(db: &dyn Database, name_fragment: &str) -> Result<i32, DbError> {
    let rows = db
        .query_raw_params(
            "SELECT id FROM crime_sources WHERE LOWER(name) LIKE '%' || LOWER($1) || '%' LIMIT 1",
            &[DatabaseValue::String(name_fragment.to_string())],
        )
        .await?;

    let row = rows.first().ok_or_else(|| DbError::Conversion {
        message: format!("No source found matching '{name_fragment}'"),
    })?;

    let id: i32 = row.to_value("id").map_err(|e| DbError::Conversion {
        message: format!("Failed to parse source id: {e}"),
    })?;

    Ok(id)
}

/// Looks up the category ID for a [`CrimeSubcategory`] by its
/// `SCREAMING_SNAKE_CASE` name.
///
/// # Errors
///
/// Returns [`DbError`] if the database operation fails or category is not
/// found.
pub async fn get_category_id(
    db: &dyn Database,
    subcategory: CrimeSubcategory,
) -> Result<i32, DbError> {
    let name = subcategory.as_ref();
    let rows = db
        .query_raw_params(
            "SELECT id FROM crime_categories WHERE name = $1",
            &[DatabaseValue::String(name.to_string())],
        )
        .await?;

    let row = rows.first().ok_or_else(|| DbError::Conversion {
        message: format!("Category not found: {name}"),
    })?;

    let id: i32 = row.to_value("id").map_err(|e| DbError::Conversion {
        message: format!("Failed to parse category id: {e}"),
    })?;

    Ok(id)
}

/// Inserts a batch of normalized incidents into the database.
///
/// Uses multi-row `INSERT ... VALUES (...), (...), ...` statements chunked
/// to stay within the 65,535 bind-parameter limit. Each row uses 16 bind
/// parameters (plus a subquery for `parent_category_id` derived from
/// `category_id`), giving ~4,095 rows per chunk.
///
/// Uses `ON CONFLICT` to skip duplicates based on
/// `(source_id, source_incident_id)`.
///
/// Incidents without coordinates use a sentinel point at `(0, 0)` with
/// `has_coordinates = FALSE` so the `NOT NULL` constraint on `location`
/// is satisfied while still storing the incident for aggregate counting.
///
/// # Errors
///
/// Returns [`DbError`] if any database operation fails.
#[allow(clippy::too_many_lines)]
pub async fn insert_incidents(
    db: &dyn Database,
    source_id: i32,
    incidents: &[NormalizedIncident],
    category_ids: &BTreeMap<CrimeSubcategory, i32>,
) -> Result<u64, DbError> {
    use std::fmt::Write as _;

    /// Number of `$N` placeholders per row in the VALUES clause.
    ///
    /// `parent_category_id` is derived via subquery from the same
    /// `category_id` bind param, so it does not add an extra placeholder.
    const PARAMS_PER_ROW: u32 = 16;
    /// Maximum number of bind parameters per statement.
    const PG_MAX_PARAMS: u32 = 65_535;
    /// Maximum rows per INSERT chunk.
    const CHUNK_SIZE: usize = (PG_MAX_PARAMS / PARAMS_PER_ROW) as usize;

    // Pre-filter incidents that have a valid category, logging warnings for
    // those that don't.
    let valid: Vec<(&NormalizedIncident, i32)> = incidents
        .iter()
        .filter_map(|incident| {
            if let Some(&cat_id) = category_ids.get(&incident.subcategory) {
                Some((incident, cat_id))
            } else {
                log::warn!(
                    "No category ID for {:?}, skipping incident {}",
                    incident.subcategory,
                    incident.source_incident_id
                );
                None
            }
        })
        .collect();

    if valid.is_empty() {
        return Ok(0);
    }

    let mut total_inserted = 0u64;

    for chunk in valid.chunks(CHUNK_SIZE) {
        let mut sql = String::from(
            "INSERT INTO crime_incidents (\
                source_id, source_incident_id, category_id, parent_category_id, location, \
                occurred_at, reported_at, description, block_address, \
                city, state, arrest_made, domestic, location_type, \
                has_coordinates, geocoded\
            ) VALUES ",
        );
        let mut params: Vec<DatabaseValue> =
            Vec::with_capacity(chunk.len() * PARAMS_PER_ROW as usize);
        let mut idx = 1u32;

        for (i, (incident, cat_id)) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            write!(
                sql,
                "(${p1}, ${p2}, ${p3}, \
                 (SELECT COALESCE(parent_id, id) FROM crime_categories WHERE id = ${p3}), \
                 ST_SetSRID(ST_MakePoint(${p4}, ${p5}), 4326)::geography, \
                 ${p6}, ${p7}, ${p8}, ${p9}, ${p10}, ${p11}, ${p12}, ${p13}, ${p14}, ${p15}, ${p16})",
                p1 = idx,
                p2 = idx + 1,
                p3 = idx + 2,
                p4 = idx + 3,
                p5 = idx + 4,
                p6 = idx + 5,
                p7 = idx + 6,
                p8 = idx + 7,
                p9 = idx + 8,
                p10 = idx + 9,
                p11 = idx + 10,
                p12 = idx + 11,
                p13 = idx + 12,
                p14 = idx + 13,
                p15 = idx + 14,
                p16 = idx + 15,
            )
            .unwrap();

            let has_coordinates = incident.longitude.is_some() && incident.latitude.is_some();

            params.push(DatabaseValue::Int32(source_id));
            params.push(DatabaseValue::String(incident.source_incident_id.clone()));
            params.push(DatabaseValue::Int32(*cat_id));
            params.push(DatabaseValue::Real64(incident.longitude.unwrap_or(0.0)));
            params.push(DatabaseValue::Real64(incident.latitude.unwrap_or(0.0)));
            params.push(
                incident
                    .occurred_at
                    .as_ref()
                    .map_or(DatabaseValue::Null, |dt| {
                        DatabaseValue::DateTime(dt.naive_utc())
                    }),
            );
            params.push(
                incident
                    .reported_at
                    .as_ref()
                    .map_or(DatabaseValue::Null, |dt| {
                        DatabaseValue::DateTime(dt.naive_utc())
                    }),
            );
            params.push(
                incident
                    .description
                    .as_ref()
                    .map_or(DatabaseValue::Null, |d| DatabaseValue::String(d.clone())),
            );
            params.push(
                incident
                    .block_address
                    .as_ref()
                    .map_or(DatabaseValue::Null, |a| DatabaseValue::String(a.clone())),
            );
            params.push(DatabaseValue::String(incident.city.clone()));
            params.push(DatabaseValue::String(incident.state.clone()));
            params.push(
                incident
                    .arrest_made
                    .map_or(DatabaseValue::Null, DatabaseValue::Bool),
            );
            params.push(
                incident
                    .domestic
                    .map_or(DatabaseValue::Null, DatabaseValue::Bool),
            );
            params.push(
                incident
                    .location_type
                    .as_ref()
                    .map_or(DatabaseValue::Null, |l| DatabaseValue::String(l.clone())),
            );
            params.push(DatabaseValue::Bool(has_coordinates));
            params.push(DatabaseValue::Bool(incident.geocoded));

            idx += PARAMS_PER_ROW;
        }

        sql.push_str(
            " ON CONFLICT (source_id, source_incident_id) DO UPDATE SET \
             category_id = EXCLUDED.category_id, \
             parent_category_id = EXCLUDED.parent_category_id, \
             location = EXCLUDED.location, \
             occurred_at = EXCLUDED.occurred_at, \
             reported_at = EXCLUDED.reported_at, \
             description = EXCLUDED.description, \
             block_address = EXCLUDED.block_address, \
             city = EXCLUDED.city, \
             state = EXCLUDED.state, \
             arrest_made = EXCLUDED.arrest_made, \
             domestic = EXCLUDED.domestic, \
             location_type = EXCLUDED.location_type, \
             has_coordinates = EXCLUDED.has_coordinates, \
             geocoded = EXCLUDED.geocoded",
        );

        total_inserted += db.exec_raw_params(&sql, &params).await?;
    }

    Ok(total_inserted)
}

/// Returns the number of incidents already stored for a given source.
///
/// Used for resume offset calculation — when a sync was interrupted, we can
/// skip API pages that were already ingested by starting at this offset.
///
/// # Errors
///
/// Returns [`DbError`] if the database operation fails.
pub async fn get_source_record_count(db: &dyn Database, source_id: i32) -> Result<u64, DbError> {
    let rows = db
        .query_raw_params(
            "SELECT COUNT(*) as count FROM crime_incidents WHERE source_id = $1",
            &[DatabaseValue::Int32(source_id)],
        )
        .await?;

    let Some(row) = rows.first() else {
        return Ok(0);
    };

    let count: i64 = row.to_value("count").unwrap_or(0);

    #[allow(clippy::cast_sign_loss)]
    Ok(count as u64)
}

/// Returns the maximum `occurred_at` timestamp for a given source, or `None`
/// if the source has no incidents yet.
///
/// Used for incremental syncing — we only need to fetch records newer than
/// this value (minus a safety buffer).
///
/// # Errors
///
/// Returns [`DbError`] if the database operation fails.
pub async fn get_source_max_occurred_at(
    db: &dyn Database,
    source_id: i32,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, DbError> {
    let rows = db
        .query_raw_params(
            "SELECT MAX(occurred_at) as max_occurred_at FROM crime_incidents WHERE source_id = $1",
            &[DatabaseValue::Int32(source_id)],
        )
        .await?;

    let Some(row) = rows.first() else {
        return Ok(None);
    };

    let naive: Option<chrono::NaiveDateTime> = row.to_value("max_occurred_at").unwrap_or(None);

    Ok(naive.map(|n| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(n, chrono::Utc)))
}

/// Updates the record count and last-synced timestamp for a source.
///
/// # Errors
///
/// Returns [`DbError`] if the database operation fails.
pub async fn update_source_stats(db: &dyn Database, source_id: i32) -> Result<(), DbError> {
    db.exec_raw_params(
        "UPDATE crime_sources SET
            record_count = (SELECT COUNT(*) FROM crime_incidents WHERE source_id = $1),
            last_synced_at = NOW()
         WHERE id = $1",
        &[DatabaseValue::Int32(source_id)],
    )
    .await?;

    Ok(())
}

/// Returns whether a source has ever completed a full (non-limited) sync.
///
/// # Errors
///
/// Returns [`DbError`] if the database operation fails.
pub async fn get_source_fully_synced(db: &dyn Database, source_id: i32) -> Result<bool, DbError> {
    let rows = db
        .query_raw_params(
            "SELECT fully_synced FROM crime_sources WHERE id = $1",
            &[DatabaseValue::Int32(source_id)],
        )
        .await?;

    let Some(row) = rows.first() else {
        return Ok(false);
    };

    let fully_synced: bool = row.to_value("fully_synced").unwrap_or(false);
    Ok(fully_synced)
}

/// Marks whether a source has completed a full sync.
///
/// Should be set to `true` only when a sync completes successfully without
/// a `--limit` cap. A limited or interrupted sync keeps this `false` so
/// the next run knows to do a full fetch.
///
/// # Errors
///
/// Returns [`DbError`] if the database operation fails.
pub async fn set_source_fully_synced(
    db: &dyn Database,
    source_id: i32,
    fully_synced: bool,
) -> Result<(), DbError> {
    db.exec_raw_params(
        "UPDATE crime_sources SET fully_synced = $2 WHERE id = $1",
        &[
            DatabaseValue::Int32(source_id),
            DatabaseValue::Bool(fully_synced),
        ],
    )
    .await?;

    Ok(())
}

/// Queries crime incidents within a bounding box and optional filters.
///
/// # Errors
///
/// Returns [`DbError`] if the database operation fails.
#[allow(clippy::too_many_lines)]
pub async fn query_incidents(
    db: &dyn Database,
    query: &IncidentQuery,
) -> Result<Vec<IncidentRow>, DbError> {
    let mut sql = String::from(
        "SELECT i.id, i.source_id, i.source_incident_id,
                i.occurred_at, i.reported_at, i.description,
                i.block_address, i.city, i.state,
                i.arrest_made, i.domestic, i.location_type,
                c.name as subcategory_name, c.severity,
                pc.name as category_name,
                ST_X(i.location::geometry) as longitude,
                ST_Y(i.location::geometry) as latitude
         FROM crime_incidents i
         JOIN crime_categories c ON i.category_id = c.id
         LEFT JOIN crime_categories pc ON c.parent_id = pc.id
         WHERE i.has_coordinates = TRUE",
    );

    let mut params: Vec<DatabaseValue> = Vec::new();
    let mut param_idx = 1u32;

    if let Some(bbox) = &query.bbox {
        write!(
            sql,
            " AND i.location && ST_MakeEnvelope(${}, ${}, ${}, ${}, 4326)::geography",
            param_idx,
            param_idx + 1,
            param_idx + 2,
            param_idx + 3,
        )
        .unwrap();
        params.push(DatabaseValue::Real64(bbox.west));
        params.push(DatabaseValue::Real64(bbox.south));
        params.push(DatabaseValue::Real64(bbox.east));
        params.push(DatabaseValue::Real64(bbox.north));
        param_idx += 4;
    }

    if let Some(from) = &query.from {
        write!(sql, " AND i.occurred_at >= ${param_idx}").unwrap();
        params.push(DatabaseValue::DateTime(from.naive_utc()));
        param_idx += 1;
    }

    if let Some(to) = &query.to {
        write!(sql, " AND i.occurred_at <= ${param_idx}").unwrap();
        params.push(DatabaseValue::DateTime(to.naive_utc()));
        param_idx += 1;
    }

    if let Some(severity_min) = &query.severity_min {
        write!(sql, " AND c.severity >= ${param_idx}").unwrap();
        params.push(DatabaseValue::Int32(i32::from(severity_min.value())));
        param_idx += 1;
    }

    if let Some(arrest) = query.arrest_made {
        write!(sql, " AND i.arrest_made = ${param_idx}").unwrap();
        params.push(DatabaseValue::Bool(arrest));
        param_idx += 1;
    }

    sql.push_str(" ORDER BY i.occurred_at DESC");

    write!(sql, " LIMIT ${param_idx}").unwrap();
    params.push(DatabaseValue::Int64(i64::from(query.limit)));
    param_idx += 1;

    write!(sql, " OFFSET ${param_idx}").unwrap();
    params.push(DatabaseValue::Int64(i64::from(query.offset)));

    let rows = db.query_raw_params(&sql, &params).await?;

    let mut incidents = Vec::with_capacity(rows.len());

    for row in &rows {
        let subcategory_name: String = row.to_value("subcategory_name").unwrap_or_default();
        let subcategory = subcategory_name
            .parse::<CrimeSubcategory>()
            .unwrap_or(CrimeSubcategory::Unknown);
        let category = subcategory.category();

        let severity_val: i32 = row.to_value("severity").unwrap_or(1);
        let severity = CrimeSeverity::from_value(u8::try_from(severity_val).unwrap_or(1))
            .unwrap_or(CrimeSeverity::Minimal);

        let occurred_at_naive: Option<chrono::NaiveDateTime> =
            row.to_value("occurred_at").unwrap_or(None);
        let occurred_at = occurred_at_naive.map(|naive| {
            chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive, chrono::Utc)
        });

        let reported_at_naive: Option<chrono::NaiveDateTime> =
            row.to_value("reported_at").unwrap_or(None);
        let reported_at = reported_at_naive.map(|naive| {
            chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive, chrono::Utc)
        });

        incidents.push(IncidentRow {
            id: row.to_value("id").unwrap_or(0),
            source_id: row.to_value("source_id").unwrap_or(0),
            source_incident_id: row.to_value("source_incident_id").unwrap_or_default(),
            subcategory,
            category,
            severity,
            longitude: row.to_value("longitude").unwrap_or(0.0),
            latitude: row.to_value("latitude").unwrap_or(0.0),
            occurred_at,
            reported_at,
            description: row.to_value("description").unwrap_or(None),
            block_address: row.to_value("block_address").unwrap_or(None),
            city: row.to_value("city").unwrap_or_default(),
            state: row.to_value("state").unwrap_or_default(),
            arrest_made: row.to_value("arrest_made").unwrap_or(None),
            domestic: row.to_value("domestic").unwrap_or(None),
            location_type: row.to_value("location_type").unwrap_or(None),
        });
    }

    Ok(incidents)
}

/// Gets all category IDs as a map from subcategory enum to database ID.
///
/// # Errors
///
/// Returns [`DbError`] if the database operation fails.
pub async fn get_all_category_ids(
    db: &dyn Database,
) -> Result<BTreeMap<CrimeSubcategory, i32>, DbError> {
    let rows = db
        .query_raw_params(
            "SELECT id, name FROM crime_categories WHERE parent_id IS NOT NULL",
            &[],
        )
        .await?;

    let mut map = BTreeMap::new();
    for row in &rows {
        let name: String = row.to_value("name").unwrap_or_default();
        let id: i32 = row.to_value("id").unwrap_or(0);
        if let Ok(sub) = name.parse::<CrimeSubcategory>() {
            map.insert(sub, id);
        }
    }

    Ok(map)
}

/// Builds an optional `AND [alias.]source_id IN (...)` SQL fragment for
/// filtering attribution queries to a specific set of sources.
///
/// When `alias` is non-empty, the column is qualified (e.g., `i.source_id`).
/// When `alias` is empty, the column is unqualified (e.g., `source_id`).
///
/// Returns an empty string when `source_ids` is `None` (no filter).
fn build_source_filter(source_ids: Option<&[i32]>, alias: &str) -> String {
    match source_ids {
        Some(ids) if !ids.is_empty() => {
            use std::fmt::Write;
            let col = if alias.is_empty() {
                "source_id".to_string()
            } else {
                format!("{alias}.source_id")
            };
            let mut clause = format!(" AND {col} IN (");
            for (i, id) in ids.iter().enumerate() {
                if i > 0 {
                    clause.push(',');
                }
                write!(clause, "{id}").unwrap();
            }
            clause.push(')');
            clause
        }
        _ => String::new(),
    }
}

/// Assigns census place GEOIDs to incidents that have coordinates but no
/// `census_place_geoid` yet.
///
/// Uses a two-pass approach, batched by state for performance:
/// 1. **Exact containment** — `ST_Covers` assigns incidents that fall
///    directly inside a place boundary. When a point is inside multiple
///    overlapping places, the smallest (by area) wins.
/// 2. **Nearest within buffer** — For remaining unmatched incidents,
///    `ST_DWithin` with a configurable buffer (meters) finds the nearest
///    place.
///
/// Performance optimizations:
/// - Casts `geography` to `geometry` for planar `ST_Covers` (10–50× faster,
///   negligible accuracy difference for CONUS).
/// - Batches by state so each spatial join only compares incidents against
///   places in the same state, dramatically reducing the cross-product.
/// - Boosts `work_mem` for the session to allow hash joins.
///
/// # Errors
///
/// Returns [`DbError`] if the database operation fails.
#[allow(clippy::too_many_lines)]
pub async fn attribute_places(
    db: &dyn Database,
    buffer_meters: f64,
    _batch_size: u32,
    source_ids: Option<&[i32]>,
    progress: Option<Arc<dyn ProgressCallback>>,
) -> Result<u64, DbError> {
    let mut total = 0u64;

    // Build source filter clauses for different query contexts:
    // - bare: no table alias (for COUNT / DISTINCT queries)
    // - i2:   subquery alias (for the inner SELECT)
    // - i:    outer UPDATE alias (prevents deadlocks by scoping row locks)
    let filter_bare = build_source_filter(source_ids, "");
    let filter_i2 = build_source_filter(source_ids, "i2");
    let filter_i = build_source_filter(source_ids, "i");

    // Boost work_mem for this session to allow efficient hash joins
    db.exec_raw("SET work_mem = '256MB'").await?;

    // Ensure partial index exists for fast lookup of unattributed rows
    db.exec_raw(
        "CREATE INDEX IF NOT EXISTS idx_incidents_unattr_place \
         ON crime_incidents (id) \
         WHERE census_place_geoid IS NULL AND has_coordinates = TRUE",
    )
    .await?;

    // Query unattributed count for progress reporting
    let unattributed: i64 = {
        let rows = db
            .query_raw_params(
                &format!(
                    "SELECT COUNT(*) as cnt FROM crime_incidents \
                     WHERE census_place_geoid IS NULL AND has_coordinates = TRUE{filter_bare}"
                ),
                &[],
            )
            .await?;
        rows.first().map_or(0, |r| r.to_value("cnt").unwrap_or(0))
    };

    if let Some(ref p) = progress {
        #[allow(clippy::cast_sign_loss)]
        p.set_total(unattributed as u64);
    }

    if unattributed == 0 {
        if let Some(ref p) = progress {
            p.finish("Place attribution complete: 0 incidents (already done)".to_string());
        }
        return Ok(0);
    }

    // Get distinct states that have unattributed incidents
    let state_rows = db
        .query_raw_params(
            &format!(
                "SELECT DISTINCT state FROM crime_incidents \
                 WHERE census_place_geoid IS NULL AND has_coordinates = TRUE{filter_bare} \
                 ORDER BY state"
            ),
            &[],
        )
        .await?;

    let states: Vec<String> = state_rows
        .iter()
        .filter_map(|r| {
            let s: String = r.to_value("state").unwrap_or_default();
            if s.is_empty() { None } else { Some(s) }
        })
        .collect();

    log::info!(
        "Pass 1: exact containment (geometry cast + state batching) for ~{unattributed} incidents across {} states...",
        states.len()
    );

    // Pass 1: Exact containment per state, using geometry cast
    for state in &states {
        let updated = db
            .exec_raw_params(
                &format!(
                    "UPDATE crime_incidents i \
                     SET census_place_geoid = sub.geoid \
                     FROM ( \
                         SELECT DISTINCT ON (i2.id) i2.id, p.geoid \
                         FROM crime_incidents i2 \
                         JOIN census_places p \
                           ON p.state_abbr = i2.state \
                          AND ST_Covers(p.boundary::geometry, i2.location::geometry) \
                         WHERE i2.census_place_geoid IS NULL \
                           AND i2.has_coordinates = TRUE \
                           AND i2.state = $1{filter_i2} \
                         ORDER BY i2.id, ST_Area(p.boundary::geometry) \
                     ) sub \
                     WHERE i.id = sub.id{filter_i}"
                ),
                &[DatabaseValue::String(state.clone())],
            )
            .await?;

        total += updated;
        if updated > 0 {
            log::info!("  {state}: {updated} incidents (exact), {total} total");
        }
        if let Some(ref p) = progress {
            p.inc(updated);
        }
    }

    // Pass 2: Nearest within buffer, per state, using geometry cast
    // Convert buffer from meters to approximate degrees (1 degree ≈ 111km)
    let buffer_degrees = buffer_meters / 111_000.0;
    log::info!("Pass 2: nearest within {buffer_meters}m buffer (~{buffer_degrees:.6} deg)...");

    for state in &states {
        let updated = db
            .exec_raw_params(
                &format!(
                    "UPDATE crime_incidents i \
                     SET census_place_geoid = sub.geoid \
                     FROM ( \
                         SELECT DISTINCT ON (i2.id) i2.id, p.geoid \
                         FROM crime_incidents i2 \
                         JOIN census_places p \
                           ON p.state_abbr = i2.state \
                          AND ST_DWithin(p.boundary::geometry, i2.location::geometry, $1) \
                         WHERE i2.census_place_geoid IS NULL \
                           AND i2.has_coordinates = TRUE \
                           AND i2.state = $2{filter_i2} \
                         ORDER BY i2.id, ST_Distance(p.boundary::geometry, i2.location::geometry) \
                     ) sub \
                     WHERE i.id = sub.id{filter_i}"
                ),
                &[
                    DatabaseValue::Real64(buffer_degrees),
                    DatabaseValue::String(state.clone()),
                ],
            )
            .await?;

        total += updated;
        if updated > 0 {
            log::info!("  {state}: {updated} incidents (buffer), {total} total");
        }
        if let Some(ref p) = progress {
            p.inc(updated);
        }
    }

    // Mark remaining unattributed rows as done for progress
    if let Some(ref p) = progress {
        #[allow(clippy::cast_sign_loss)]
        let remaining = (unattributed as u64).saturating_sub(total);
        if remaining > 0 {
            p.inc(remaining);
        }
        p.finish(format!("Place attribution complete: {total} incidents"));
    }

    Ok(total)
}

/// Assigns census tract GEOIDs to incidents that have coordinates but no
/// `census_tract_geoid` yet.
///
/// Performance optimizations:
/// - Casts `geography` to `geometry` for planar `ST_Covers` (10–50× faster).
/// - Batches by state so each spatial join only compares incidents against
///   tracts in the same state.
/// - Census tracts tile the US without overlap, so no `DISTINCT ON` is needed.
///
/// # Errors
///
/// Returns [`DbError`] if the database operation fails.
#[allow(clippy::too_many_lines)]
pub async fn attribute_tracts(
    db: &dyn Database,
    _batch_size: u32,
    source_ids: Option<&[i32]>,
    progress: Option<Arc<dyn ProgressCallback>>,
) -> Result<u64, DbError> {
    let mut total = 0u64;

    // Build source filter clauses for different query contexts:
    // - bare: no table alias (for COUNT / DISTINCT queries)
    // - i:    outer UPDATE alias (prevents deadlocks by scoping row locks)
    let filter_bare = build_source_filter(source_ids, "");
    let filter_i = build_source_filter(source_ids, "i");

    // Boost work_mem for this session to allow efficient hash joins
    db.exec_raw("SET work_mem = '256MB'").await?;

    // Ensure partial index exists for fast lookup of unattributed rows
    db.exec_raw(
        "CREATE INDEX IF NOT EXISTS idx_incidents_unattr_tract \
         ON crime_incidents (id) \
         WHERE census_tract_geoid IS NULL AND has_coordinates = TRUE",
    )
    .await?;

    // Query unattributed count for progress reporting
    let unattributed: i64 = {
        let rows = db
            .query_raw_params(
                &format!(
                    "SELECT COUNT(*) as cnt FROM crime_incidents \
                     WHERE census_tract_geoid IS NULL AND has_coordinates = TRUE{filter_bare}"
                ),
                &[],
            )
            .await?;
        rows.first().map_or(0, |r| r.to_value("cnt").unwrap_or(0))
    };

    if let Some(ref p) = progress {
        #[allow(clippy::cast_sign_loss)]
        p.set_total(unattributed as u64);
    }

    if unattributed == 0 {
        if let Some(ref p) = progress {
            p.finish("Tract attribution complete: 0 incidents (already done)".to_string());
        }
        return Ok(0);
    }

    // Get distinct states that have unattributed incidents
    let state_rows = db
        .query_raw_params(
            &format!(
                "SELECT DISTINCT state FROM crime_incidents \
                 WHERE census_tract_geoid IS NULL AND has_coordinates = TRUE{filter_bare} \
                 ORDER BY state"
            ),
            &[],
        )
        .await?;

    let states: Vec<String> = state_rows
        .iter()
        .filter_map(|r| {
            let s: String = r.to_value("state").unwrap_or_default();
            if s.is_empty() { None } else { Some(s) }
        })
        .collect();

    log::info!(
        "Bulk tract attribution (geometry cast + state batching) for ~{unattributed} incidents across {} states...",
        states.len()
    );

    // Process each state separately — tracts don't overlap so no DISTINCT ON
    // The source filter on the outer UPDATE (filter_i) ensures PostgreSQL only
    // locks rows belonging to this partition's sources, preventing deadlocks
    // when multiple partition jobs run concurrently.
    for state in &states {
        let updated = db
            .exec_raw_params(
                &format!(
                    "UPDATE crime_incidents i \
                     SET census_tract_geoid = ct.geoid \
                     FROM census_tracts ct \
                     WHERE ct.state_abbr = i.state \
                       AND ST_Covers(ct.boundary::geometry, i.location::geometry) \
                       AND i.census_tract_geoid IS NULL \
                       AND i.has_coordinates = TRUE \
                       AND i.state = $1{filter_i}"
                ),
                &[DatabaseValue::String(state.clone())],
            )
            .await?;

        total += updated;
        log::info!("  {state}: {updated} incidents attributed, {total} total");
        if let Some(ref p) = progress {
            p.inc(updated);
        }
    }

    if let Some(ref p) = progress {
        #[allow(clippy::cast_sign_loss)]
        let remaining = (unattributed as u64).saturating_sub(total);
        if remaining > 0 {
            p.inc(remaining);
        }
        p.finish(format!("Tract attribution complete: {total} incidents"));
    }

    Ok(total)
}
