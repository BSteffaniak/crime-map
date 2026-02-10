//! Database query functions for crime data.
//!
//! Spatial queries use `query_raw_params()` with `PostGIS` functions.
//! Non-spatial queries use the typed `switchy_database` query builder.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crime_map_crime_models::{CrimeSeverity, CrimeSubcategory};
use crime_map_database_models::{IncidentQuery, IncidentRow};
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
) -> Result<i32, DbError> {
    let rows = db
        .query_raw_params(
            "INSERT INTO crime_sources (name, source_type, api_url, coverage_area)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (name) DO UPDATE SET
                 source_type = EXCLUDED.source_type,
                 api_url = EXCLUDED.api_url
             RETURNING id",
            &[
                DatabaseValue::String(name.to_string()),
                DatabaseValue::String(source_type.to_string()),
                api_url.map_or(DatabaseValue::Null, |u| {
                    DatabaseValue::String(u.to_string())
                }),
                DatabaseValue::String(coverage_area.to_string()),
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
/// Uses `ON CONFLICT` to skip duplicates based on
/// `(source_id, source_incident_id)`.
///
/// # Errors
///
/// Returns [`DbError`] if any database operation fails.
pub async fn insert_incidents(
    db: &dyn Database,
    source_id: i32,
    incidents: &[NormalizedIncident],
    category_ids: &BTreeMap<CrimeSubcategory, i32>,
) -> Result<u64, DbError> {
    let mut inserted = 0u64;

    for incident in incidents {
        let Some(&cat_id) = category_ids.get(&incident.subcategory) else {
            log::warn!(
                "No category ID for {:?}, skipping incident {}",
                incident.subcategory,
                incident.source_incident_id
            );
            continue;
        };

        let result = db
            .exec_raw_params(
                "INSERT INTO crime_incidents (
                    source_id, source_incident_id, category_id, location,
                    occurred_at, reported_at, description, block_address,
                    city, state, arrest_made, domestic, location_type
                ) VALUES (
                    $1, $2, $3,
                    ST_SetSRID(ST_MakePoint($4, $5), 4326)::geography,
                    $6, $7, $8, $9, $10, $11, $12, $13, $14
                )
                ON CONFLICT (source_id, source_incident_id) DO NOTHING",
                &[
                    DatabaseValue::Int32(source_id),
                    DatabaseValue::String(incident.source_incident_id.clone()),
                    DatabaseValue::Int32(cat_id),
                    DatabaseValue::Real64(incident.longitude),
                    DatabaseValue::Real64(incident.latitude),
                    DatabaseValue::DateTime(incident.occurred_at.naive_utc()),
                    incident
                        .reported_at
                        .as_ref()
                        .map_or(DatabaseValue::Null, |dt| {
                            DatabaseValue::DateTime(dt.naive_utc())
                        }),
                    incident
                        .description
                        .as_ref()
                        .map_or(DatabaseValue::Null, |d| DatabaseValue::String(d.clone())),
                    incident
                        .block_address
                        .as_ref()
                        .map_or(DatabaseValue::Null, |a| DatabaseValue::String(a.clone())),
                    DatabaseValue::String(incident.city.clone()),
                    DatabaseValue::String(incident.state.clone()),
                    incident
                        .arrest_made
                        .map_or(DatabaseValue::Null, DatabaseValue::Bool),
                    incident
                        .domestic
                        .map_or(DatabaseValue::Null, DatabaseValue::Bool),
                    incident
                        .location_type
                        .as_ref()
                        .map_or(DatabaseValue::Null, |l| DatabaseValue::String(l.clone())),
                ],
            )
            .await?;

        inserted += result;
    }

    Ok(inserted)
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
         WHERE 1=1",
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

        let occurred_at_naive: chrono::NaiveDateTime =
            row.to_value("occurred_at").unwrap_or_default();
        let occurred_at = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
            occurred_at_naive,
            chrono::Utc,
        );

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
