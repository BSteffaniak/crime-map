//! Discovery database management.
//!
//! Opens (or creates) a `SQLite` database for tracking discovery leads,
//! verified sources, search history, legal information, scrape targets,
//! geocoding candidates, and API patterns. Uses `switchy_database` for
//! all database operations following the same patterns as the generate
//! package.

// Many CRUD functions are defined ahead of their CLI command wiring.
#![allow(dead_code)]

use std::path::Path;

use crime_map_discover_models::{
    ApiPattern, GeocodingCandidate, Lead, LegalInfo, ScrapeTarget, SearchEntry, Source,
};
use moosicbox_json_utils::database::ToValue as _;
use switchy_database::{Database, DatabaseValue};
use switchy_database_connection::init_sqlite_rusqlite;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur during discovery database operations.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// A database query or command failed.
    #[error("Database error: {0}")]
    Database(String),

    /// An I/O operation failed (e.g., creating the database file).
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

// ---------------------------------------------------------------------------
// Status summary
// ---------------------------------------------------------------------------

/// Aggregate counts of leads and sources by status.
#[derive(Debug, Clone)]
pub struct StatusSummary {
    /// Total number of leads across all statuses.
    pub total_leads: i64,
    /// Leads with status `new`.
    pub new_leads: i64,
    /// Leads with status `investigating`.
    pub investigating_leads: i64,
    /// Leads with status `verified_good`.
    pub verified_good_leads: i64,
    /// Leads with status `integrated`.
    pub integrated_leads: i64,
    /// Leads with status `rejected`.
    pub rejected_leads: i64,
    /// Total number of tracked sources.
    pub total_sources: i64,
    /// Sources with status `active`.
    pub active_sources: i64,
    /// Total number of search history entries.
    pub total_searches: i64,
}

// ---------------------------------------------------------------------------
// Database lifecycle
// ---------------------------------------------------------------------------

/// Opens (or creates) the discovery `SQLite` database at the given path and
/// ensures all tables exist.
///
/// # Errors
///
/// Returns [`DbError`] if the database file cannot be created or the schema
/// DDL fails.
pub async fn open_db(path: &Path) -> Result<Box<dyn Database>, DbError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let db = init_sqlite_rusqlite(Some(path)).map_err(|e| DbError::Database(e.to_string()))?;

    ensure_schema(db.as_ref()).await?;

    Ok(db)
}

/// Creates all tables if they don't already exist.
#[allow(clippy::too_many_lines)]
async fn ensure_schema(db: &dyn Database) -> Result<(), DbError> {
    db.exec_raw(
        "CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            jurisdiction TEXT NOT NULL,
            source_name TEXT NOT NULL,
            api_type TEXT,
            url TEXT,
            status TEXT NOT NULL DEFAULT 'new',
            priority TEXT NOT NULL DEFAULT 'medium',
            likelihood REAL,
            record_count INTEGER,
            has_coordinates INTEGER,
            has_dates INTEGER,
            coordinate_type TEXT,
            date_format TEXT,
            sample_record TEXT,
            field_notes TEXT,
            distance_from_dc_miles REAL,
            notes TEXT,
            discovered_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            investigated_at TEXT
        )",
    )
    .await
    .map_err(|e| DbError::Database(e.to_string()))?;

    db.exec_raw(
        "CREATE TABLE IF NOT EXISTS sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL UNIQUE,
            jurisdiction TEXT NOT NULL,
            api_type TEXT NOT NULL,
            url TEXT NOT NULL,
            record_count INTEGER,
            date_range_start TEXT,
            date_range_end TEXT,
            toml_filename TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            last_verified TEXT,
            notes TEXT
        )",
    )
    .await
    .map_err(|e| DbError::Database(e.to_string()))?;

    db.exec_raw(
        "CREATE TABLE IF NOT EXISTS search_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            search_type TEXT NOT NULL,
            query TEXT NOT NULL,
            geographic_scope TEXT,
            results_summary TEXT,
            searched_at TEXT NOT NULL,
            session_id TEXT
        )",
    )
    .await
    .map_err(|e| DbError::Database(e.to_string()))?;

    db.exec_raw(
        "CREATE TABLE IF NOT EXISTS legal_info (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER REFERENCES leads(id),
            source_id INTEGER REFERENCES sources(id),
            license_type TEXT,
            tos_url TEXT,
            allows_bulk_download INTEGER,
            allows_api_access INTEGER,
            allows_redistribution INTEGER,
            allows_scraping INTEGER,
            attribution_required INTEGER,
            attribution_text TEXT,
            rate_limits TEXT,
            notes TEXT,
            reviewed_at TEXT
        )",
    )
    .await
    .map_err(|e| DbError::Database(e.to_string()))?;

    db.exec_raw(
        "CREATE TABLE IF NOT EXISTS scrape_targets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL REFERENCES leads(id),
            url TEXT NOT NULL,
            scrape_strategy TEXT,
            pagination_method TEXT,
            auth_required INTEGER NOT NULL DEFAULT 0,
            anti_bot TEXT,
            estimated_effort TEXT,
            notes TEXT,
            created_at TEXT NOT NULL
        )",
    )
    .await
    .map_err(|e| DbError::Database(e.to_string()))?;

    db.exec_raw(
        "CREATE TABLE IF NOT EXISTS geocoding_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL REFERENCES leads(id),
            address_fields TEXT NOT NULL,
            city_field TEXT,
            state_field TEXT,
            zip_field TEXT,
            sample_addresses TEXT,
            geocode_quality TEXT,
            estimated_geocode_rate REAL,
            geocoder_notes TEXT,
            created_at TEXT NOT NULL
        )",
    )
    .await
    .map_err(|e| DbError::Database(e.to_string()))?;

    db.exec_raw(
        "CREATE TABLE IF NOT EXISTS api_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern_name TEXT NOT NULL UNIQUE,
            discovery_strategy TEXT NOT NULL,
            typical_fields TEXT,
            typical_issues TEXT,
            quality_rating TEXT,
            notes TEXT
        )",
    )
    .await
    .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Converts an `Option<&str>` to a [`DatabaseValue`], using `Null` for `None`.
fn opt_str(value: Option<&str>) -> DatabaseValue {
    value.map_or(DatabaseValue::Null, |s| {
        DatabaseValue::String(s.to_string())
    })
}

/// Converts an `Option<f64>` to a [`DatabaseValue`], using `Null` for `None`.
fn opt_f64(value: Option<f64>) -> DatabaseValue {
    value.map_or(DatabaseValue::Null, DatabaseValue::Real64)
}

/// Converts an `Option<i64>` to a [`DatabaseValue`], using `Null` for `None`.
fn opt_i64(value: Option<i64>) -> DatabaseValue {
    value.map_or(DatabaseValue::Null, DatabaseValue::Int64)
}

/// Converts an `Option<bool>` to a [`DatabaseValue`] integer (1/0), using
/// `Null` for `None`.
fn opt_bool(value: Option<bool>) -> DatabaseValue {
    value.map_or(DatabaseValue::Null, |b| DatabaseValue::Int64(i64::from(b)))
}

/// Reads an `INTEGER` column as an `Option<bool>` (1 = true, 0 = false).
fn row_opt_bool(row: &switchy_database::Row, col: &str) -> Option<bool> {
    row.to_value::<Option<i64>>(col)
        .unwrap_or(None)
        .map(|v| v != 0)
}

/// Extracts an `i64` from the `id` column of the first row returned by a
/// `RETURNING id` clause.
fn returning_id(rows: &[switchy_database::Row]) -> i64 {
    rows.first()
        .and_then(|r| r.to_value("id").ok())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Leads CRUD
// ---------------------------------------------------------------------------

/// Inserts a new discovery lead and returns its auto-generated ID.
///
/// The `status` is set to `new` and timestamps are set to the current UTC
/// time.
///
/// # Errors
///
/// Returns [`DbError`] if the insert fails.
#[allow(clippy::too_many_arguments)]
pub async fn insert_lead(
    db: &dyn Database,
    jurisdiction: &str,
    source_name: &str,
    api_type: Option<&str>,
    url: Option<&str>,
    priority: &str,
    likelihood: Option<f64>,
    notes: Option<&str>,
) -> Result<i64, DbError> {
    let now = chrono::Utc::now().to_rfc3339();

    let rows = db
        .query_raw_params(
            "INSERT INTO leads (jurisdiction, source_name, api_type, url, status, priority,
                 likelihood, notes, discovered_at, updated_at)
             VALUES (?, ?, ?, ?, 'new', ?, ?, ?, ?, ?)
             RETURNING id",
            &[
                DatabaseValue::String(jurisdiction.to_string()),
                DatabaseValue::String(source_name.to_string()),
                opt_str(api_type),
                opt_str(url),
                DatabaseValue::String(priority.to_string()),
                opt_f64(likelihood),
                opt_str(notes),
                DatabaseValue::String(now.clone()),
                DatabaseValue::String(now),
            ],
        )
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(returning_id(&rows))
}

/// Updates the status of an existing lead by ID.
///
/// Also bumps `updated_at` to the current UTC time. If the new status is
/// `investigating`, sets `investigated_at` as well.
///
/// # Errors
///
/// Returns [`DbError`] if the update fails.
pub async fn update_lead_status(db: &dyn Database, id: i64, status: &str) -> Result<(), DbError> {
    let now = chrono::Utc::now().to_rfc3339();

    if status == "investigating" {
        db.exec_raw_params(
            "UPDATE leads SET status = ?, updated_at = ?, investigated_at = ? WHERE id = ?",
            &[
                DatabaseValue::String(status.to_string()),
                DatabaseValue::String(now.clone()),
                DatabaseValue::String(now),
                DatabaseValue::Int64(id),
            ],
        )
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;
    } else {
        db.exec_raw_params(
            "UPDATE leads SET status = ?, updated_at = ? WHERE id = ?",
            &[
                DatabaseValue::String(status.to_string()),
                DatabaseValue::String(now),
                DatabaseValue::Int64(id),
            ],
        )
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;
    }

    Ok(())
}

/// Retrieves leads, optionally filtered by status.
///
/// When `status_filter` is `None`, all leads are returned. Results are
/// ordered by `discovered_at` descending (newest first).
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub async fn get_leads(
    db: &dyn Database,
    status_filter: Option<&str>,
) -> Result<Vec<Lead>, DbError> {
    let (sql, params) = status_filter.map_or_else(
        || {
            (
                "SELECT * FROM leads ORDER BY discovered_at DESC".to_string(),
                Vec::new(),
            )
        },
        |status| {
            (
                "SELECT * FROM leads WHERE status = ? ORDER BY discovered_at DESC".to_string(),
                vec![DatabaseValue::String(status.to_string())],
            )
        },
    );

    let rows = db
        .query_raw_params(&sql, &params)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(rows.iter().map(row_to_lead).collect())
}

/// Retrieves a single lead by ID.
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub async fn get_lead(db: &dyn Database, id: i64) -> Result<Option<Lead>, DbError> {
    let rows = db
        .query_raw_params(
            "SELECT * FROM leads WHERE id = ?",
            &[DatabaseValue::Int64(id)],
        )
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(rows.first().map(row_to_lead))
}

/// Converts a database row into a [`Lead`].
fn row_to_lead(row: &switchy_database::Row) -> Lead {
    Lead {
        id: row.to_value("id").unwrap_or(0),
        jurisdiction: row.to_value("jurisdiction").unwrap_or_default(),
        source_name: row.to_value("source_name").unwrap_or_default(),
        api_type: row
            .to_value::<Option<String>>("api_type")
            .unwrap_or(None)
            .and_then(|s| s.parse().ok()),
        url: row.to_value("url").unwrap_or(None),
        status: row
            .to_value::<String>("status")
            .unwrap_or_default()
            .parse()
            .unwrap_or(crime_map_discover_models::LeadStatus::New),
        priority: row
            .to_value::<String>("priority")
            .unwrap_or_default()
            .parse()
            .unwrap_or(crime_map_discover_models::Priority::Medium),
        likelihood: row.to_value("likelihood").unwrap_or(None),
        record_count: row.to_value("record_count").unwrap_or(None),
        has_coordinates: row_opt_bool(row, "has_coordinates"),
        has_dates: row_opt_bool(row, "has_dates"),
        coordinate_type: row
            .to_value::<Option<String>>("coordinate_type")
            .unwrap_or(None)
            .and_then(|s| s.parse().ok()),
        date_format: row.to_value("date_format").unwrap_or(None),
        sample_record: row.to_value("sample_record").unwrap_or(None),
        field_notes: row.to_value("field_notes").unwrap_or(None),
        distance_from_dc_miles: row.to_value("distance_from_dc_miles").unwrap_or(None),
        notes: row.to_value("notes").unwrap_or(None),
        discovered_at: row.to_value("discovered_at").unwrap_or_default(),
        updated_at: row.to_value("updated_at").unwrap_or_default(),
        investigated_at: row.to_value("investigated_at").unwrap_or(None),
    }
}

/// Updates multiple fields on an existing lead by ID.
///
/// Only fields that are `Some` are updated; `None` fields are left unchanged.
/// Always bumps `updated_at` to the current UTC time.
///
/// # Errors
///
/// Returns [`DbError`] if the update fails.
#[allow(clippy::too_many_arguments)]
pub async fn update_lead(
    db: &dyn Database,
    id: i64,
    status: Option<&str>,
    record_count: Option<i64>,
    has_coordinates: Option<bool>,
    notes: Option<&str>,
) -> Result<(), DbError> {
    let now = chrono::Utc::now().to_rfc3339();
    let mut set_clauses = vec!["updated_at = ?".to_string()];
    let mut params: Vec<DatabaseValue> = vec![DatabaseValue::String(now.clone())];

    if let Some(s) = status {
        set_clauses.push("status = ?".to_string());
        params.push(DatabaseValue::String(s.to_string()));
        if s == "investigating" {
            set_clauses.push("investigated_at = ?".to_string());
            params.push(DatabaseValue::String(now));
        }
    }
    if let Some(count) = record_count {
        set_clauses.push("record_count = ?".to_string());
        params.push(DatabaseValue::Int64(count));
    }
    if let Some(coords) = has_coordinates {
        set_clauses.push("has_coordinates = ?".to_string());
        params.push(DatabaseValue::Int64(i64::from(coords)));
    }
    if let Some(n) = notes {
        set_clauses.push("notes = ?".to_string());
        params.push(DatabaseValue::String(n.to_string()));
    }

    params.push(DatabaseValue::Int64(id));
    let sql = format!("UPDATE leads SET {} WHERE id = ?", set_clauses.join(", "));

    db.exec_raw_params(&sql, &params)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(())
}

/// Retrieves all legal information records, ordered by review date descending.
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub async fn get_all_legal(db: &dyn Database) -> Result<Vec<LegalInfo>, DbError> {
    let rows = db
        .query_raw_params("SELECT * FROM legal_info ORDER BY reviewed_at DESC", &[])
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(rows.iter().map(row_to_legal).collect())
}

/// Retrieves a single legal information record by ID.
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub async fn get_legal(db: &dyn Database, id: i64) -> Result<Option<LegalInfo>, DbError> {
    let rows = db
        .query_raw_params(
            "SELECT * FROM legal_info WHERE id = ?",
            &[DatabaseValue::Int64(id)],
        )
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(rows.first().map(row_to_legal))
}

// ---------------------------------------------------------------------------
// Sources CRUD
// ---------------------------------------------------------------------------

/// Inserts a new tracked source and returns its auto-generated ID.
///
/// # Errors
///
/// Returns [`DbError`] if the insert fails (e.g., duplicate `source_id`).
#[allow(clippy::too_many_arguments)]
pub async fn insert_source(
    db: &dyn Database,
    source_id: &str,
    jurisdiction: &str,
    api_type: &str,
    url: &str,
    record_count: Option<i64>,
    date_range_start: Option<&str>,
    date_range_end: Option<&str>,
    toml_filename: Option<&str>,
    notes: Option<&str>,
) -> Result<i64, DbError> {
    let rows = db
        .query_raw_params(
            "INSERT INTO sources (source_id, jurisdiction, api_type, url, record_count,
                 date_range_start, date_range_end, toml_filename, status, notes)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)
             RETURNING id",
            &[
                DatabaseValue::String(source_id.to_string()),
                DatabaseValue::String(jurisdiction.to_string()),
                DatabaseValue::String(api_type.to_string()),
                DatabaseValue::String(url.to_string()),
                opt_i64(record_count),
                opt_str(date_range_start),
                opt_str(date_range_end),
                opt_str(toml_filename),
                opt_str(notes),
            ],
        )
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(returning_id(&rows))
}

/// Retrieves sources, optionally filtered by status.
///
/// When `status_filter` is `None`, all sources are returned. Results are
/// ordered by `jurisdiction` ascending.
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub async fn get_sources(
    db: &dyn Database,
    status_filter: Option<&str>,
) -> Result<Vec<Source>, DbError> {
    let (sql, params) = status_filter.map_or_else(
        || {
            (
                "SELECT * FROM sources ORDER BY jurisdiction ASC".to_string(),
                Vec::new(),
            )
        },
        |status| {
            (
                "SELECT * FROM sources WHERE status = ? ORDER BY jurisdiction ASC".to_string(),
                vec![DatabaseValue::String(status.to_string())],
            )
        },
    );

    let rows = db
        .query_raw_params(&sql, &params)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(rows.iter().map(row_to_source).collect())
}

/// Converts a database row into a [`Source`].
fn row_to_source(row: &switchy_database::Row) -> Source {
    Source {
        id: row.to_value("id").unwrap_or(0),
        source_id: row.to_value("source_id").unwrap_or_default(),
        jurisdiction: row.to_value("jurisdiction").unwrap_or_default(),
        api_type: row
            .to_value::<String>("api_type")
            .unwrap_or_default()
            .parse()
            .unwrap_or(crime_map_discover_models::ApiType::Unknown),
        url: row.to_value("url").unwrap_or_default(),
        record_count: row.to_value("record_count").unwrap_or(None),
        date_range_start: row.to_value("date_range_start").unwrap_or(None),
        date_range_end: row.to_value("date_range_end").unwrap_or(None),
        toml_filename: row.to_value("toml_filename").unwrap_or(None),
        status: row
            .to_value::<String>("status")
            .unwrap_or_default()
            .parse()
            .unwrap_or(crime_map_discover_models::SourceStatus::Active),
        last_verified: row.to_value("last_verified").unwrap_or(None),
        notes: row.to_value("notes").unwrap_or(None),
    }
}

// ---------------------------------------------------------------------------
// Search history CRUD
// ---------------------------------------------------------------------------

/// Records a discovery search and returns its auto-generated ID.
///
/// # Errors
///
/// Returns [`DbError`] if the insert fails.
pub async fn insert_search(
    db: &dyn Database,
    search_type: &str,
    query: &str,
    geographic_scope: Option<&str>,
    results_summary: Option<&str>,
    session_id: Option<&str>,
) -> Result<i64, DbError> {
    let now = chrono::Utc::now().to_rfc3339();

    let rows = db
        .query_raw_params(
            "INSERT INTO search_history (search_type, query, geographic_scope,
                 results_summary, searched_at, session_id)
             VALUES (?, ?, ?, ?, ?, ?)
             RETURNING id",
            &[
                DatabaseValue::String(search_type.to_string()),
                DatabaseValue::String(query.to_string()),
                opt_str(geographic_scope),
                opt_str(results_summary),
                DatabaseValue::String(now),
                opt_str(session_id),
            ],
        )
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(returning_id(&rows))
}

/// Retrieves search history entries, optionally filtered by search type.
///
/// When `search_type_filter` is `None`, all entries are returned. Results
/// are ordered by `searched_at` descending (most recent first).
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub async fn get_searches(
    db: &dyn Database,
    search_type_filter: Option<&str>,
) -> Result<Vec<SearchEntry>, DbError> {
    let (sql, params) = search_type_filter.map_or_else(
        || {
            (
                "SELECT * FROM search_history ORDER BY searched_at DESC".to_string(),
                Vec::new(),
            )
        },
        |st| {
            (
                "SELECT * FROM search_history WHERE search_type = ? ORDER BY searched_at DESC"
                    .to_string(),
                vec![DatabaseValue::String(st.to_string())],
            )
        },
    );

    let rows = db
        .query_raw_params(&sql, &params)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(rows.iter().map(row_to_search).collect())
}

/// Converts a database row into a [`SearchEntry`].
fn row_to_search(row: &switchy_database::Row) -> SearchEntry {
    SearchEntry {
        id: row.to_value("id").unwrap_or(0),
        search_type: row.to_value("search_type").unwrap_or_default(),
        query: row.to_value("query").unwrap_or_default(),
        geographic_scope: row.to_value("geographic_scope").unwrap_or(None),
        results_summary: row.to_value("results_summary").unwrap_or(None),
        searched_at: row.to_value("searched_at").unwrap_or_default(),
        session_id: row.to_value("session_id").unwrap_or(None),
    }
}

// ---------------------------------------------------------------------------
// Legal info CRUD
// ---------------------------------------------------------------------------

/// Inserts a legal information record and returns its auto-generated ID.
///
/// # Errors
///
/// Returns [`DbError`] if the insert fails.
#[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
pub async fn insert_legal(
    db: &dyn Database,
    lead_id: Option<i64>,
    source_id: Option<i64>,
    license_type: Option<&str>,
    tos_url: Option<&str>,
    allows_bulk_download: Option<bool>,
    allows_api_access: Option<bool>,
    allows_redistribution: Option<bool>,
    allows_scraping: Option<bool>,
    attribution_required: Option<bool>,
    attribution_text: Option<&str>,
    rate_limits: Option<&str>,
    notes: Option<&str>,
) -> Result<i64, DbError> {
    let now = chrono::Utc::now().to_rfc3339();

    let rows = db
        .query_raw_params(
            "INSERT INTO legal_info (lead_id, source_id, license_type, tos_url,
                 allows_bulk_download, allows_api_access, allows_redistribution,
                 allows_scraping, attribution_required, attribution_text,
                 rate_limits, notes, reviewed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             RETURNING id",
            &[
                opt_i64(lead_id),
                opt_i64(source_id),
                opt_str(license_type),
                opt_str(tos_url),
                opt_bool(allows_bulk_download),
                opt_bool(allows_api_access),
                opt_bool(allows_redistribution),
                opt_bool(allows_scraping),
                opt_bool(attribution_required),
                opt_str(attribution_text),
                opt_str(rate_limits),
                opt_str(notes),
                DatabaseValue::String(now),
            ],
        )
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(returning_id(&rows))
}

/// Retrieves all legal information records associated with a lead.
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub async fn get_legal_for_lead(
    db: &dyn Database,
    lead_id: i64,
) -> Result<Vec<LegalInfo>, DbError> {
    let rows = db
        .query_raw_params(
            "SELECT * FROM legal_info WHERE lead_id = ? ORDER BY reviewed_at DESC",
            &[DatabaseValue::Int64(lead_id)],
        )
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(rows.iter().map(row_to_legal).collect())
}

/// Converts a database row into a [`LegalInfo`].
fn row_to_legal(row: &switchy_database::Row) -> LegalInfo {
    LegalInfo {
        id: row.to_value("id").unwrap_or(0),
        lead_id: row.to_value("lead_id").unwrap_or(None),
        source_id: row.to_value("source_id").unwrap_or(None),
        license_type: row
            .to_value::<Option<String>>("license_type")
            .unwrap_or(None)
            .and_then(|s| s.parse().ok()),
        tos_url: row.to_value("tos_url").unwrap_or(None),
        allows_bulk_download: row_opt_bool(row, "allows_bulk_download"),
        allows_api_access: row_opt_bool(row, "allows_api_access"),
        allows_redistribution: row_opt_bool(row, "allows_redistribution"),
        allows_scraping: row_opt_bool(row, "allows_scraping"),
        attribution_required: row_opt_bool(row, "attribution_required"),
        attribution_text: row.to_value("attribution_text").unwrap_or(None),
        rate_limits: row.to_value("rate_limits").unwrap_or(None),
        notes: row.to_value("notes").unwrap_or(None),
        reviewed_at: row.to_value("reviewed_at").unwrap_or(None),
    }
}

// ---------------------------------------------------------------------------
// Scrape targets CRUD
// ---------------------------------------------------------------------------

/// Inserts a new scrape target and returns its auto-generated ID.
///
/// # Errors
///
/// Returns [`DbError`] if the insert fails.
#[allow(clippy::too_many_arguments)]
pub async fn insert_scrape_target(
    db: &dyn Database,
    lead_id: i64,
    url: &str,
    scrape_strategy: Option<&str>,
    pagination_method: Option<&str>,
    auth_required: bool,
    anti_bot: Option<&str>,
    estimated_effort: Option<&str>,
    notes: Option<&str>,
) -> Result<i64, DbError> {
    let now = chrono::Utc::now().to_rfc3339();

    let rows = db
        .query_raw_params(
            "INSERT INTO scrape_targets (lead_id, url, scrape_strategy, pagination_method,
                 auth_required, anti_bot, estimated_effort, notes, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             RETURNING id",
            &[
                DatabaseValue::Int64(lead_id),
                DatabaseValue::String(url.to_string()),
                opt_str(scrape_strategy),
                opt_str(pagination_method),
                DatabaseValue::Int64(i64::from(auth_required)),
                opt_str(anti_bot),
                opt_str(estimated_effort),
                opt_str(notes),
                DatabaseValue::String(now),
            ],
        )
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(returning_id(&rows))
}

/// Retrieves all scrape targets ordered by creation time descending.
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub async fn get_scrape_targets(db: &dyn Database) -> Result<Vec<ScrapeTarget>, DbError> {
    let rows = db
        .query_raw_params("SELECT * FROM scrape_targets ORDER BY created_at DESC", &[])
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(rows.iter().map(row_to_scrape_target).collect())
}

/// Converts a database row into a [`ScrapeTarget`].
fn row_to_scrape_target(row: &switchy_database::Row) -> ScrapeTarget {
    ScrapeTarget {
        id: row.to_value("id").unwrap_or(0),
        lead_id: row.to_value("lead_id").unwrap_or(0),
        url: row.to_value("url").unwrap_or_default(),
        scrape_strategy: row
            .to_value::<Option<String>>("scrape_strategy")
            .unwrap_or(None)
            .and_then(|s| s.parse().ok()),
        pagination_method: row.to_value("pagination_method").unwrap_or(None),
        auth_required: row
            .to_value::<i64>("auth_required")
            .map(|v| v != 0)
            .unwrap_or(false),
        anti_bot: row
            .to_value::<Option<String>>("anti_bot")
            .unwrap_or(None)
            .and_then(|s| s.parse().ok()),
        estimated_effort: row.to_value("estimated_effort").unwrap_or(None),
        notes: row.to_value("notes").unwrap_or(None),
        created_at: row.to_value("created_at").unwrap_or_default(),
    }
}

// ---------------------------------------------------------------------------
// Geocoding candidates CRUD (read helpers)
// ---------------------------------------------------------------------------

/// Retrieves all geocoding candidates, ordered by creation time descending.
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub async fn get_geocoding_candidates(
    db: &dyn Database,
) -> Result<Vec<GeocodingCandidate>, DbError> {
    let rows = db
        .query_raw_params(
            "SELECT * FROM geocoding_candidates ORDER BY created_at DESC",
            &[],
        )
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(rows.iter().map(row_to_geocoding_candidate).collect())
}

/// Converts a database row into a [`GeocodingCandidate`].
fn row_to_geocoding_candidate(row: &switchy_database::Row) -> GeocodingCandidate {
    GeocodingCandidate {
        id: row.to_value("id").unwrap_or(0),
        lead_id: row.to_value("lead_id").unwrap_or(0),
        address_fields: row.to_value("address_fields").unwrap_or_default(),
        city_field: row.to_value("city_field").unwrap_or(None),
        state_field: row.to_value("state_field").unwrap_or(None),
        zip_field: row.to_value("zip_field").unwrap_or(None),
        sample_addresses: row.to_value("sample_addresses").unwrap_or(None),
        geocode_quality: row
            .to_value::<Option<String>>("geocode_quality")
            .unwrap_or(None)
            .and_then(|s| s.parse().ok()),
        estimated_geocode_rate: row.to_value("estimated_geocode_rate").unwrap_or(None),
        geocoder_notes: row.to_value("geocoder_notes").unwrap_or(None),
        created_at: row.to_value("created_at").unwrap_or_default(),
    }
}

// ---------------------------------------------------------------------------
// API patterns CRUD (read helpers)
// ---------------------------------------------------------------------------

/// Retrieves all API patterns, ordered by pattern name.
///
/// # Errors
///
/// Returns [`DbError`] if the query fails.
pub async fn get_api_patterns(db: &dyn Database) -> Result<Vec<ApiPattern>, DbError> {
    let rows = db
        .query_raw_params("SELECT * FROM api_patterns ORDER BY pattern_name ASC", &[])
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    Ok(rows.iter().map(row_to_api_pattern).collect())
}

/// Converts a database row into an [`ApiPattern`].
fn row_to_api_pattern(row: &switchy_database::Row) -> ApiPattern {
    ApiPattern {
        id: row.to_value("id").unwrap_or(0),
        pattern_name: row.to_value("pattern_name").unwrap_or_default(),
        discovery_strategy: row.to_value("discovery_strategy").unwrap_or_default(),
        typical_fields: row.to_value("typical_fields").unwrap_or(None),
        typical_issues: row.to_value("typical_issues").unwrap_or(None),
        quality_rating: row.to_value("quality_rating").unwrap_or(None),
        notes: row.to_value("notes").unwrap_or(None),
    }
}

// ---------------------------------------------------------------------------
// Status summary
// ---------------------------------------------------------------------------

/// Executes a `SELECT COUNT(*) AS cnt` query and returns the count.
async fn count_query(db: &dyn Database, sql: &str) -> Result<i64, DbError> {
    let rows = db
        .query_raw_params(sql, &[])
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

    let n: i64 = rows
        .first()
        .and_then(|r| r.to_value("cnt").ok())
        .unwrap_or(0);

    Ok(n)
}

/// Computes aggregate counts of leads, sources, and searches by status.
///
/// # Errors
///
/// Returns [`DbError`] if any of the underlying queries fail.
pub async fn get_status_summary(db: &dyn Database) -> Result<StatusSummary, DbError> {
    let total_leads = count_query(db, "SELECT COUNT(*) AS cnt FROM leads").await?;
    let new_leads =
        count_query(db, "SELECT COUNT(*) AS cnt FROM leads WHERE status = 'new'").await?;
    let investigating_leads = count_query(
        db,
        "SELECT COUNT(*) AS cnt FROM leads WHERE status = 'investigating'",
    )
    .await?;
    let verified_good_leads = count_query(
        db,
        "SELECT COUNT(*) AS cnt FROM leads WHERE status = 'verified_good'",
    )
    .await?;
    let integrated_leads = count_query(
        db,
        "SELECT COUNT(*) AS cnt FROM leads WHERE status = 'integrated'",
    )
    .await?;
    let rejected_leads = count_query(
        db,
        "SELECT COUNT(*) AS cnt FROM leads WHERE status = 'rejected'",
    )
    .await?;
    let total_sources = count_query(db, "SELECT COUNT(*) AS cnt FROM sources").await?;
    let active_sources = count_query(
        db,
        "SELECT COUNT(*) AS cnt FROM sources WHERE status = 'active'",
    )
    .await?;
    let total_searches = count_query(db, "SELECT COUNT(*) AS cnt FROM search_history").await?;

    Ok(StatusSummary {
        total_leads,
        new_leads,
        investigating_leads,
        verified_good_leads,
        integrated_leads,
        rejected_leads,
        total_sources,
        active_sources,
        total_searches,
    })
}
