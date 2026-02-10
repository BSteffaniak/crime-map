//! HTTP handler functions for the crime map API.

use actix_web::{HttpResponse, web};
use crime_map_crime_models::{CrimeCategory, CrimeSubcategory};
use crime_map_database::queries;
use crime_map_database_models::{BoundingBox, IncidentQuery};
use crime_map_server_models::{
    ApiCategoryNode, ApiHealth, ApiIncident, ApiSubcategoryNode, IncidentQueryParams,
    SidebarIncident, SidebarQueryParams, SidebarResponse,
};
use moosicbox_json_utils::database::ToValue as _;
use switchy_database::{DatabaseValue, Row};

use crate::AppState;

/// `GET /api/health`
pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(ApiHealth {
        healthy: true,
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// `GET /api/categories`
///
/// Returns the hierarchical crime category taxonomy.
pub async fn categories() -> HttpResponse {
    let tree: Vec<ApiCategoryNode> = CrimeCategory::all()
        .iter()
        .map(|cat| {
            let children: Vec<ApiSubcategoryNode> = CrimeSubcategory::for_category(*cat)
                .into_iter()
                .map(|sub| ApiSubcategoryNode {
                    name: sub.to_string(),
                    severity: sub.severity().value(),
                })
                .collect();

            ApiCategoryNode {
                name: cat.to_string(),
                severity: cat.default_severity().value(),
                children,
            }
        })
        .collect();

    HttpResponse::Ok().json(tree)
}

/// `GET /api/incidents`
///
/// Queries incidents with bounding box, time range, and category filters.
pub async fn incidents(
    state: web::Data<AppState>,
    params: web::Query<IncidentQueryParams>,
) -> HttpResponse {
    let bbox = params.bbox.as_deref().and_then(parse_bbox);

    let categories: Vec<CrimeCategory> = params
        .categories
        .as_deref()
        .map(|s| s.split(',').filter_map(|c| c.trim().parse().ok()).collect())
        .unwrap_or_default();

    let subcategories: Vec<CrimeSubcategory> = params
        .subcategories
        .as_deref()
        .map(|s| s.split(',').filter_map(|c| c.trim().parse().ok()).collect())
        .unwrap_or_default();

    let severity_min = params
        .severity_min
        .and_then(|v| crime_map_crime_models::CrimeSeverity::from_value(v).ok());

    let query = IncidentQuery {
        bbox,
        from: params.from,
        to: params.to,
        categories,
        subcategories,
        severity_min,
        source_ids: Vec::new(),
        arrest_made: None,
        limit: params.limit.unwrap_or(100),
        offset: params.offset.unwrap_or(0),
    };

    match queries::query_incidents(state.db.as_ref(), &query).await {
        Ok(rows) => {
            let api_incidents: Vec<ApiIncident> = rows.into_iter().map(ApiIncident::from).collect();
            HttpResponse::Ok().json(api_incidents)
        }
        Err(e) => {
            log::error!("Failed to query incidents: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to query incidents"
            }))
        }
    }
}

/// `GET /api/sources`
///
/// Lists all configured data sources and their sync status.
pub async fn sources(state: web::Data<AppState>) -> HttpResponse {
    match state
        .db
        .query_raw_params("SELECT * FROM crime_sources ORDER BY name", &[])
        .await
    {
        Ok(rows) => {
            let sources: Vec<serde_json::Value> = rows
                .iter()
                .map(|row| {
                    let id: i32 = row.to_value("id").unwrap_or(0);
                    let name: String = row.to_value("name").unwrap_or_default();
                    let source_type: String = row.to_value("source_type").unwrap_or_default();
                    let record_count: i64 = row.to_value("record_count").unwrap_or(0);
                    let coverage_area: String = row.to_value("coverage_area").unwrap_or_default();
                    serde_json::json!({
                        "id": id,
                        "name": name,
                        "sourceType": source_type,
                        "recordCount": record_count,
                        "coverageArea": coverage_area,
                    })
                })
                .collect();
            HttpResponse::Ok().json(sources)
        }
        Err(e) => {
            log::error!("Failed to query sources: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to query sources"
            }))
        }
    }
}

/// `GET /api/sidebar`
///
/// Returns paginated crime incidents within a bounding box from the
/// pre-generated `SQLite` sidebar database. Supports filtering by date
/// range, category, subcategory, severity, and arrest status.
///
/// The features query walks the `occurred_at DESC` index in `SQLite` and
/// checks the bounding box inline, relying on `LIMIT` to short-circuit
/// early.
///
/// The count query uses the pre-aggregated `count_summary` table in
/// `DuckDB` for sub-10ms performance on any bounding box.
pub async fn sidebar(
    state: web::Data<AppState>,
    params: web::Query<SidebarQueryParams>,
) -> HttpResponse {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);
    let bbox = params.bbox.as_deref().and_then(parse_bbox);

    let (features_query, feature_params) =
        build_features_query(&params, bbox.as_ref(), limit, offset);

    let sidebar_db = state.sidebar_db.as_ref();

    let features_result = sidebar_db
        .query_raw_params(&features_query, &feature_params)
        .await;

    // Build and execute the DuckDB count query
    let count_db = state.count_db.clone();
    let count_params_owned = params.into_inner();
    let bbox_owned = bbox;

    let count_result = web::block(move || {
        let conn = count_db
            .lock()
            .map_err(|e| format!("Failed to lock DuckDB connection: {e}"))?;
        execute_duckdb_count(&conn, &count_params_owned, bbox_owned.as_ref())
    })
    .await;

    match (features_result, count_result) {
        (Ok(rows), Ok(Ok(total_count))) => {
            let features: Vec<SidebarIncident> = rows.iter().map(parse_sidebar_row).collect();

            #[allow(clippy::cast_possible_truncation)]
            let has_more = (u64::from(offset) + features.len() as u64) < total_count;

            HttpResponse::Ok().json(SidebarResponse {
                features,
                total_count,
                has_more,
            })
        }
        (Err(e), _) => {
            log::error!("Sidebar features query failed: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to query sidebar data"
            }))
        }
        (_, Ok(Err(e))) => {
            log::error!("Sidebar count query failed: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to query sidebar count"
            }))
        }
        (_, Err(e)) => {
            log::error!("Sidebar count query blocking error: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to query sidebar count"
            }))
        }
    }
}

/// Parses a `SQLite` sidebar row into a [`SidebarIncident`].
fn parse_sidebar_row(row: &Row) -> SidebarIncident {
    let arrest_int: Option<i32> = row.to_value("arrest_made").unwrap_or(None);

    SidebarIncident {
        id: row.to_value("id").unwrap_or(0),
        source_incident_id: row.to_value("source_incident_id").unwrap_or(None),
        subcategory: row.to_value("subcategory").unwrap_or_default(),
        category: row.to_value("category").unwrap_or_default(),
        severity: row.to_value("severity").unwrap_or(1),
        longitude: row.to_value("longitude").unwrap_or(0.0),
        latitude: row.to_value("latitude").unwrap_or(0.0),
        occurred_at: row.to_value("occurred_at").unwrap_or_default(),
        description: row.to_value("description").unwrap_or(None),
        block_address: row.to_value("block_address").unwrap_or(None),
        city: row.to_value("city").unwrap_or(None),
        state: row.to_value("state").unwrap_or(None),
        arrest_made: arrest_int.map(|v| v != 0),
        location_type: row.to_value("location_type").unwrap_or(None),
    }
}

/// Builds the features SQL query with parameter vector from the sidebar
/// query parameters. This query runs against `SQLite`.
///
/// Returns `(features_query, feature_params)`.
fn build_features_query(
    params: &SidebarQueryParams,
    bbox: Option<&BoundingBox>,
    limit: u32,
    offset: u32,
) -> (String, Vec<DatabaseValue>) {
    let mut conditions: Vec<String> = Vec::new();
    let mut feature_params: Vec<DatabaseValue> = Vec::new();
    let mut feat_idx: usize = 1;

    if let Some(b) = bbox {
        conditions.push(format!(
            "longitude >= ${feat_idx} AND longitude <= ${} AND latitude >= ${} AND latitude <= ${}",
            feat_idx + 1,
            feat_idx + 2,
            feat_idx + 3
        ));
        feature_params.push(DatabaseValue::Real64(b.west));
        feature_params.push(DatabaseValue::Real64(b.east));
        feature_params.push(DatabaseValue::Real64(b.south));
        feature_params.push(DatabaseValue::Real64(b.north));
        feat_idx += 4;
    }

    if let Some(ref from) = params.from {
        conditions.push(format!("occurred_at >= ${feat_idx}"));
        feature_params.push(DatabaseValue::String(from.clone()));
        feat_idx += 1;
    }
    if let Some(ref to) = params.to {
        conditions.push(format!("occurred_at <= ${feat_idx}"));
        feature_params.push(DatabaseValue::String(to.clone()));
        feat_idx += 1;
    }

    add_features_in_filter(
        params.categories.as_deref(),
        "category",
        &mut conditions,
        &mut feature_params,
        &mut feat_idx,
    );

    add_features_in_filter(
        params.subcategories.as_deref(),
        "subcategory",
        &mut conditions,
        &mut feature_params,
        &mut feat_idx,
    );

    if let Some(sev) = params.severity_min
        && sev > 1
    {
        conditions.push(format!("severity >= ${feat_idx}"));
        feature_params.push(DatabaseValue::Int32(i32::from(sev)));
        feat_idx += 1;
    }

    if let Some(arrest) = params.arrest_made {
        conditions.push(format!("arrest_made = ${feat_idx}"));
        feature_params.push(DatabaseValue::Int32(i32::from(arrest)));
        feat_idx += 1;
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    let query = format!(
        "SELECT id, source_incident_id, subcategory, category, severity,
                longitude, latitude, occurred_at, description, block_address,
                city, state, arrest_made, location_type
         FROM incidents{where_clause}
         ORDER BY occurred_at DESC
         LIMIT ${feat_idx} OFFSET ${}",
        feat_idx + 1
    );
    feature_params.push(DatabaseValue::UInt32(limit));
    feature_params.push(DatabaseValue::UInt32(offset));

    (query, feature_params)
}

/// Adds an `IN (...)` filter clause for a comma-separated parameter value
/// to the features query builder.
fn add_features_in_filter(
    param_value: Option<&str>,
    column: &str,
    conditions: &mut Vec<String>,
    feature_params: &mut Vec<DatabaseValue>,
    feat_idx: &mut usize,
) {
    let Some(raw) = param_value else { return };
    let items: Vec<&str> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if items.is_empty() {
        return;
    }

    let placeholders: Vec<String> = items
        .iter()
        .enumerate()
        .map(|(i, _)| format!("${}", *feat_idx + i))
        .collect();
    conditions.push(format!("{column} IN ({})", placeholders.join(", ")));
    for item in &items {
        feature_params.push(DatabaseValue::String((*item).to_string()));
    }
    *feat_idx += items.len();
}

/// Executes the count query against the `DuckDB` `count_summary` table.
///
/// Translates bounding box into cell coordinates and applies all sidebar
/// filters (subcategory, category, severity, arrest, date range) against
/// the pre-aggregated dimensions.
fn execute_duckdb_count(
    db_conn: &duckdb::Connection,
    params: &SidebarQueryParams,
    bbox: Option<&BoundingBox>,
) -> Result<u64, String> {
    let mut conditions: Vec<String> = Vec::new();
    let mut bind_values: Vec<DuckValue> = Vec::new();

    build_count_conditions(params, bbox, &mut conditions, &mut bind_values);

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    let sql = format!("SELECT SUM(cnt) AS total FROM count_summary{where_clause}");

    let mut stmt = db_conn
        .prepare(&sql)
        .map_err(|e| format!("DuckDB prepare failed: {e}"))?;

    // Build a Vec of boxed ToSql values, then collect references for query
    let boxed_params: Vec<Box<dyn duckdb::ToSql>> = bind_values
        .into_iter()
        .map(|v| -> Box<dyn duckdb::ToSql> {
            match v {
                DuckValue::Int(i) => Box::new(i),
                DuckValue::Str(s) => Box::new(s),
            }
        })
        .collect();
    let param_refs: Vec<&dyn duckdb::ToSql> = boxed_params.iter().map(AsRef::as_ref).collect();

    let total: u64 = stmt
        .query_row(param_refs.as_slice(), |row| {
            let val: Option<i64> = row.get(0)?;
            Ok(val.unwrap_or(0).try_into().unwrap_or(0))
        })
        .map_err(|e| format!("DuckDB query failed: {e}"))?;

    Ok(total)
}

/// Builds the WHERE conditions and bind values for the `DuckDB` count query.
fn build_count_conditions(
    params: &SidebarQueryParams,
    bbox: Option<&BoundingBox>,
    conditions: &mut Vec<String>,
    bind_values: &mut Vec<DuckValue>,
) {
    if let Some(b) = bbox {
        // Convert bbox to cell coordinates: floor(coord * 100)
        #[allow(clippy::cast_possible_truncation)]
        let cell_west = (b.west * 100.0).floor() as i32;
        #[allow(clippy::cast_possible_truncation)]
        let cell_east = (b.east * 100.0).floor() as i32;
        #[allow(clippy::cast_possible_truncation)]
        let cell_south = (b.south * 100.0).floor() as i32;
        #[allow(clippy::cast_possible_truncation)]
        let cell_north = (b.north * 100.0).floor() as i32;

        conditions.push("cell_lng >= ? AND cell_lng <= ?".to_string());
        bind_values.push(DuckValue::Int(cell_west));
        bind_values.push(DuckValue::Int(cell_east));

        conditions.push("cell_lat >= ? AND cell_lat <= ?".to_string());
        bind_values.push(DuckValue::Int(cell_south));
        bind_values.push(DuckValue::Int(cell_north));
    }

    if let Some(ref from) = params.from {
        conditions.push("day >= ?".to_string());
        bind_values.push(DuckValue::Str(extract_date_part(from)));
    }
    if let Some(ref to) = params.to {
        conditions.push("day <= ?".to_string());
        bind_values.push(DuckValue::Str(extract_date_part(to)));
    }

    add_count_in_filter(
        params.categories.as_deref(),
        "category",
        conditions,
        bind_values,
    );
    add_count_in_filter(
        params.subcategories.as_deref(),
        "subcategory",
        conditions,
        bind_values,
    );

    if let Some(sev) = params.severity_min
        && sev > 1
    {
        conditions.push("severity >= ?".to_string());
        bind_values.push(DuckValue::Int(i32::from(sev)));
    }

    if let Some(arrest) = params.arrest_made {
        conditions.push("arrest = ?".to_string());
        bind_values.push(DuckValue::Int(i32::from(arrest)));
    }
}

/// Adds an `IN (...)` filter for a comma-separated parameter to the `DuckDB`
/// count query conditions and bind values.
fn add_count_in_filter(
    param_value: Option<&str>,
    column: &str,
    conditions: &mut Vec<String>,
    bind_values: &mut Vec<DuckValue>,
) {
    let Some(raw) = param_value else { return };
    let items: Vec<&str> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if items.is_empty() {
        return;
    }

    let placeholders: Vec<&str> = items.iter().map(|_| "?").collect();
    conditions.push(format!("{column} IN ({})", placeholders.join(", ")));
    for item in &items {
        bind_values.push(DuckValue::Str((*item).to_string()));
    }
}

/// Helper enum for `DuckDB` parameter binding.
enum DuckValue {
    Int(i32),
    Str(String),
}

/// Extracts the date portion (`YYYY-MM-DD`) from a date or RFC 3339 string.
///
/// Truncates at the `T` separator if present, otherwise takes the first 10
/// characters.
fn extract_date_part(s: &str) -> String {
    s.find('T').map_or_else(
        || {
            if s.len() >= 10 {
                s[..10].to_string()
            } else {
                s.to_string()
            }
        },
        |idx| s[..idx].to_string(),
    )
}

/// Parses a bounding box string `"west,south,east,north"` into a
/// [`BoundingBox`].
fn parse_bbox(s: &str) -> Option<BoundingBox> {
    let parts: Vec<f64> = s.split(',').filter_map(|p| p.trim().parse().ok()).collect();
    if parts.len() == 4 {
        Some(BoundingBox::new(parts[0], parts[1], parts[2], parts[3]))
    } else {
        None
    }
}

/// Query parameters for the AI ask endpoint.
#[derive(Debug, serde::Deserialize)]
pub struct AiQueryParams {
    /// The user's natural-language question.
    pub q: String,
}

/// `GET /api/ai/ask?q=...`
///
/// Server-Sent Events endpoint that streams AI agent progress and final
/// answer. The agent interprets the user's question, calls analytical
/// tools against the crime database, and produces a markdown answer.
pub async fn ai_ask(state: web::Data<AppState>, params: web::Query<AiQueryParams>) -> HttpResponse {
    let question = params.q.trim().to_string();

    if question.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Query parameter 'q' is required"
        }));
    }

    if question.len() > 2000 {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Question too long (max 2000 characters)"
        }));
    }

    // Check if AI is configured
    let provider = match crime_map_ai::providers::create_provider_from_env().await {
        Ok(p) => p,
        Err(e) => {
            log::error!("AI provider not configured: {e}");
            return HttpResponse::ServiceUnavailable().json(serde_json::json!({
                "error": format!("AI not configured: {e}")
            }));
        }
    };

    let db = state.db.clone();
    let context = state.ai_context.clone();

    // Create channel for agent events
    let (tx, mut rx) = tokio::sync::mpsc::channel::<crime_map_ai::AgentEvent>(32);

    // Spawn the agent loop with a 60-second timeout
    let agent_handle = tokio::spawn(async move {
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            crime_map_ai::agent::run_agent(
                provider.as_ref(),
                db.as_ref(),
                &context,
                &question,
                tx.clone(),
            ),
        )
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                log::error!("Agent error: {e}");
                let _ = tx
                    .send(crime_map_ai::AgentEvent::Error {
                        message: format!("Agent error: {e}"),
                    })
                    .await;
            }
            Err(_) => {
                log::error!("Agent timed out after 60 seconds");
                let _ = tx
                    .send(crime_map_ai::AgentEvent::Error {
                        message: "Request timed out. Please try a simpler question.".to_string(),
                    })
                    .await;
            }
        }
    });

    // Stream events as SSE
    let stream = async_stream::stream! {
        while let Some(event) = rx.recv().await {
            let json = serde_json::to_string(&event).unwrap_or_default();
            let sse = format!("data: {json}\n\n");
            yield Ok::<_, actix_web::Error>(actix_web::web::Bytes::from(sse));
        }

        // Wait for agent to finish
        let _ = agent_handle.await;

        // Send done event
        yield Ok::<_, actix_web::Error>(actix_web::web::Bytes::from("data: {\"type\":\"done\"}\n\n"));
    };

    HttpResponse::Ok()
        .content_type("text/event-stream")
        .insert_header(("Cache-Control", "no-cache"))
        .insert_header(("Connection", "keep-alive"))
        .insert_header(("X-Accel-Buffering", "no"))
        .streaming(stream)
}
