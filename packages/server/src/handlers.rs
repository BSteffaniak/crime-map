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
/// The features query walks the `occurred_at DESC` index and checks the
/// bounding box inline, relying on `LIMIT` to short-circuit early.
/// The count query uses the R-tree spatial index for efficient counting.
pub async fn sidebar(
    state: web::Data<AppState>,
    params: web::Query<SidebarQueryParams>,
) -> HttpResponse {
    let limit = params.limit.unwrap_or(50);
    let offset = params.offset.unwrap_or(0);
    let bbox = params.bbox.as_deref().and_then(parse_bbox);

    let (features_query, feature_params, count_query, count_params) =
        build_sidebar_queries(&params, bbox.as_ref(), limit, offset);

    let sidebar_db = state.sidebar_db.as_ref();

    let features_result = sidebar_db
        .query_raw_params(&features_query, &feature_params)
        .await;
    let count_result = sidebar_db
        .query_raw_params(&count_query, &count_params)
        .await;

    match (features_result, count_result) {
        (Ok(rows), Ok(count_rows)) => {
            let total_count: u64 = count_rows
                .first()
                .and_then(|r| r.to_value::<i64>("cnt").ok())
                .unwrap_or(0)
                .try_into()
                .unwrap_or(0);

            let features: Vec<SidebarIncident> = rows.iter().map(parse_sidebar_row).collect();

            #[allow(clippy::cast_possible_truncation)]
            let has_more = (u64::from(offset) + features.len() as u64) < total_count;

            HttpResponse::Ok().json(SidebarResponse {
                features,
                total_count,
                has_more,
            })
        }
        (Err(e), _) | (_, Err(e)) => {
            log::error!("Sidebar query failed: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to query sidebar data"
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

/// Builds the features and count SQL queries with their parameter vectors
/// from the sidebar query parameters.
///
/// Returns `(features_query, feature_params, count_query, count_params)`.
fn build_sidebar_queries(
    params: &SidebarQueryParams,
    bbox: Option<&BoundingBox>,
    limit: u32,
    offset: u32,
) -> (String, Vec<DatabaseValue>, String, Vec<DatabaseValue>) {
    let mut conditions: Vec<String> = Vec::new();
    let mut count_conditions: Vec<String> = Vec::new();
    let mut feature_params: Vec<DatabaseValue> = Vec::new();
    let mut count_params: Vec<DatabaseValue> = Vec::new();
    let mut feat_idx: usize = 1;
    let mut count_idx: usize = 1;

    if let Some(b) = bbox {
        add_bbox_filter(
            b,
            &mut conditions,
            &mut count_conditions,
            &mut feature_params,
            &mut count_params,
            &mut feat_idx,
            &mut count_idx,
        );
    }

    add_date_range_filters(
        params,
        &mut conditions,
        &mut count_conditions,
        &mut feature_params,
        &mut count_params,
        &mut feat_idx,
        &mut count_idx,
    );

    add_in_filter(
        params.categories.as_deref(),
        "category",
        "i.category",
        &mut conditions,
        &mut count_conditions,
        &mut feature_params,
        &mut count_params,
        &mut feat_idx,
        &mut count_idx,
    );

    add_in_filter(
        params.subcategories.as_deref(),
        "subcategory",
        "i.subcategory",
        &mut conditions,
        &mut count_conditions,
        &mut feature_params,
        &mut count_params,
        &mut feat_idx,
        &mut count_idx,
    );

    add_scalar_filters(
        params,
        &mut conditions,
        &mut count_conditions,
        &mut feature_params,
        &mut count_params,
        &mut feat_idx,
        &mut count_idx,
    );

    let features_query =
        assemble_features_query(&conditions, &mut feature_params, feat_idx, limit, offset);
    let count_query = assemble_count_query(bbox.is_some(), &count_conditions);

    (features_query, feature_params, count_query, count_params)
}

/// Adds bounding-box filter clauses for both the features (plain columns) and
/// count (R-tree) queries.
#[allow(clippy::too_many_arguments)]
fn add_bbox_filter(
    b: &BoundingBox,
    conditions: &mut Vec<String>,
    count_conditions: &mut Vec<String>,
    feature_params: &mut Vec<DatabaseValue>,
    count_params: &mut Vec<DatabaseValue>,
    feat_idx: &mut usize,
    count_idx: &mut usize,
) {
    conditions.push(format!(
        "longitude >= ${feat_idx} AND longitude <= ${} AND latitude >= ${} AND latitude <= ${}",
        *feat_idx + 1,
        *feat_idx + 2,
        *feat_idx + 3
    ));
    feature_params.push(DatabaseValue::Real64(b.west));
    feature_params.push(DatabaseValue::Real64(b.east));
    feature_params.push(DatabaseValue::Real64(b.south));
    feature_params.push(DatabaseValue::Real64(b.north));
    *feat_idx += 4;

    count_conditions.push(format!(
        "r.min_lng >= ${count_idx} AND r.max_lng <= ${} AND r.min_lat >= ${} AND r.max_lat <= ${}",
        *count_idx + 1,
        *count_idx + 2,
        *count_idx + 3
    ));
    count_params.push(DatabaseValue::Real64(b.west));
    count_params.push(DatabaseValue::Real64(b.east));
    count_params.push(DatabaseValue::Real64(b.south));
    count_params.push(DatabaseValue::Real64(b.north));
    *count_idx += 4;
}

/// Adds date-range (`from` / `to`) filter clauses to both query builders.
#[allow(clippy::too_many_arguments)]
fn add_date_range_filters(
    params: &SidebarQueryParams,
    conditions: &mut Vec<String>,
    count_conditions: &mut Vec<String>,
    feature_params: &mut Vec<DatabaseValue>,
    count_params: &mut Vec<DatabaseValue>,
    feat_idx: &mut usize,
    count_idx: &mut usize,
) {
    if let Some(ref from) = params.from {
        conditions.push(format!("occurred_at >= ${feat_idx}"));
        feature_params.push(DatabaseValue::String(from.clone()));
        *feat_idx += 1;

        count_conditions.push(format!("i.occurred_at >= ${count_idx}"));
        count_params.push(DatabaseValue::String(from.clone()));
        *count_idx += 1;
    }
    if let Some(ref to) = params.to {
        conditions.push(format!("occurred_at <= ${feat_idx}"));
        feature_params.push(DatabaseValue::String(to.clone()));
        *feat_idx += 1;

        count_conditions.push(format!("i.occurred_at <= ${count_idx}"));
        count_params.push(DatabaseValue::String(to.clone()));
        *count_idx += 1;
    }
}

/// Adds severity and arrest scalar filter clauses to both query builders.
#[allow(clippy::too_many_arguments)]
fn add_scalar_filters(
    params: &SidebarQueryParams,
    conditions: &mut Vec<String>,
    count_conditions: &mut Vec<String>,
    feature_params: &mut Vec<DatabaseValue>,
    count_params: &mut Vec<DatabaseValue>,
    feat_idx: &mut usize,
    count_idx: &mut usize,
) {
    if let Some(sev) = params.severity_min
        && sev > 1
    {
        conditions.push(format!("severity >= ${feat_idx}"));
        feature_params.push(DatabaseValue::Int32(i32::from(sev)));
        *feat_idx += 1;

        count_conditions.push(format!("i.severity >= ${count_idx}"));
        count_params.push(DatabaseValue::Int32(i32::from(sev)));
        *count_idx += 1;
    }

    if let Some(arrest) = params.arrest_made {
        conditions.push(format!("arrest_made = ${feat_idx}"));
        feature_params.push(DatabaseValue::Int32(i32::from(arrest)));
        *feat_idx += 1;

        count_conditions.push(format!("i.arrest_made = ${count_idx}"));
        count_params.push(DatabaseValue::Int32(i32::from(arrest)));
        #[allow(unused_assignments)]
        {
            *count_idx += 1;
        }
    }
}

/// Assembles the final features SELECT query with WHERE, ORDER BY, LIMIT, and
/// OFFSET clauses.
fn assemble_features_query(
    conditions: &[String],
    feature_params: &mut Vec<DatabaseValue>,
    feat_idx: usize,
    limit: u32,
    offset: u32,
) -> String {
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
    query
}

/// Assembles the count query, optionally joining the R-tree index for bbox
/// filtering.
fn assemble_count_query(has_bbox: bool, count_conditions: &[String]) -> String {
    if has_bbox || !count_conditions.is_empty() {
        let rtree_join = if has_bbox {
            " JOIN incidents_rtree r ON r.id = i.id"
        } else {
            ""
        };
        let conds = if count_conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", count_conditions.join(" AND "))
        };
        format!("SELECT COUNT(*) as cnt FROM incidents i{rtree_join}{conds}")
    } else {
        "SELECT COUNT(*) as cnt FROM incidents".to_string()
    }
}

/// Adds an `IN (...)` filter clause for a comma-separated parameter value
/// to both the features and count query builders.
#[allow(clippy::too_many_arguments)]
fn add_in_filter(
    param_value: Option<&str>,
    feat_column: &str,
    count_column: &str,
    conditions: &mut Vec<String>,
    count_conditions: &mut Vec<String>,
    feature_params: &mut Vec<DatabaseValue>,
    count_params: &mut Vec<DatabaseValue>,
    feat_idx: &mut usize,
    count_idx: &mut usize,
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

    let feat_placeholders: Vec<String> = items
        .iter()
        .enumerate()
        .map(|(i, _)| format!("${}", *feat_idx + i))
        .collect();
    conditions.push(format!(
        "{feat_column} IN ({})",
        feat_placeholders.join(", ")
    ));
    for item in &items {
        feature_params.push(DatabaseValue::String((*item).to_string()));
    }
    *feat_idx += items.len();

    let count_placeholders: Vec<String> = items
        .iter()
        .enumerate()
        .map(|(i, _)| format!("${}", *count_idx + i))
        .collect();
    count_conditions.push(format!(
        "{count_column} IN ({})",
        count_placeholders.join(", ")
    ));
    for item in &items {
        count_params.push(DatabaseValue::String((*item).to_string()));
    }
    *count_idx += items.len();
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
