//! HTTP handler functions for the crime map API.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use actix_web::{HttpResponse, web};
use crime_map_crime_models::{CrimeCategory, CrimeSubcategory};
use crime_map_database::queries;
use crime_map_database_models::{BoundingBox, IncidentQuery};
use crime_map_server_models::{
    ApiCategoryNode, ApiHealth, ApiIncident, ApiSubcategoryNode, ClusterEntry, ClusterQueryParams,
    CountFilterParams, HexbinEntry, HexbinQueryParams, IncidentQueryParams, SidebarIncident,
    SidebarQueryParams, SidebarResponse,
};
use moosicbox_json_utils::database::ToValue as _;
use serde::Deserialize;
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
        let filter_params = CountFilterParams::from(&count_params_owned);
        execute_duckdb_count(&conn, &filter_params, bbox_owned.as_ref())
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
    params: &CountFilterParams,
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

    let boxed_params: Vec<Box<dyn duckdb::ToSql>> =
        bind_values.into_iter().map(duck_value_to_boxed).collect();
    let param_refs: Vec<&dyn duckdb::ToSql> = boxed_params.iter().map(AsRef::as_ref).collect();

    let total: u64 = stmt
        .query_row(param_refs.as_slice(), |row| {
            let val: Option<i64> = row.get(0)?;
            Ok(val.unwrap_or(0).try_into().unwrap_or(0))
        })
        .map_err(|e| format!("DuckDB query failed: {e}"))?;

    Ok(total)
}

/// Shared hexbin configuration loaded from `config/hexbins.json` at
/// compile time. Defines the zoom-to-H3-resolution mapping for the
/// hexagonal choropleth overlay.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HexbinConfig {
    zoom_resolution_map: BTreeMap<String, u8>,
}

/// Hexbin configuration embedded at compile time from the shared JSON.
const HEXBIN_CONFIG_JSON: &str = include_str!("../../../config/hexbins.json");

/// Parsed hexbin configuration (lazily initialized on first access).
static HEXBIN_CONFIG: LazyLock<HexbinConfig> = LazyLock::new(|| {
    serde_json::from_str(HEXBIN_CONFIG_JSON).expect("invalid config/hexbins.json")
});

/// Returns the H3 resolution for a given zoom level using the shared
/// configuration. Falls back to resolution 5 if the zoom level is not
/// mapped.
fn resolution_for_zoom(zoom: u8) -> u8 {
    HEXBIN_CONFIG
        .zoom_resolution_map
        .get(&zoom.to_string())
        .copied()
        .unwrap_or(5)
}

/// `GET /api/hexbins`
///
/// Returns H3 hexagonal bin data with polygon boundaries from the
/// `DuckDB` `h3_counts` table. Supports all the same filters as the
/// sidebar endpoint. The response is `MessagePack`-encoded for compact
/// payloads.
pub async fn hexbins(
    state: web::Data<AppState>,
    params: web::Query<HexbinQueryParams>,
) -> HttpResponse {
    let bbox = params.bbox.as_deref().and_then(parse_bbox);
    let zoom = params.zoom.unwrap_or(9);
    let resolution = resolution_for_zoom(zoom);

    let h3_db = state.h3_db.clone();
    let params_owned = params.into_inner();
    let bbox_owned = bbox;

    let result = web::block(move || {
        let conn = h3_db
            .lock()
            .map_err(|e| format!("Failed to lock H3 DuckDB connection: {e}"))?;
        let filter_params = CountFilterParams::from(&params_owned);
        execute_h3_hexbins(&conn, &filter_params, bbox_owned.as_ref(), resolution)
    })
    .await;

    match result {
        Ok(Ok(entries)) => match rmp_serde::to_vec(&entries) {
            Ok(msgpack_bytes) => HttpResponse::Ok()
                .content_type("application/msgpack")
                .body(msgpack_bytes),
            Err(e) => {
                log::error!("MessagePack serialization failed: {e}");
                HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": "Failed to serialize hexbin data"
                }))
            }
        },
        Ok(Err(e)) => {
            log::error!("Hexbins query failed: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to query hexbin data"
            }))
        }
        Err(e) => {
            log::error!("Hexbins query blocking error: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to query hexbin data"
            }))
        }
    }
}

/// Shared cluster configuration loaded from `config/clusters.json` at
/// compile time. Defines the base density and per-zoom multipliers used
/// to compute the target number of k-means output clusters.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClusterConfig {
    density: usize,
    zoom_multipliers: BTreeMap<String, f64>,
}

/// Cluster configuration embedded at compile time from the shared JSON.
const CLUSTER_CONFIG_JSON: &str = include_str!("../../../config/clusters.json");

/// Parsed cluster configuration (lazily initialized on first access).
static CLUSTER_CONFIG: LazyLock<ClusterConfig> = LazyLock::new(|| {
    serde_json::from_str(CLUSTER_CONFIG_JSON).expect("invalid config/clusters.json")
});

/// Computes the target cluster count for a given zoom level using the
/// shared configuration's density and zoom multipliers.
fn compute_target_k(zoom: u8) -> usize {
    let config = &*CLUSTER_CONFIG;
    let multiplier = config
        .zoom_multipliers
        .get(&zoom.to_string())
        .copied()
        .unwrap_or(1.5);

    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let k = (config.density as f64 * multiplier).round() as usize;
    k.max(1)
}

/// `GET /api/clusters`
///
/// Returns density-based cluster data with weighted centroids from the
/// `DuckDB` `count_summary` table. Uses weighted k-means clustering to
/// produce natural, non-grid-aligned cluster positions. Supports all the
/// same filters as the sidebar endpoint.
///
/// The `k` query parameter overrides the default target cluster count
/// computed from `config/clusters.json`.
pub async fn clusters(
    state: web::Data<AppState>,
    params: web::Query<ClusterQueryParams>,
) -> HttpResponse {
    let bbox = params.bbox.as_deref().and_then(parse_bbox);
    let zoom = params.zoom.unwrap_or(9);
    let target_k = params.k.unwrap_or_else(|| compute_target_k(zoom));

    let count_db = state.count_db.clone();
    let params_owned = params.into_inner();
    let bbox_owned = bbox;

    let result = web::block(move || {
        let conn = count_db
            .lock()
            .map_err(|e| format!("Failed to lock DuckDB connection: {e}"))?;
        let filter_params = CountFilterParams::from(&params_owned);
        execute_duckdb_clusters(&conn, &filter_params, bbox_owned.as_ref(), target_k)
    })
    .await;

    match result {
        Ok(Ok(entries)) => HttpResponse::Ok().json(entries),
        Ok(Err(e)) => {
            log::error!("Clusters query failed: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to query cluster data"
            }))
        }
        Err(e) => {
            log::error!("Clusters query blocking error: {e}");
            HttpResponse::InternalServerError().json(serde_json::json!({
                "error": "Failed to query cluster data"
            }))
        }
    }
}

/// A micro-cell returned from the `DuckDB` query: one row per distinct
/// `(cell_lng, cell_lat)` pair with the filter dimensions collapsed.
struct MicroCell {
    /// Weighted centroid longitude.
    centroid_lng: f64,
    /// Weighted centroid latitude.
    centroid_lat: f64,
    /// Incident count in this micro-cell.
    count: u64,
}

/// Runs weighted k-means clustering on micro-cells to produce `k` output
/// clusters with natural, density-driven positions.
///
/// **Algorithm:**
/// 1. Place `k` initial seed centroids evenly across the bounding box.
/// 2. Repeat for up to `max_iterations`:
///    a. Assign each micro-cell to the nearest centroid (by Euclidean
///    distance in degree space).
///    b. Recompute each centroid as the weighted average of its assigned
///    cells: `centroid = SUM(cell_centroid * count) / SUM(count)`.
///    c. If no assignments changed, stop early.
/// 3. Emit one [`ClusterEntry`] per non-empty centroid.
///
/// If the number of micro-cells is at most `k`, each cell becomes its own
/// cluster directly (no iteration needed).
#[allow(clippy::too_many_lines)]
fn weighted_kmeans(cells: &[MicroCell], k: usize, bbox: &BoundingBox) -> Vec<ClusterEntry> {
    if cells.is_empty() {
        return Vec::new();
    }

    // If fewer cells than target clusters, return each cell directly
    if cells.len() <= k {
        return cells
            .iter()
            .map(|c| ClusterEntry {
                lng: c.centroid_lng,
                lat: c.centroid_lat,
                count: c.count,
            })
            .collect();
    }

    // Initialize k seed centroids spread across the bbox in a grid pattern
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let cols = (k as f64).sqrt().ceil() as usize;
    let rows = k.div_ceil(cols);
    #[allow(clippy::cast_precision_loss)]
    let lng_step = (bbox.east - bbox.west) / (cols as f64 + 1.0);
    #[allow(clippy::cast_precision_loss)]
    let lat_step = (bbox.north - bbox.south) / (rows as f64 + 1.0);

    let mut centroids: Vec<(f64, f64)> = Vec::with_capacity(k);
    for r in 0..rows {
        for c in 0..cols {
            if centroids.len() >= k {
                break;
            }
            #[allow(clippy::cast_precision_loss)]
            let lng = lng_step.mul_add(c as f64 + 1.0, bbox.west);
            #[allow(clippy::cast_precision_loss)]
            let lat = lat_step.mul_add(r as f64 + 1.0, bbox.south);
            centroids.push((lng, lat));
        }
    }

    let mut assignments: Vec<usize> = vec![0; cells.len()];
    let max_iterations = 10;

    for _ in 0..max_iterations {
        let mut changed = false;

        // Assign each micro-cell to the nearest centroid
        for (i, cell) in cells.iter().enumerate() {
            let mut best_idx = 0;
            let mut best_dist = f64::MAX;

            for (j, &(clng, clat)) in centroids.iter().enumerate() {
                let dlng = cell.centroid_lng - clng;
                let dlat = cell.centroid_lat - clat;
                let dist = dlng.mul_add(dlng, dlat * dlat);
                if dist < best_dist {
                    best_dist = dist;
                    best_idx = j;
                }
            }

            if assignments[i] != best_idx {
                assignments[i] = best_idx;
                changed = true;
            }
        }

        if !changed {
            break;
        }

        // Recompute centroids as weighted averages
        let mut sum_lng = vec![0.0_f64; k];
        let mut sum_lat = vec![0.0_f64; k];
        let mut sum_cnt = vec![0_u64; k];

        for (i, cell) in cells.iter().enumerate() {
            let idx = assignments[i];
            #[allow(clippy::cast_precision_loss)]
            let w = cell.count as f64;
            sum_lng[idx] = w.mul_add(cell.centroid_lng, sum_lng[idx]);
            sum_lat[idx] = w.mul_add(cell.centroid_lat, sum_lat[idx]);
            sum_cnt[idx] += cell.count;
        }

        for j in 0..k {
            if sum_cnt[j] > 0 {
                #[allow(clippy::cast_precision_loss)]
                let total = sum_cnt[j] as f64;
                centroids[j] = (sum_lng[j] / total, sum_lat[j] / total);
            }
        }
    }

    // Collect final clusters (skip empty centroids)
    let mut result_lng = vec![0.0_f64; k];
    let mut result_lat = vec![0.0_f64; k];
    let mut result_cnt = vec![0_u64; k];

    for (i, cell) in cells.iter().enumerate() {
        let idx = assignments[i];
        #[allow(clippy::cast_precision_loss)]
        let w = cell.count as f64;
        result_lng[idx] = w.mul_add(cell.centroid_lng, result_lng[idx]);
        result_lat[idx] = w.mul_add(cell.centroid_lat, result_lat[idx]);
        result_cnt[idx] += cell.count;
    }

    (0..k)
        .filter(|&j| result_cnt[j] > 0)
        .map(|j| {
            #[allow(clippy::cast_precision_loss)]
            let total = result_cnt[j] as f64;
            ClusterEntry {
                lng: result_lng[j] / total,
                lat: result_lat[j] / total,
                count: result_cnt[j],
            }
        })
        .collect()
}

/// Executes the cluster query against the `DuckDB` `count_summary` table.
///
/// Fetches micro-cells (one per distinct spatial cell within the viewport),
/// then runs weighted k-means to produce naturally positioned clusters.
fn execute_duckdb_clusters(
    db_conn: &duckdb::Connection,
    params: &CountFilterParams,
    bbox: Option<&BoundingBox>,
    target_k: usize,
) -> Result<Vec<ClusterEntry>, String> {
    let mut conditions: Vec<String> = Vec::new();
    let mut bind_values: Vec<DuckValue> = Vec::new();

    build_count_conditions(params, bbox, &mut conditions, &mut bind_values);

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    let sql = format!(
        "SELECT
             cell_lng,
             cell_lat,
             SUM(cnt) AS count,
             SUM(sum_lng) AS total_lng,
             SUM(sum_lat) AS total_lat
         FROM count_summary{where_clause}
         GROUP BY cell_lng, cell_lat
         HAVING SUM(cnt) > 0"
    );

    let mut stmt = db_conn
        .prepare(&sql)
        .map_err(|e| format!("DuckDB prepare failed: {e}"))?;

    let boxed_params: Vec<Box<dyn duckdb::ToSql>> =
        bind_values.into_iter().map(duck_value_to_boxed).collect();
    let param_refs: Vec<&dyn duckdb::ToSql> = boxed_params.iter().map(AsRef::as_ref).collect();

    let mut rows = stmt
        .query(param_refs.as_slice())
        .map_err(|e| format!("DuckDB query failed: {e}"))?;

    let mut micro_cells = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("DuckDB row error: {e}"))? {
        let count: i64 = row.get(2).map_err(|e| format!("DuckDB get count: {e}"))?;
        let total_lng: f64 = row
            .get(3)
            .map_err(|e| format!("DuckDB get total_lng: {e}"))?;
        let total_lat: f64 = row
            .get(4)
            .map_err(|e| format!("DuckDB get total_lat: {e}"))?;

        #[allow(clippy::cast_precision_loss)]
        let count_f = count as f64;

        micro_cells.push(MicroCell {
            centroid_lng: total_lng / count_f,
            centroid_lat: total_lat / count_f,
            count: count.try_into().unwrap_or(0),
        });
    }

    // Use the viewport bbox for k-means seeding; fall back to data extent
    let effective_bbox = bbox.copied().unwrap_or_else(|| {
        let mut west = f64::MAX;
        let mut east = f64::MIN;
        let mut south = f64::MAX;
        let mut north = f64::MIN;
        for c in &micro_cells {
            west = west.min(c.centroid_lng);
            east = east.max(c.centroid_lng);
            south = south.min(c.centroid_lat);
            north = north.max(c.centroid_lat);
        }
        BoundingBox::new(west, south, east, north)
    });

    Ok(weighted_kmeans(&micro_cells, target_k, &effective_bbox))
}

/// Executes the H3 hexbin query against the `DuckDB` `h3_counts` table.
///
/// Fetches aggregated counts per H3 cell within the viewport, then
/// computes hex boundary polygons using `h3o`. Returns a compact array
/// of [`HexbinEntry`] structs ready for `MessagePack` serialization.
fn execute_h3_hexbins(
    db_conn: &duckdb::Connection,
    params: &CountFilterParams,
    bbox: Option<&BoundingBox>,
    resolution: u8,
) -> Result<Vec<HexbinEntry>, String> {
    let mut conditions: Vec<String> = Vec::new();
    let mut bind_values: Vec<DuckValue> = Vec::new();

    // Filter by resolution
    conditions.push("resolution = ?".to_string());
    bind_values.push(DuckValue::Int(i32::from(resolution)));

    // Apply standard filters (date, category, severity, arrest)
    build_h3_conditions(params, &mut conditions, &mut bind_values);

    // Apply bbox filter using h3o's Tiler for exact H3 cell coverage
    if let Some(b) = bbox {
        let Ok(res) = h3o::Resolution::try_from(resolution) else {
            return Err(format!("Invalid H3 resolution: {resolution}"));
        };

        let rect = geo::Rect::new(
            geo::Coord {
                x: b.west,
                y: b.south,
            },
            geo::Coord {
                x: b.east,
                y: b.north,
            },
        );
        let polygon = geo::Polygon::from(rect);

        let mut tiler = h3o::geom::TilerBuilder::new(res)
            .containment_mode(h3o::geom::ContainmentMode::IntersectsBoundary)
            .build();
        tiler
            .add(polygon)
            .map_err(|e| format!("H3 tiler error: {e}"))?;
        let cells: Vec<h3o::CellIndex> = tiler.into_coverage().collect();

        if cells.is_empty() {
            return Ok(Vec::new());
        }

        // Build IN clause for the H3 cell indices
        let placeholders: Vec<&str> = cells.iter().map(|_| "?").collect();
        conditions.push(format!("h3_index IN ({})", placeholders.join(", ")));
        for cell in &cells {
            #[allow(clippy::cast_possible_wrap)]
            bind_values.push(DuckValue::BigInt(u64::from(*cell) as i64));
        }
    }

    let where_clause = format!(" WHERE {}", conditions.join(" AND "));

    let sql = format!(
        "SELECT h3_index, CAST(SUM(cnt) AS BIGINT) AS count
         FROM h3_counts{where_clause}
         GROUP BY h3_index
         HAVING SUM(cnt) > 0"
    );

    let mut stmt = db_conn
        .prepare(&sql)
        .map_err(|e| format!("H3 DuckDB prepare failed: {e}"))?;

    let boxed_params: Vec<Box<dyn duckdb::ToSql>> =
        bind_values.into_iter().map(duck_value_to_boxed).collect();
    let param_refs: Vec<&dyn duckdb::ToSql> = boxed_params.iter().map(AsRef::as_ref).collect();

    let mut rows = stmt
        .query(param_refs.as_slice())
        .map_err(|e| format!("H3 DuckDB query failed: {e}"))?;

    let mut entries = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|e| format!("H3 DuckDB row error: {e}"))?
    {
        let h3_raw: i64 = row.get(0).map_err(|e| format!("H3 get h3_index: {e}"))?;
        let count: i64 = row.get(1).map_err(|e| format!("H3 get count: {e}"))?;

        // Convert raw i64 back to CellIndex and compute boundary
        #[allow(clippy::cast_sign_loss)]
        let h3_u64 = h3_raw as u64;
        let Some(cell) = h3o::CellIndex::try_from(h3_u64).ok() else {
            continue;
        };

        let boundary = cell.boundary();
        let vertices: Vec<[f64; 2]> = boundary
            .iter()
            .map(|coord| [coord.lng(), coord.lat()])
            .collect();

        entries.push(HexbinEntry {
            vertices,
            count: count.try_into().unwrap_or(0),
        });
    }

    Ok(entries)
}

/// Builds the WHERE conditions and bind values for `DuckDB` queries
/// against the `count_summary` table.
fn build_count_conditions(
    params: &CountFilterParams,
    bbox: Option<&BoundingBox>,
    conditions: &mut Vec<String>,
    bind_values: &mut Vec<DuckValue>,
) {
    if let Some(b) = bbox {
        // Convert bbox to cell coordinates: floor(coord * 1000)
        #[allow(clippy::cast_possible_truncation)]
        let cell_west = (b.west * 1000.0).floor() as i32;
        #[allow(clippy::cast_possible_truncation)]
        let cell_east = (b.east * 1000.0).floor() as i32;
        #[allow(clippy::cast_possible_truncation)]
        let cell_south = (b.south * 1000.0).floor() as i32;
        #[allow(clippy::cast_possible_truncation)]
        let cell_north = (b.north * 1000.0).floor() as i32;

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
    BigInt(i64),
    Str(String),
}

/// Converts a [`DuckValue`] to a boxed `dyn ToSql` for `DuckDB` parameter
/// binding.
fn duck_value_to_boxed(v: DuckValue) -> Box<dyn duckdb::ToSql> {
    match v {
        DuckValue::Int(i) => Box::new(i),
        DuckValue::BigInt(i) => Box::new(i),
        DuckValue::Str(s) => Box::new(s),
    }
}

/// Builds the WHERE conditions and bind values for `DuckDB` queries
/// against the `h3_counts` table. Applies the same filter dimensions as
/// `build_count_conditions`: date range, categories, subcategories,
/// severity, and arrest status.
fn build_h3_conditions(
    params: &CountFilterParams,
    conditions: &mut Vec<String>,
    bind_values: &mut Vec<DuckValue>,
) {
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

/// JSON body for the AI ask endpoint.
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiAskRequest {
    /// The user's natural-language question.
    pub question: String,
    /// Conversation ID to continue a prior session. Omit or `null` for new conversations.
    pub conversation_id: Option<String>,
}

/// `POST /api/ai/ask`
///
/// Server-Sent Events endpoint that streams AI agent progress and final
/// answer. The agent interprets the user's question, calls analytical
/// tools against the crime database, and produces a markdown answer.
///
/// Supports multi-turn conversations: pass the `conversationId` from a
/// prior response to continue the same conversation with full context.
/// Conversation history is persisted to `SQLite` between requests.
#[allow(clippy::too_many_lines)]
pub async fn ai_ask(state: web::Data<AppState>, body: web::Json<AiAskRequest>) -> HttpResponse {
    let question = body.question.trim().to_string();

    if question.is_empty() {
        return HttpResponse::BadRequest().json(serde_json::json!({
            "error": "Field 'question' is required"
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

    // Resolve or create conversation ID
    let conversation_id = body
        .conversation_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // Load prior messages from persistent storage
    let prior_messages = match crime_map_conversations::load_messages(
        state.conversations_db.as_ref(),
        &conversation_id,
    )
    .await
    {
        Ok(msgs) => msgs,
        Err(e) => {
            log::error!("Failed to load conversation {conversation_id}: {e}");
            None
        }
    };

    // ── Eager-save: persist the user's new question immediately ────────
    // This ensures the question is never lost, even if the agent crashes
    // or the safety-net timeout fires and cancels the future.
    {
        let mut eager_messages = prior_messages.clone().unwrap_or_default();
        eager_messages.push(crime_map_ai::providers::Message {
            role: "user".to_string(),
            content: crime_map_ai::providers::MessageContent::Text(question.clone()),
        });
        if let Err(e) = crime_map_conversations::save_conversation(
            state.conversations_db.as_ref(),
            &conversation_id,
            &eager_messages,
        )
        .await
        {
            log::error!("Failed to eager-save user message for {conversation_id}: {e}");
        }
    }

    let db = state.db.clone();
    let context = state.ai_context.clone();
    let conversations_db = state.conversations_db.clone();
    let conv_id = conversation_id.clone();

    // Create channel for agent events
    let (tx, mut rx) = tokio::sync::mpsc::channel::<crime_map_ai::AgentEvent>(32);

    // The agent manages its own resource limits (per-tool timeouts,
    // tool call budget, duration budget) via AgentLimits. A generous
    // outer safety-net timeout remains as a last resort.
    let limits = crime_map_ai::agent::AgentLimits::default();
    let safety_timeout = std::time::Duration::from_secs(limits.duration_hard_limit.as_secs() + 60);

    // Spawn the agent loop
    let agent_handle = tokio::spawn(async move {
        let result = tokio::time::timeout(
            safety_timeout,
            crime_map_ai::agent::run_agent(
                provider.as_ref(),
                db.as_ref(),
                &context,
                &question,
                prior_messages,
                &limits,
                tx.clone(),
            ),
        )
        .await;

        // Helper: persist conversation state to SQLite
        let save = |msgs: &[crime_map_ai::providers::Message]| {
            let db = conversations_db.clone();
            let id = conv_id.clone();
            let msgs = msgs.to_vec();
            async move {
                if let Err(e) =
                    crime_map_conversations::save_conversation(db.as_ref(), &id, &msgs).await
                {
                    log::error!("Failed to save conversation {id}: {e}");
                }
            }
        };

        if let Ok(outcome) = result {
            // Always save the messages — whether the agent succeeded or failed
            save(&outcome.messages).await;

            if let Err(e) = outcome.result {
                log::error!("Agent error: {e}");
                let _ = tx
                    .send(crime_map_ai::AgentEvent::Error {
                        message: format!("Agent error: {e}"),
                    })
                    .await;
            }
        } else {
            // Safety-net timeout — the agent future was cancelled so we
            // don't have its messages. The eager-save above ensures the
            // user's question is at least persisted.
            log::error!("Agent hit safety-net timeout after {safety_timeout:?}");
            let _ = tx
                .send(crime_map_ai::AgentEvent::Error {
                    message: "Request exceeded the safety timeout. Please try again.".to_string(),
                })
                .await;
        }
    });

    // Stream events as SSE
    let stream = async_stream::stream! {
        // First event: send the conversation ID so the frontend can store it
        let conv_event = crime_map_ai::AgentEvent::ConversationId {
            id: conversation_id,
        };
        let json = serde_json::to_string(&conv_event).unwrap_or_default();
        yield Ok::<_, actix_web::Error>(actix_web::web::Bytes::from(format!("data: {json}\n\n")));

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
