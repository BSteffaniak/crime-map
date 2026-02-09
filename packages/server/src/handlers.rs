//! HTTP handler functions for the crime map API.

use actix_web::{HttpResponse, web};
use crime_map_crime_models::{CrimeCategory, CrimeSubcategory};
use crime_map_database::queries;
use crime_map_database_models::{BoundingBox, IncidentQuery};
use crime_map_server_models::{
    ApiCategoryNode, ApiHealth, ApiIncident, ApiSubcategoryNode, IncidentQueryParams,
};
use moosicbox_json_utils::database::ToValue as _;

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
