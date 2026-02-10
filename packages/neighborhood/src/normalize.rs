//! Normalizes raw `GeoJSON` features into [`NormalizedBoundary`] values.
//!
//! Uses the source's [`NeighborhoodFieldMapping`] to extract the
//! neighborhood name and geometry from each feature, regardless of
//! the API-specific field naming.

use crime_map_neighborhood_models::{
    GeometryExtractor, NeighborhoodFieldMapping, NormalizedBoundary,
};

/// Normalizes a list of raw `GeoJSON` features into boundaries.
///
/// Skips features with missing names or empty geometries.
#[must_use]
pub fn normalize_features(
    features: &[serde_json::Value],
    fields: &NeighborhoodFieldMapping,
) -> Vec<NormalizedBoundary> {
    features
        .iter()
        .filter_map(|feature| normalize_feature(feature, fields))
        .collect()
}

/// Normalizes a single `GeoJSON` feature.
fn normalize_feature(
    feature: &serde_json::Value,
    fields: &NeighborhoodFieldMapping,
) -> Option<NormalizedBoundary> {
    let props = feature.get("properties")?;

    // Extract the neighborhood name
    let name = props
        .get(&fields.name)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())?
        .to_string();

    // Extract and convert geometry
    let geometry_json = match &fields.geometry {
        GeometryExtractor::Geojson => {
            let geom = feature.get("geometry")?;
            if geom.is_null() {
                return None;
            }
            serde_json::to_string(geom).ok()?
        }
        GeometryExtractor::EsriRings => {
            let geom = feature.get("geometry")?;
            let geojson = esri_to_geojson(geom)?;
            serde_json::to_string(&geojson).ok()?
        }
    };

    if geometry_json.is_empty() || geometry_json == "null" {
        return None;
    }

    Some(NormalizedBoundary {
        name,
        geometry_json,
    })
}

/// Converts Esri JSON geometry (`{ "rings": [...] }`) to a `GeoJSON`
/// Polygon/`MultiPolygon`.
fn esri_to_geojson(esri_geom: &serde_json::Value) -> Option<serde_json::Value> {
    let rings = esri_geom.get("rings")?.as_array()?;

    if rings.is_empty() {
        return None;
    }

    if rings.len() == 1 {
        Some(serde_json::json!({
            "type": "Polygon",
            "coordinates": rings,
        }))
    } else {
        // Multiple rings — wrap each as a single-ring polygon in a
        // MultiPolygon. (A more sophisticated approach would detect
        // holes, but for neighborhood boundaries single-exterior-ring
        // is the common case.)
        let polygons: Vec<serde_json::Value> =
            rings.iter().map(|ring| serde_json::json!([ring])).collect();

        Some(serde_json::json!({
            "type": "MultiPolygon",
            "coordinates": polygons,
        }))
    }
}
