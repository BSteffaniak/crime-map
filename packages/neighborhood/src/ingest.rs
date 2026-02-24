//! Database ingestion for neighborhood boundaries.
//!
//! Fetches, normalizes, and inserts neighborhood polygons into `DuckDB`,
//! then builds the `tract_neighborhoods` crosswalk using Rust spatial
//! lookups.

use crime_map_neighborhood_models::NeighborhoodSource;
use duckdb::Connection;
use geo::{Contains, MultiPolygon};
use geojson::GeoJson;
use rstar::{AABB, RTree, RTreeObject};

use crate::NeighborhoodError;

/// A neighborhood polygon for the R-tree.
struct NeighborhoodEntry {
    id: i32,
    envelope: AABB<[f64; 2]>,
    polygon: MultiPolygon<f64>,
}

impl RTreeObject for NeighborhoodEntry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

/// Ingests boundaries from a single neighborhood source.
///
/// Fetches the data, normalizes it, and upserts into the `neighborhoods`
/// table. Returns the number of neighborhoods inserted/updated.
///
/// # Errors
///
/// Returns [`NeighborhoodError`] if fetching, parsing, or database
/// operations fail.
pub async fn ingest_source(
    conn: &Connection,
    client: &reqwest::Client,
    source: &NeighborhoodSource,
) -> Result<u64, NeighborhoodError> {
    log::info!(
        "Fetching neighborhoods for {} ({}, {}): {}",
        source.id,
        source.city,
        source.state,
        source.name,
    );

    let features = crate::fetchers::fetch_features(client, source).await?;
    log::info!("{}: fetched {} raw features", source.id, features.len());

    let boundaries = crate::normalize::normalize_features(&features, &source.fields);
    log::info!(
        "{}: normalized {} boundaries from {} features",
        source.id,
        boundaries.len(),
        features.len()
    );

    // Ensure unique index exists for ON CONFLICT
    conn.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_neighborhoods_source_name
         ON neighborhoods (source_id, name);",
    )?;

    let mut inserted = 0u64;
    let mut stmt = conn.prepare(
        "INSERT INTO neighborhoods (id, source_id, city, state, name, boundary_geojson)
         VALUES (nextval('neighborhoods_id_seq'), ?, ?, ?, ?, ?)
         ON CONFLICT (source_id, name) DO UPDATE SET
             boundary_geojson = EXCLUDED.boundary_geojson",
    )?;

    for boundary in &boundaries {
        let rows = stmt.execute(duckdb::params![
            &source.id,
            &source.city,
            &source.state,
            &boundary.name,
            &boundary.geometry_json,
        ])?;
        inserted += u64::try_from(rows).unwrap_or(0);
    }

    log::info!("{}: inserted/updated {inserted} neighborhoods", source.id);

    Ok(inserted)
}

/// Rebuilds the `tract_neighborhoods` crosswalk table.
///
/// For each census tract, finds which neighborhood polygon contains
/// the tract's centroid using Rust spatial lookups.
///
/// # Errors
///
/// Returns [`NeighborhoodError`] if the database operations fail.
pub fn build_crosswalk(conn: &Connection) -> Result<u64, NeighborhoodError> {
    log::info!("Building tract-to-neighborhood crosswalk...");

    // Load neighborhood polygons into R-tree
    let mut entries = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT id, boundary_geojson FROM neighborhoods WHERE boundary_geojson IS NOT NULL",
        )?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let id: i32 = row.get(0)?;
            let geojson_str: String = row.get(1)?;

            if let Some(mp) = parse_geojson_to_multipolygon(&geojson_str) {
                let envelope = compute_envelope(&mp);
                entries.push(NeighborhoodEntry {
                    id,
                    envelope,
                    polygon: mp,
                });
            }
        }
    }

    let rtree = RTree::bulk_load(entries);
    log::info!("Built R-tree with {} neighborhoods", rtree.size());

    // Load tract centroids
    let mut tract_mappings: Vec<(String, i32)> = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT geoid, centroid_lon, centroid_lat FROM census_tracts
             WHERE centroid_lon IS NOT NULL AND centroid_lat IS NOT NULL",
        )?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let geoid: String = row.get(0)?;
            let lon: f64 = row.get(1)?;
            let lat: f64 = row.get(2)?;

            let point = geo::Point::new(lon, lat);
            let query_env = AABB::from_point([lon, lat]);

            for entry in rtree.locate_in_envelope_intersecting(&query_env) {
                if entry.polygon.contains(&point) {
                    tract_mappings.push((geoid.clone(), entry.id));
                }
            }
        }
    }

    // Clear and rebuild crosswalk
    conn.execute_batch("DELETE FROM tract_neighborhoods;")?;

    let mut stmt = conn.prepare(
        "INSERT INTO tract_neighborhoods (geoid, neighborhood_id) VALUES (?, ?)
         ON CONFLICT DO NOTHING",
    )?;

    let mut inserted = 0u64;
    for (geoid, nbhd_id) in &tract_mappings {
        let rows = stmt.execute(duckdb::params![geoid, nbhd_id])?;
        inserted += u64::try_from(rows).unwrap_or(0);
    }

    log::info!("Crosswalk built: {inserted} tract-neighborhood mappings");

    Ok(inserted)
}

/// Parse a `GeoJSON` string into a `MultiPolygon`.
fn parse_geojson_to_multipolygon(geojson_str: &str) -> Option<MultiPolygon<f64>> {
    let geojson: GeoJson = geojson_str.parse().ok()?;
    if let GeoJson::Geometry(geom) = geojson {
        let geo_geom: geo::Geometry<f64> = geom.try_into().ok()?;
        match geo_geom {
            geo::Geometry::MultiPolygon(mp) => Some(mp),
            geo::Geometry::Polygon(p) => Some(MultiPolygon(vec![p])),
            _ => None,
        }
    } else {
        None
    }
}

/// Compute the bounding box envelope for a `MultiPolygon`.
fn compute_envelope(mp: &MultiPolygon<f64>) -> AABB<[f64; 2]> {
    use geo::BoundingRect;

    mp.bounding_rect().map_or_else(
        || AABB::from_point([0.0, 0.0]),
        |rect| AABB::from_corners([rect.min().x, rect.min().y], [rect.max().x, rect.max().y]),
    )
}
