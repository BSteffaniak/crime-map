//! Database ingestion for neighborhood boundaries.
//!
//! Fetches, normalizes, and inserts neighborhood polygons into `PostGIS`,
//! then builds the `tract_neighborhoods` crosswalk via spatial join.

use crime_map_neighborhood_models::NeighborhoodSource;
use switchy_database::{Database, DatabaseValue};

use crate::NeighborhoodError;

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
    db: &dyn Database,
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

    // Fetch raw features from the API
    let features = crate::fetchers::fetch_features(client, source).await?;
    log::info!("{}: fetched {} raw features", source.id, features.len());

    // Normalize into boundaries
    let boundaries = crate::normalize::normalize_features(&features, &source.fields);
    log::info!(
        "{}: normalized {} boundaries from {} features",
        source.id,
        boundaries.len(),
        features.len()
    );

    // Upsert into the database
    let mut inserted = 0u64;
    for boundary in &boundaries {
        let result = db
            .exec_raw_params(
                "INSERT INTO neighborhoods (source_id, city, state, name, boundary)
                 VALUES ($1, $2, $3, $4, ST_Multi(ST_GeomFromGeoJSON($5))::geography)
                 ON CONFLICT (source_id, name) DO UPDATE SET
                     boundary = EXCLUDED.boundary",
                &[
                    DatabaseValue::String(source.id.clone()),
                    DatabaseValue::String(source.city.clone()),
                    DatabaseValue::String(source.state.clone()),
                    DatabaseValue::String(boundary.name.clone()),
                    DatabaseValue::String(boundary.geometry_json.clone()),
                ],
            )
            .await?;
        inserted += result;
    }

    log::info!("{}: inserted/updated {inserted} neighborhoods", source.id);

    Ok(inserted)
}

/// Rebuilds the `tract_neighborhoods` crosswalk table.
///
/// For each census tract, finds which neighborhood polygon contains
/// the tract's centroid via `ST_Contains`. Clears existing crosswalk
/// data first.
///
/// # Errors
///
/// Returns [`NeighborhoodError`] if the database operations fail.
pub async fn build_crosswalk(db: &dyn Database) -> Result<u64, NeighborhoodError> {
    log::info!("Building tract-to-neighborhood crosswalk...");

    // Rebuild crosswalk atomically: delete + insert in a single statement via CTE
    let inserted = db
        .exec_raw_params(
            "WITH cleared AS (
                DELETE FROM tract_neighborhoods
            )
            INSERT INTO tract_neighborhoods (geoid, neighborhood_id)
            SELECT ct.geoid, n.id
            FROM census_tracts ct
            JOIN neighborhoods n
              ON ST_Contains(
                  n.boundary::geometry,
                  ST_SetSRID(ST_Point(ct.centroid_lon, ct.centroid_lat), 4326)
              )
            WHERE ct.centroid_lon IS NOT NULL
              AND ct.centroid_lat IS NOT NULL
            ON CONFLICT DO NOTHING",
            &[],
        )
        .await?;

    log::info!("Crosswalk built: {inserted} tract-neighborhood mappings");

    Ok(inserted)
}
