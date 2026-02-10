//! Database queries for census tract data.
//!
//! Provides functions for looking up tracts by point, bounding box,
//! and for computing aggregate crime statistics by tract.

use crime_map_geography_models::CensusTract;
use moosicbox_json_utils::database::ToValue as _;
use switchy_database::{Database, DatabaseValue};

use crate::GeoError;

/// Finds the census tract containing a given point.
///
/// # Errors
///
/// Returns [`GeoError`] if the database operation fails.
pub async fn find_tract_by_point(
    db: &dyn Database,
    lon: f64,
    lat: f64,
) -> Result<Option<CensusTract>, GeoError> {
    let rows = db
        .query_raw_params(
            "SELECT geoid, name, state_fips, county_fips, state_abbr, county_name,
                    land_area_sq_mi, population, centroid_lon, centroid_lat
             FROM census_tracts
             WHERE ST_Covers(boundary, ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography)
             LIMIT 1",
            &[DatabaseValue::Real64(lon), DatabaseValue::Real64(lat)],
        )
        .await?;

    Ok(rows.first().map(|row| CensusTract {
        geoid: row.to_value("geoid").unwrap_or_default(),
        name: row.to_value("name").unwrap_or_default(),
        state_fips: row.to_value("state_fips").unwrap_or_default(),
        county_fips: row.to_value("county_fips").unwrap_or_default(),
        state_abbr: row.to_value("state_abbr").unwrap_or(None),
        county_name: row.to_value("county_name").unwrap_or(None),
        land_area_sq_mi: row.to_value("land_area_sq_mi").unwrap_or(None),
        population: row.to_value("population").unwrap_or(None),
        centroid_lon: row.to_value("centroid_lon").unwrap_or(None),
        centroid_lat: row.to_value("centroid_lat").unwrap_or(None),
    }))
}

/// Returns all census tracts that intersect a bounding box.
///
/// # Errors
///
/// Returns [`GeoError`] if the database operation fails.
pub async fn find_tracts_in_bbox(
    db: &dyn Database,
    west: f64,
    south: f64,
    east: f64,
    north: f64,
) -> Result<Vec<CensusTract>, GeoError> {
    let rows = db
        .query_raw_params(
            "SELECT geoid, name, state_fips, county_fips, state_abbr, county_name,
                    land_area_sq_mi, population, centroid_lon, centroid_lat
             FROM census_tracts
             WHERE boundary && ST_MakeEnvelope($1, $2, $3, $4, 4326)::geography",
            &[
                DatabaseValue::Real64(west),
                DatabaseValue::Real64(south),
                DatabaseValue::Real64(east),
                DatabaseValue::Real64(north),
            ],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|row| CensusTract {
            geoid: row.to_value("geoid").unwrap_or_default(),
            name: row.to_value("name").unwrap_or_default(),
            state_fips: row.to_value("state_fips").unwrap_or_default(),
            county_fips: row.to_value("county_fips").unwrap_or_default(),
            state_abbr: row.to_value("state_abbr").unwrap_or(None),
            county_name: row.to_value("county_name").unwrap_or(None),
            land_area_sq_mi: row.to_value("land_area_sq_mi").unwrap_or(None),
            population: row.to_value("population").unwrap_or(None),
            centroid_lon: row.to_value("centroid_lon").unwrap_or(None),
            centroid_lat: row.to_value("centroid_lat").unwrap_or(None),
        })
        .collect())
}

/// Returns the total number of census tracts in the database.
///
/// # Errors
///
/// Returns [`GeoError`] if the database operation fails.
pub async fn count_tracts(db: &dyn Database) -> Result<u64, GeoError> {
    let rows = db
        .query_raw_params("SELECT COUNT(*) as count FROM census_tracts", &[])
        .await?;

    let count: i64 = rows.first().map_or(0, |r| r.to_value("count").unwrap_or(0));

    #[allow(clippy::cast_sign_loss)]
    Ok(count as u64)
}

/// Returns all distinct cities that have crime data.
///
/// # Errors
///
/// Returns [`GeoError`] if the database operation fails.
pub async fn get_available_cities(db: &dyn Database) -> Result<Vec<(String, String)>, GeoError> {
    let rows = db
        .query_raw_params(
            "SELECT DISTINCT city, state FROM crime_incidents
             WHERE city IS NOT NULL AND city != ''
             ORDER BY state, city",
            &[],
        )
        .await?;

    Ok(rows
        .iter()
        .map(|row| {
            let city: String = row.to_value("city").unwrap_or_default();
            let state: String = row.to_value("state").unwrap_or_default();
            (city, state)
        })
        .collect())
}

/// Returns the date range of available crime data.
///
/// # Errors
///
/// Returns [`GeoError`] if the database operation fails.
pub async fn get_data_date_range(
    db: &dyn Database,
) -> Result<(Option<String>, Option<String>), GeoError> {
    let rows = db
        .query_raw_params(
            "SELECT MIN(occurred_at)::text as min_date, MAX(occurred_at)::text as max_date
             FROM crime_incidents",
            &[],
        )
        .await?;

    let min_date: Option<String> = rows
        .first()
        .and_then(|r| r.to_value("min_date").unwrap_or(None));
    let max_date: Option<String> = rows
        .first()
        .and_then(|r| r.to_value("max_date").unwrap_or(None));

    Ok((min_date, max_date))
}
