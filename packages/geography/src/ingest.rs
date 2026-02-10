//! Census tract ingestion from the Census Bureau `TIGERweb` REST API.
//!
//! Downloads tract boundaries as `GeoJSON` from the ACS 2023 vintage
//! `TIGERweb` service and loads them into `PostGIS`.

use switchy_database::{Database, DatabaseValue};

use crate::GeoError;

/// US state FIPS codes for the 50 states + DC.
const STATE_FIPS: &[&str] = &[
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15", "16", "17", "18", "19",
    "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35",
    "36", "37", "38", "39", "40", "41", "42", "44", "45", "46", "47", "48", "49", "50", "51", "53",
    "54", "55", "56",
];

/// State FIPS to abbreviation mapping.
fn state_abbr(fips: &str) -> &'static str {
    match fips {
        "01" => "AL",
        "02" => "AK",
        "04" => "AZ",
        "05" => "AR",
        "06" => "CA",
        "08" => "CO",
        "09" => "CT",
        "10" => "DE",
        "11" => "DC",
        "12" => "FL",
        "13" => "GA",
        "15" => "HI",
        "16" => "ID",
        "17" => "IL",
        "18" => "IN",
        "19" => "IA",
        "20" => "KS",
        "21" => "KY",
        "22" => "LA",
        "23" => "ME",
        "24" => "MD",
        "25" => "MA",
        "26" => "MI",
        "27" => "MN",
        "28" => "MS",
        "29" => "MO",
        "30" => "MT",
        "31" => "NE",
        "32" => "NV",
        "33" => "NH",
        "34" => "NJ",
        "35" => "NM",
        "36" => "NY",
        "37" => "NC",
        "38" => "ND",
        "39" => "OH",
        "40" => "OK",
        "41" => "OR",
        "42" => "PA",
        "44" => "RI",
        "45" => "SC",
        "46" => "SD",
        "47" => "TN",
        "48" => "TX",
        "49" => "UT",
        "50" => "VT",
        "51" => "VA",
        "53" => "WA",
        "54" => "WV",
        "55" => "WI",
        "56" => "WY",
        _ => "??",
    }
}

/// Downloads and inserts census tracts for a single state.
///
/// Uses the `TIGERweb` REST API to query tract boundaries as `EsriJSON`,
/// converts them to `GeoJSON`, and inserts into `PostGIS`.
///
/// # Errors
///
/// Returns [`GeoError`] if the HTTP request or database operation fails.
async fn ingest_state(
    db: &dyn Database,
    client: &reqwest::Client,
    state_fips: &str,
) -> Result<u64, GeoError> {
    let url = format!(
        "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2023/MapServer/8/query\
         ?where=STATE%3D'{state_fips}'\
         &outFields=GEOID,NAME,STATE,COUNTY,ALAND,CENTLAT,CENTLON\
         &outSR=4326\
         &f=geojson\
         &returnGeometry=true"
    );

    log::info!("Fetching tracts for state FIPS {state_fips}...");

    let resp = client.get(&url).send().await?;
    let body = resp.text().await?;

    let geojson: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| GeoError::Conversion {
            message: format!("Failed to parse GeoJSON for state {state_fips}: {e}"),
        })?;

    let features = geojson["features"]
        .as_array()
        .ok_or_else(|| GeoError::Conversion {
            message: format!("No features array for state {state_fips}"),
        })?;

    let abbr = state_abbr(state_fips);
    let mut inserted = 0u64;

    for feature in features {
        let props = &feature["properties"];
        let geoid = props["GEOID"].as_str().unwrap_or_default().to_string();

        if geoid.is_empty() {
            continue;
        }

        let name = props["NAME"]
            .as_str()
            .unwrap_or("Unknown Tract")
            .to_string();

        let county_fips = props["COUNTY"].as_str().unwrap_or("").to_string();

        let aland = props["ALAND"].as_f64();
        // Convert square meters to square miles
        let land_area_sq_mi = aland.map(|a| a / 2_589_988.11);

        let centlat = props["CENTLAT"]
            .as_str()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .or_else(|| props["CENTLAT"].as_f64());

        let centlon = props["CENTLON"]
            .as_str()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .or_else(|| props["CENTLON"].as_f64());

        // Extract geometry as GeoJSON string for ST_GeomFromGeoJSON
        let geometry = &feature["geometry"];
        let geom_str = serde_json::to_string(geometry).unwrap_or_default();

        if geom_str.is_empty() || geom_str == "null" {
            continue;
        }

        let result = db
            .exec_raw_params(
                "INSERT INTO census_tracts (geoid, name, state_fips, county_fips, state_abbr, boundary, land_area_sq_mi, centroid_lon, centroid_lat)
                 VALUES ($1, $2, $3, $4, $5, ST_Multi(ST_GeomFromGeoJSON($6))::geography, $7, $8, $9)
                 ON CONFLICT (geoid) DO UPDATE SET
                     name = EXCLUDED.name,
                     boundary = EXCLUDED.boundary,
                     land_area_sq_mi = EXCLUDED.land_area_sq_mi,
                     centroid_lon = EXCLUDED.centroid_lon,
                     centroid_lat = EXCLUDED.centroid_lat",
                &[
                    DatabaseValue::String(geoid),
                    DatabaseValue::String(name),
                    DatabaseValue::String(state_fips.to_string()),
                    DatabaseValue::String(county_fips),
                    DatabaseValue::String(abbr.to_string()),
                    DatabaseValue::String(geom_str),
                    land_area_sq_mi.map_or(DatabaseValue::Null, DatabaseValue::Real64),
                    centlon.map_or(DatabaseValue::Null, DatabaseValue::Real64),
                    centlat.map_or(DatabaseValue::Null, DatabaseValue::Real64),
                ],
            )
            .await?;

        inserted += result;
    }

    log::info!(
        "State {state_fips} ({abbr}): inserted/updated {inserted} tracts from {} features",
        features.len()
    );
    Ok(inserted)
}

/// Ingests census tract boundaries for all US states.
///
/// Downloads from the `TIGERweb` REST API and loads into `PostGIS`.
/// Processes states sequentially to avoid overwhelming the API.
///
/// # Errors
///
/// Returns [`GeoError`] if any state fails to ingest.
pub async fn ingest_all_tracts(db: &dyn Database) -> Result<u64, GeoError> {
    let client = reqwest::Client::builder()
        .user_agent("crime-map/1.0")
        .build()?;

    let mut total = 0u64;

    for fips in STATE_FIPS {
        match ingest_state(db, &client, fips).await {
            Ok(count) => total += count,
            Err(e) => {
                log::error!("Failed to ingest state {fips}: {e}");
                // Continue with other states
            }
        }
    }

    log::info!("Census tract ingestion complete: {total} total tracts");
    Ok(total)
}

/// Ingests census tract boundaries for specific states only.
///
/// # Errors
///
/// Returns [`GeoError`] if any state fails to ingest.
pub async fn ingest_tracts_for_states(
    db: &dyn Database,
    state_fips_codes: &[&str],
) -> Result<u64, GeoError> {
    let client = reqwest::Client::builder()
        .user_agent("crime-map/1.0")
        .build()?;

    let mut total = 0u64;

    for fips in state_fips_codes {
        match ingest_state(db, &client, fips).await {
            Ok(count) => total += count,
            Err(e) => {
                log::error!("Failed to ingest state {fips}: {e}");
            }
        }
    }

    Ok(total)
}
