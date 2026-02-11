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
         &outFields=GEOID,NAME,STATE,COUNTY,AREALAND,CENTLAT,CENTLON\
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

        let aland = props["AREALAND"].as_f64();
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
/// After loading boundaries, also fetches population data from the ACS
/// and county names from `TIGERweb`.
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
            Ok(count) => {
                total += count;
                // Populate supplemental data for this state
                if let Err(e) = populate_population(db, &client, fips).await {
                    log::error!("Failed to populate population for state {fips}: {e}");
                }
                if let Err(e) = populate_county_names(db, &client, fips).await {
                    log::error!("Failed to populate county names for state {fips}: {e}");
                }
            }
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
/// Also fetches population data and county names for the specified states.
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
            Ok(count) => {
                total += count;
                if let Err(e) = populate_population(db, &client, fips).await {
                    log::error!("Failed to populate population for state {fips}: {e}");
                }
                if let Err(e) = populate_county_names(db, &client, fips).await {
                    log::error!("Failed to populate county names for state {fips}: {e}");
                }
            }
            Err(e) => {
                log::error!("Failed to ingest state {fips}: {e}");
            }
        }
    }

    Ok(total)
}

/// Fetches ACS 5-year population estimates and updates the `census_tracts`
/// table.
///
/// Uses the Census Bureau API to get the total population (`B01001_001E`)
/// for every tract in a state. No API key is required.
///
/// # Errors
///
/// Returns [`GeoError`] if the HTTP request or database update fails.
async fn populate_population(
    db: &dyn Database,
    client: &reqwest::Client,
    state_fips: &str,
) -> Result<(), GeoError> {
    let url = format!(
        "https://api.census.gov/data/2023/acs/acs5\
         ?get=B01001_001E\
         &for=tract:*\
         &in=state:{state_fips}"
    );

    log::info!("Fetching ACS population data for state FIPS {state_fips}...");

    let resp = client.get(&url).send().await?;
    let body = resp.text().await?;

    // Response is a JSON array of arrays:
    // [["B01001_001E","state","county","tract"],
    //  ["1181","11","001","000101"], ...]
    let rows: Vec<Vec<String>> = serde_json::from_str(&body).map_err(|e| GeoError::Conversion {
        message: format!("Failed to parse ACS response for state {state_fips}: {e}"),
    })?;

    let mut updated = 0u64;

    // Skip the header row
    for row in rows.iter().skip(1) {
        if row.len() < 4 {
            continue;
        }

        let population: Option<i32> = row[0].parse().ok();
        let state = &row[1];
        let county = &row[2];
        let tract = &row[3];

        // Construct the GEOID: state FIPS + county FIPS + tract code
        let geoid = format!("{state}{county}{tract}");

        if let Some(pop) = population {
            let result = db
                .exec_raw_params(
                    "UPDATE census_tracts SET population = $1 WHERE geoid = $2",
                    &[DatabaseValue::Int32(pop), DatabaseValue::String(geoid)],
                )
                .await?;
            updated += result;
        }
    }

    let abbr = state_abbr(state_fips);
    log::info!("State {state_fips} ({abbr}): updated population for {updated} tracts");

    Ok(())
}

/// Fetches county names from the `TIGERweb` Counties layer and updates
/// the `census_tracts` table.
///
/// # Errors
///
/// Returns [`GeoError`] if the HTTP request or database update fails.
async fn populate_county_names(
    db: &dyn Database,
    client: &reqwest::Client,
    state_fips: &str,
) -> Result<(), GeoError> {
    let url = format!(
        "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2023/MapServer/82/query\
         ?where=STATE%3D'{state_fips}'\
         &outFields=STATE,COUNTY,BASENAME\
         &f=json\
         &returnGeometry=false"
    );

    log::info!("Fetching county names for state FIPS {state_fips}...");

    let resp = client.get(&url).send().await?;
    let body = resp.text().await?;

    let json: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| GeoError::Conversion {
            message: format!("Failed to parse county response for state {state_fips}: {e}"),
        })?;

    let features = json["features"]
        .as_array()
        .ok_or_else(|| GeoError::Conversion {
            message: format!("No features in county response for state {state_fips}"),
        })?;

    let mut updated = 0u64;

    for feature in features {
        let attrs = &feature["attributes"];
        let county_fips = attrs["COUNTY"].as_str().unwrap_or_default();
        let county_name = attrs["BASENAME"].as_str().unwrap_or_default();

        if county_fips.is_empty() || county_name.is_empty() {
            continue;
        }

        let result = db
            .exec_raw_params(
                "UPDATE census_tracts SET county_name = $1 \
                 WHERE state_fips = $2 AND county_fips = $3",
                &[
                    DatabaseValue::String(county_name.to_string()),
                    DatabaseValue::String(state_fips.to_string()),
                    DatabaseValue::String(county_fips.to_string()),
                ],
            )
            .await?;
        updated += result;
    }

    let abbr = state_abbr(state_fips);
    log::info!(
        "State {state_fips} ({abbr}): updated county names for {updated} tracts ({} counties)",
        features.len()
    );

    Ok(())
}

/// Downloads and inserts Census places (incorporated cities and CDPs) for a
/// single state from a specific `TIGERweb` layer.
///
/// Layer 28 = Incorporated Places, Layer 30 = Census Designated Places.
async fn ingest_places_layer(
    db: &dyn Database,
    client: &reqwest::Client,
    state_fips: &str,
    layer: u32,
    place_type: &str,
) -> Result<u64, GeoError> {
    let url = format!(
        "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2023/MapServer/{layer}/query\
         ?where=STATE%3D'{state_fips}'\
         &outFields=GEOID,BASENAME,NAME,STATE,PLACE,AREALAND,CENTLAT,CENTLON\
         &outSR=4326\
         &f=geojson\
         &returnGeometry=true"
    );

    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        return Err(GeoError::Conversion {
            message: format!(
                "TIGERweb layer {layer} request failed with status {} for state {state_fips}",
                resp.status()
            ),
        });
    }
    let body = resp.text().await?;

    let geojson: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| GeoError::Conversion {
            message: format!("Failed to parse GeoJSON for layer {layer}, state {state_fips}: {e}"),
        })?;

    let features = geojson["features"]
        .as_array()
        .ok_or_else(|| GeoError::Conversion {
            message: format!("No features array for layer {layer}, state {state_fips}"),
        })?;

    let abbr = state_abbr(state_fips);
    let mut inserted = 0u64;

    for feature in features {
        let props = &feature["properties"];
        let geoid = props["GEOID"].as_str().unwrap_or_default().to_string();

        if geoid.is_empty() {
            continue;
        }

        let basename = props["BASENAME"]
            .as_str()
            .unwrap_or("Unknown Place")
            .to_string();

        let full_name = props["NAME"].as_str().unwrap_or(&basename).to_string();

        let aland = props["AREALAND"].as_f64();
        let land_area_sq_mi = aland.map(|a| a / 2_589_988.11);

        let centlat = props["CENTLAT"]
            .as_str()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .or_else(|| props["CENTLAT"].as_f64());

        let centlon = props["CENTLON"]
            .as_str()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .or_else(|| props["CENTLON"].as_f64());

        let geometry = &feature["geometry"];
        let geom_str = serde_json::to_string(geometry).unwrap_or_default();

        if geom_str.is_empty() || geom_str == "null" {
            continue;
        }

        let result = db
            .exec_raw_params(
                "INSERT INTO census_places (geoid, name, full_name, state_fips, state_abbr, place_type, boundary, land_area_sq_mi, centroid_lon, centroid_lat)
                 VALUES ($1, $2, $3, $4, $5, $6, ST_Multi(ST_GeomFromGeoJSON($7))::geography, $8, $9, $10)
                 ON CONFLICT (geoid) DO UPDATE SET
                     name = EXCLUDED.name,
                     full_name = EXCLUDED.full_name,
                     boundary = EXCLUDED.boundary,
                     land_area_sq_mi = EXCLUDED.land_area_sq_mi,
                     centroid_lon = EXCLUDED.centroid_lon,
                     centroid_lat = EXCLUDED.centroid_lat",
                &[
                    DatabaseValue::String(geoid),
                    DatabaseValue::String(basename),
                    DatabaseValue::String(full_name),
                    DatabaseValue::String(state_fips.to_string()),
                    DatabaseValue::String(abbr.to_string()),
                    DatabaseValue::String(place_type.to_string()),
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
        "State {state_fips} ({abbr}): inserted/updated {inserted} {place_type} places from {} features",
        features.len()
    );
    Ok(inserted)
}

/// Downloads and inserts Census places for a single state.
///
/// Fetches both Incorporated Places (layer 28) and Census Designated
/// Places (layer 30), then populates population data from the ACS.
async fn ingest_state_places(
    db: &dyn Database,
    client: &reqwest::Client,
    state_fips: &str,
) -> Result<u64, GeoError> {
    let mut total = 0u64;

    // Layer 28: Incorporated Places
    match ingest_places_layer(db, client, state_fips, 28, "incorporated").await {
        Ok(count) => total += count,
        Err(e) => log::error!("Failed to ingest incorporated places for state {state_fips}: {e}"),
    }

    // Layer 30: Census Designated Places
    match ingest_places_layer(db, client, state_fips, 30, "cdp").await {
        Ok(count) => total += count,
        Err(e) => log::error!("Failed to ingest CDPs for state {state_fips}: {e}"),
    }

    // Populate population data
    if let Err(e) = populate_place_population(db, client, state_fips).await {
        log::error!("Failed to populate place population for state {state_fips}: {e}");
    }

    Ok(total)
}

/// Fetches ACS 5-year population estimates for Census places and updates
/// the `census_places` table.
///
/// # Errors
///
/// Returns [`GeoError`] if the HTTP request or database update fails.
async fn populate_place_population(
    db: &dyn Database,
    client: &reqwest::Client,
    state_fips: &str,
) -> Result<(), GeoError> {
    let url = format!(
        "https://api.census.gov/data/2023/acs/acs5\
         ?get=B01001_001E\
         &for=place:*\
         &in=state:{state_fips}"
    );

    log::info!("Fetching ACS place population data for state FIPS {state_fips}...");

    let resp = client.get(&url).send().await?;
    let body = resp.text().await?;

    // Response: [["B01001_001E","state","place"], ["4337","24","01600"], ...]
    let rows: Vec<Vec<String>> = serde_json::from_str(&body).map_err(|e| GeoError::Conversion {
        message: format!("Failed to parse ACS place response for state {state_fips}: {e}"),
    })?;

    let mut updated = 0u64;

    for row in rows.iter().skip(1) {
        if row.len() < 3 {
            continue;
        }

        let population: Option<i32> = row[0].parse().ok();
        let state = &row[1];
        let place = &row[2];
        let geoid = format!("{state}{place}");

        if let Some(pop) = population {
            let result = db
                .exec_raw_params(
                    "UPDATE census_places SET population = $1 WHERE geoid = $2",
                    &[DatabaseValue::Int32(pop), DatabaseValue::String(geoid)],
                )
                .await?;
            updated += result;
        }
    }

    let abbr = state_abbr(state_fips);
    log::info!("State {state_fips} ({abbr}): updated population for {updated} places");

    Ok(())
}

/// Ingests Census place boundaries for all US states.
///
/// Downloads Incorporated Places and CDPs from `TIGERweb`, loads into
/// `PostGIS`, then fetches ACS population data.
///
/// # Errors
///
/// Returns [`GeoError`] if any state fails to ingest.
pub async fn ingest_all_places(db: &dyn Database) -> Result<u64, GeoError> {
    let client = reqwest::Client::builder()
        .user_agent("crime-map/1.0")
        .build()?;

    let mut total = 0u64;

    for fips in STATE_FIPS {
        match ingest_state_places(db, &client, fips).await {
            Ok(count) => total += count,
            Err(e) => log::error!("Failed to ingest places for state {fips}: {e}"),
        }
    }

    log::info!("Census place ingestion complete: {total} total places");
    Ok(total)
}

/// Ingests Census place boundaries for specific states only.
///
/// # Errors
///
/// Returns [`GeoError`] if any state fails to ingest.
pub async fn ingest_places_for_states(
    db: &dyn Database,
    state_fips_codes: &[&str],
) -> Result<u64, GeoError> {
    let client = reqwest::Client::builder()
        .user_agent("crime-map/1.0")
        .build()?;

    let mut total = 0u64;

    for fips in state_fips_codes {
        match ingest_state_places(db, &client, fips).await {
            Ok(count) => total += count,
            Err(e) => log::error!("Failed to ingest places for state {fips}: {e}"),
        }
    }

    Ok(total)
}
