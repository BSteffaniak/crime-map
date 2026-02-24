//! Census tract ingestion from the Census Bureau `TIGERweb` REST API.
//!
//! Downloads tract boundaries as `GeoJSON` from the ACS 2023 vintage
//! `TIGERweb` service and loads them into `PostGIS`.

use moosicbox_json_utils::database::ToValue as _;
use switchy_database::{Database, DatabaseValue};

use crate::GeoError;

/// Page size for `TIGERweb` paginated requests. Kept low to avoid WAF
/// blocks on large geospatial responses.
const TIGERWEB_PAGE_SIZE: u32 = 100;

/// Maximum retry attempts per page request.
const TIGERWEB_MAX_RETRIES: u32 = 5;

/// Delay between processing successive states to avoid WAF throttling.
const INTER_STATE_DELAY: std::time::Duration = std::time::Duration::from_millis(500);

/// Browser-like User-Agent to avoid WAF blocks on `TIGERweb`.
const TIGERWEB_USER_AGENT: &str = "Mozilla/5.0 (compatible; CrimeMap/1.0; +https://github.com)";

/// Builds a `reqwest::Client` configured for `TIGERweb` requests.
///
/// # Errors
///
/// Returns [`GeoError`] if the client cannot be built.
fn build_tigerweb_client() -> Result<reqwest::Client, GeoError> {
    reqwest::Client::builder()
        .user_agent(TIGERWEB_USER_AGENT)
        .build()
        .map_err(Into::into)
}

use crime_map_geography_models::fips::{STATE_FIPS, state_abbr};

// ============================================================
// Paginated TIGERweb fetcher
// ============================================================

/// Fetches all features from a `TIGERweb` `ArcGIS` REST endpoint using
/// paginated requests (`resultOffset` + `resultRecordCount`).
///
/// The `base_url` should contain all query parameters **except**
/// `resultOffset` and `resultRecordCount`. This function appends those
/// on each page and accumulates all features.
///
/// Each page request is retried up to 5 times with exponential backoff
/// on transient failures (WAF blocks, server errors). The raw response
/// body is logged on failure for debugging.
///
/// Works with both `f=geojson` (features have `properties`) and `f=json`
/// (features have `attributes`).
///
/// # Errors
///
/// Returns [`GeoError`] if all retry attempts are exhausted for any page.
#[allow(clippy::too_many_lines)]
async fn fetch_tigerweb_paginated(
    client: &reqwest::Client,
    base_url: &str,
    label: &str,
) -> Result<Vec<serde_json::Value>, GeoError> {
    let mut all_features: Vec<serde_json::Value> = Vec::new();
    let mut offset = 0u32;

    loop {
        let sep = if base_url.contains('?') { '&' } else { '?' };
        let url =
            format!("{base_url}{sep}resultRecordCount={TIGERWEB_PAGE_SIZE}&resultOffset={offset}");

        let json =
            fetch_tigerweb_page_with_retry(client, &url, label, offset, TIGERWEB_MAX_RETRIES)
                .await?;

        let features = json["features"]
            .as_array()
            .ok_or_else(|| GeoError::Conversion {
                message: format!(
                    "No features array in TIGERweb response for {label} (offset={offset})"
                ),
            })?;

        if features.is_empty() {
            break;
        }

        #[allow(clippy::cast_possible_truncation)]
        let page_len = features.len() as u32;

        all_features.extend(features.iter().cloned());

        // ArcGIS sets exceededTransferLimit=true when more pages exist
        let exceeded = json
            .get("exceededTransferLimit")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);

        if !exceeded {
            break;
        }

        offset += page_len;
        log::info!(
            "{label}: fetched {page_len} features (total so far: {}), fetching next page...",
            all_features.len()
        );

        // Courtesy delay between pages to avoid hammering the API
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    Ok(all_features)
}

/// Truncates a string for logging, appending "..." if it exceeds `max_len`.
fn truncate_for_log(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len])
    }
}

/// Fetches a single page from `TIGERweb` with retry logic and exponential
/// backoff. Logs the raw response body on failure for debugging.
async fn fetch_tigerweb_page_with_retry(
    client: &reqwest::Client,
    url: &str,
    label: &str,
    offset: u32,
    max_retries: u32,
) -> Result<serde_json::Value, GeoError> {
    let mut last_error = String::new();

    for attempt in 0..max_retries {
        if attempt > 0 {
            let delay_secs = 1u64 << (attempt + 1); // 4s, 8s
            log::warn!(
                "{label} (offset={offset}): retry {attempt}/{max_retries} in {delay_secs}s..."
            );
            tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;
        }

        // Send request
        let resp = match client.get(url).send().await {
            Ok(r) => r,
            Err(e) => {
                last_error = format!("HTTP request error: {e}");
                log::warn!("{label} (offset={offset}, attempt {attempt}): {last_error}");
                continue;
            }
        };

        let status = resp.status();
        let body = match resp.text().await {
            Ok(b) => b,
            Err(e) => {
                last_error = format!("Failed to read response body: {e}");
                log::warn!("{label} (offset={offset}, attempt {attempt}): {last_error}");
                continue;
            }
        };

        // Non-200 status
        if !status.is_success() {
            last_error = format!("HTTP {status}");
            log::warn!(
                "{label} (offset={offset}, attempt {attempt}): {last_error}. Response body: {}",
                truncate_for_log(&body, 500)
            );
            continue;
        }

        // Try to parse as JSON
        let json: serde_json::Value = match serde_json::from_str(&body) {
            Ok(j) => j,
            Err(e) => {
                last_error = format!("JSON parse error: {e}");
                log::warn!(
                    "{label} (offset={offset}, attempt {attempt}): {last_error}. Response body: {}",
                    truncate_for_log(&body, 500)
                );
                continue;
            }
        };

        // Check for ArcGIS error envelope: {"error": {"code": 500, "message": "..."}}
        if let Some(error_obj) = json.get("error") {
            let code = error_obj
                .get("code")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            let msg = error_obj
                .get("message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            last_error = format!("ArcGIS error {code}: {msg}");
            log::warn!(
                "{label} (offset={offset}, attempt {attempt}): {last_error}. Full error: {error_obj}"
            );
            continue;
        }

        return Ok(json);
    }

    Err(GeoError::Conversion {
        message: format!(
            "TIGERweb request failed after {max_retries} attempts for {label} (offset={offset}): {last_error}"
        ),
    })
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
    force: bool,
) -> Result<u64, GeoError> {
    let abbr = state_abbr(state_fips);

    // Skip if tracts already exist for this state (unless --force)
    if !force {
        let rows = db
            .query_raw_params(
                "SELECT COUNT(*) as count FROM census_tracts \
                 WHERE state_fips = $1 AND boundary IS NOT NULL",
                &[DatabaseValue::String(state_fips.to_string())],
            )
            .await?;
        let count: i64 = rows.first().map_or(0, |r| r.to_value("count").unwrap_or(0));
        if count > 0 {
            log::info!(
                "State {state_fips} ({abbr}): {count} tracts already exist, skipping \
                 (use --force to re-import)"
            );
            return Ok(0);
        }
    }

    let url = format!(
        "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2023/MapServer/8/query\
         ?where=STATE%3D%27{state_fips}%27\
         &outFields=GEOID,NAME,STATE,COUNTY,AREALAND,CENTLAT,CENTLON\
         &outSR=4326\
         &f=geojson\
         &returnGeometry=true"
    );

    let label = format!("tracts for state {state_fips} ({abbr})");
    log::info!("Fetching {label}...");

    let features = fetch_tigerweb_paginated(client, &url, &label).await?;

    let mut inserted = 0u64;

    for feature in &features {
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
pub async fn ingest_all_tracts(db: &dyn Database, force: bool) -> Result<u64, GeoError> {
    let client = build_tigerweb_client()?;

    let mut total = 0u64;
    let mut prev_fetched = false;

    for fips in STATE_FIPS {
        if prev_fetched {
            tokio::time::sleep(INTER_STATE_DELAY).await;
        }
        prev_fetched = false;
        match ingest_state(db, &client, fips, force).await {
            Ok(count) => {
                total += count;
                if count > 0 {
                    prev_fetched = true;
                }
                // Populate supplemental data for this state
                if let Err(e) = populate_population(db, &client, fips, force).await {
                    log::error!("Failed to populate population for state {fips}: {e}");
                }
                if let Err(e) = populate_county_names(db, &client, fips, force).await {
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
    force: bool,
) -> Result<u64, GeoError> {
    let client = build_tigerweb_client()?;

    let mut total = 0u64;
    let mut prev_fetched = false;

    for fips in state_fips_codes {
        if prev_fetched {
            tokio::time::sleep(INTER_STATE_DELAY).await;
        }
        prev_fetched = false;
        match ingest_state(db, &client, fips, force).await {
            Ok(count) => {
                total += count;
                if count > 0 {
                    prev_fetched = true;
                }
                if let Err(e) = populate_population(db, &client, fips, force).await {
                    log::error!("Failed to populate population for state {fips}: {e}");
                }
                if let Err(e) = populate_county_names(db, &client, fips, force).await {
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
    force: bool,
) -> Result<(), GeoError> {
    // Skip if all tracts in this state already have population data
    if !force {
        let rows = db
            .query_raw_params(
                "SELECT COUNT(*) as cnt FROM census_tracts \
                 WHERE state_fips = $1 AND population IS NULL AND boundary IS NOT NULL",
                &[DatabaseValue::String(state_fips.to_string())],
            )
            .await?;
        let unpopulated: i64 = rows.first().map_or(0, |r| r.to_value("cnt").unwrap_or(0));
        if unpopulated == 0 {
            let abbr = state_abbr(state_fips);
            log::info!("State {state_fips} ({abbr}): tract population already populated, skipping");
            return Ok(());
        }
    }

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
    force: bool,
) -> Result<(), GeoError> {
    // Skip if all tracts in this state already have county names
    if !force {
        let rows = db
            .query_raw_params(
                "SELECT COUNT(*) as cnt FROM census_tracts \
                 WHERE state_fips = $1 AND county_name IS NULL AND boundary IS NOT NULL",
                &[DatabaseValue::String(state_fips.to_string())],
            )
            .await?;
        let unpopulated: i64 = rows.first().map_or(0, |r| r.to_value("cnt").unwrap_or(0));
        if unpopulated == 0 {
            let abbr = state_abbr(state_fips);
            log::info!(
                "State {state_fips} ({abbr}): tract county names already populated, skipping"
            );
            return Ok(());
        }
    }

    let url = format!(
        "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2023/MapServer/82/query\
         ?where=STATE%3D%27{state_fips}%27\
         &outFields=STATE,COUNTY,BASENAME\
         &f=json\
         &returnGeometry=false"
    );

    let abbr = state_abbr(state_fips);
    let label = format!("county names for state {state_fips} ({abbr})");
    log::info!("Fetching {label}...");

    let features = fetch_tigerweb_paginated(client, &url, &label).await?;

    let mut updated = 0u64;

    for feature in &features {
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
    force: bool,
) -> Result<u64, GeoError> {
    let abbr = state_abbr(state_fips);

    // Skip if places of this type already exist for this state (unless --force)
    if !force {
        let rows = db
            .query_raw_params(
                "SELECT COUNT(*) as count FROM census_places \
                 WHERE state_fips = $1 AND place_type = $2 AND boundary IS NOT NULL",
                &[
                    DatabaseValue::String(state_fips.to_string()),
                    DatabaseValue::String(place_type.to_string()),
                ],
            )
            .await?;
        let count: i64 = rows.first().map_or(0, |r| r.to_value("count").unwrap_or(0));
        if count > 0 {
            log::info!(
                "State {state_fips} ({abbr}): {count} {place_type} places already exist, \
                 skipping (use --force to re-import)"
            );
            return Ok(0);
        }
    }

    let url = format!(
        "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2023/MapServer/{layer}/query\
         ?where=STATE%3D%27{state_fips}%27\
         &outFields=GEOID,BASENAME,NAME,STATE,PLACE,AREALAND,CENTLAT,CENTLON\
         &outSR=4326\
         &f=geojson\
         &returnGeometry=true"
    );

    let label = format!("{place_type} places for state {state_fips} ({abbr})");

    let features = fetch_tigerweb_paginated(client, &url, &label).await?;

    let mut inserted = 0u64;

    for feature in &features {
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
    force: bool,
) -> Result<u64, GeoError> {
    let mut total = 0u64;

    // Layer 28: Incorporated Places
    match ingest_places_layer(db, client, state_fips, 28, "incorporated", force).await {
        Ok(count) => total += count,
        Err(e) => log::error!("Failed to ingest incorporated places for state {state_fips}: {e}"),
    }

    // Layer 30: Census Designated Places
    match ingest_places_layer(db, client, state_fips, 30, "cdp", force).await {
        Ok(count) => total += count,
        Err(e) => log::error!("Failed to ingest CDPs for state {state_fips}: {e}"),
    }

    // Populate population data
    if let Err(e) = populate_place_population(db, client, state_fips, force).await {
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
    force: bool,
) -> Result<(), GeoError> {
    // Skip if all places in this state already have population data
    if !force {
        let rows = db
            .query_raw_params(
                "SELECT COUNT(*) as cnt FROM census_places \
                 WHERE state_fips = $1 AND population IS NULL AND boundary IS NOT NULL",
                &[DatabaseValue::String(state_fips.to_string())],
            )
            .await?;
        let unpopulated: i64 = rows.first().map_or(0, |r| r.to_value("cnt").unwrap_or(0));
        if unpopulated == 0 {
            let abbr = state_abbr(state_fips);
            log::info!("State {state_fips} ({abbr}): place population already populated, skipping");
            return Ok(());
        }
    }

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
pub async fn ingest_all_places(db: &dyn Database, force: bool) -> Result<u64, GeoError> {
    let client = build_tigerweb_client()?;

    let mut total = 0u64;
    let mut prev_fetched = false;

    for fips in STATE_FIPS {
        if prev_fetched {
            tokio::time::sleep(INTER_STATE_DELAY).await;
        }
        prev_fetched = false;
        match ingest_state_places(db, &client, fips, force).await {
            Ok(count) => {
                total += count;
                if count > 0 {
                    prev_fetched = true;
                }
            }
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
    force: bool,
) -> Result<u64, GeoError> {
    let client = build_tigerweb_client()?;

    let mut total = 0u64;
    let mut prev_fetched = false;

    for fips in state_fips_codes {
        if prev_fetched {
            tokio::time::sleep(INTER_STATE_DELAY).await;
        }
        prev_fetched = false;
        match ingest_state_places(db, &client, fips, force).await {
            Ok(count) => {
                total += count;
                if count > 0 {
                    prev_fetched = true;
                }
            }
            Err(e) => log::error!("Failed to ingest places for state {fips}: {e}"),
        }
    }

    Ok(total)
}

// ============================================================
// County boundary ingestion
// ============================================================

/// Downloads and inserts county boundaries for a single state from
/// `TIGERweb` Layer 82 (Counties).
async fn ingest_state_counties(
    db: &dyn Database,
    client: &reqwest::Client,
    state_fips: &str,
    force: bool,
) -> Result<u64, GeoError> {
    let abbr = state_abbr(state_fips);

    // Skip if counties already exist for this state (unless --force)
    if !force {
        let rows = db
            .query_raw_params(
                "SELECT COUNT(*) as count FROM census_counties \
                 WHERE state_fips = $1 AND boundary IS NOT NULL",
                &[DatabaseValue::String(state_fips.to_string())],
            )
            .await?;
        let count: i64 = rows.first().map_or(0, |r| r.to_value("count").unwrap_or(0));
        if count > 0 {
            log::info!(
                "State {state_fips} ({abbr}): {count} counties already exist, skipping \
                 (use --force to re-import)"
            );
            return Ok(0);
        }
    }

    let url = format!(
        "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2023/MapServer/82/query\
         ?where=STATE%3D%27{state_fips}%27\
         &outFields=GEOID,STATE,COUNTY,BASENAME,NAME,AREALAND,CENTLAT,CENTLON\
         &outSR=4326\
         &f=geojson\
         &returnGeometry=true"
    );

    let label = format!("county boundaries for state {state_fips} ({abbr})");
    log::info!("Fetching {label}...");

    let features = fetch_tigerweb_paginated(client, &url, &label).await?;

    let mut inserted = 0u64;

    for feature in &features {
        let props = &feature["properties"];
        let geoid = props["GEOID"].as_str().unwrap_or_default().to_string();

        if geoid.is_empty() {
            continue;
        }

        let county_fips = props["COUNTY"].as_str().unwrap_or("").to_string();
        let basename = props["BASENAME"]
            .as_str()
            .unwrap_or("Unknown County")
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
                "INSERT INTO census_counties (geoid, name, full_name, state_fips, county_fips, state_abbr, boundary, land_area_sq_mi, centroid_lon, centroid_lat)
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
        "State {state_fips} ({abbr}): inserted/updated {inserted} counties from {} features",
        features.len()
    );

    // Populate county population
    if let Err(e) = populate_county_population(db, client, state_fips, force).await {
        log::error!("Failed to populate county population for state {state_fips}: {e}");
    }

    Ok(inserted)
}

/// Fetches ACS 5-year population estimates for counties and updates
/// the `census_counties` table.
async fn populate_county_population(
    db: &dyn Database,
    client: &reqwest::Client,
    state_fips: &str,
    force: bool,
) -> Result<(), GeoError> {
    // Skip if all counties in this state already have population data
    if !force {
        let rows = db
            .query_raw_params(
                "SELECT COUNT(*) as cnt FROM census_counties \
                 WHERE state_fips = $1 AND population IS NULL AND boundary IS NOT NULL",
                &[DatabaseValue::String(state_fips.to_string())],
            )
            .await?;
        let unpopulated: i64 = rows.first().map_or(0, |r| r.to_value("cnt").unwrap_or(0));
        if unpopulated == 0 {
            let abbr = state_abbr(state_fips);
            log::info!(
                "State {state_fips} ({abbr}): county population already populated, skipping"
            );
            return Ok(());
        }
    }

    let url = format!(
        "https://api.census.gov/data/2023/acs/acs5\
         ?get=B01001_001E\
         &for=county:*\
         &in=state:{state_fips}"
    );

    log::info!("Fetching ACS county population data for state FIPS {state_fips}...");

    let resp = client.get(&url).send().await?;
    let body = resp.text().await?;

    let rows: Vec<Vec<String>> = serde_json::from_str(&body).map_err(|e| GeoError::Conversion {
        message: format!("Failed to parse ACS county response for state {state_fips}: {e}"),
    })?;

    let mut updated = 0u64;

    for row in rows.iter().skip(1) {
        if row.len() < 3 {
            continue;
        }

        let population: Option<i32> = row[0].parse().ok();
        let state = &row[1];
        let county = &row[2];
        let geoid = format!("{state}{county}");

        if let Some(pop) = population {
            let result = db
                .exec_raw_params(
                    "UPDATE census_counties SET population = $1 WHERE geoid = $2",
                    &[DatabaseValue::Int32(pop), DatabaseValue::String(geoid)],
                )
                .await?;
            updated += result;
        }
    }

    let abbr = state_abbr(state_fips);
    log::info!("State {state_fips} ({abbr}): updated population for {updated} counties");

    Ok(())
}

/// Ingests county boundaries for all US states.
///
/// Downloads from the `TIGERweb` REST API and loads into `PostGIS`.
///
/// # Errors
///
/// Returns [`GeoError`] if any state fails to ingest.
pub async fn ingest_all_counties(db: &dyn Database, force: bool) -> Result<u64, GeoError> {
    let client = build_tigerweb_client()?;

    let mut total = 0u64;
    let mut prev_fetched = false;

    for fips in STATE_FIPS {
        if prev_fetched {
            tokio::time::sleep(INTER_STATE_DELAY).await;
        }
        prev_fetched = false;
        match ingest_state_counties(db, &client, fips, force).await {
            Ok(count) => {
                total += count;
                if count > 0 {
                    prev_fetched = true;
                }
            }
            Err(e) => log::error!("Failed to ingest counties for state {fips}: {e}"),
        }
    }

    log::info!("County boundary ingestion complete: {total} total counties");
    Ok(total)
}

/// Ingests county boundaries for specific states only.
///
/// # Errors
///
/// Returns [`GeoError`] if any state fails to ingest.
pub async fn ingest_counties_for_states(
    db: &dyn Database,
    state_fips_codes: &[&str],
    force: bool,
) -> Result<u64, GeoError> {
    let client = build_tigerweb_client()?;

    let mut total = 0u64;
    let mut prev_fetched = false;

    for fips in state_fips_codes {
        if prev_fetched {
            tokio::time::sleep(INTER_STATE_DELAY).await;
        }
        prev_fetched = false;
        match ingest_state_counties(db, &client, fips, force).await {
            Ok(count) => {
                total += count;
                if count > 0 {
                    prev_fetched = true;
                }
            }
            Err(e) => log::error!("Failed to ingest counties for state {fips}: {e}"),
        }
    }

    Ok(total)
}

// ============================================================
// State boundary ingestion
// ============================================================

use crime_map_geography_models::fips::state_name;

/// Downloads and inserts all US state boundaries from `TIGERweb` Layer 84
/// (States).
///
/// # Errors
///
/// Returns [`GeoError`] if the HTTP request or database operation fails.
pub async fn ingest_all_states(db: &dyn Database, force: bool) -> Result<u64, GeoError> {
    let client = build_tigerweb_client()?;

    // Skip if states already exist (unless --force)
    if !force {
        let rows = db
            .query_raw_params(
                "SELECT COUNT(*) as count FROM census_states WHERE boundary IS NOT NULL",
                &[],
            )
            .await?;
        let count: i64 = rows.first().map_or(0, |r| r.to_value("count").unwrap_or(0));
        if count > 0 {
            log::info!(
                "{count} state boundaries already exist, skipping (use --force to re-import)"
            );
            return Ok(0);
        }
    }

    // Query all states at once (only 51 features)
    let url = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_ACS2023/MapServer/84/query\
         ?where=1%3D1\
         &outFields=GEOID,STATE,BASENAME,NAME,AREALAND,CENTLAT,CENTLON\
         &outSR=4326\
         &f=geojson\
         &returnGeometry=true";

    log::info!("Fetching all state boundaries from TIGERweb...");

    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        return Err(GeoError::Conversion {
            message: format!(
                "TIGERweb states request failed with status {}",
                resp.status()
            ),
        });
    }
    let body = resp.text().await?;

    let geojson: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| GeoError::Conversion {
            message: format!("Failed to parse state GeoJSON: {e}"),
        })?;

    let features = geojson["features"]
        .as_array()
        .ok_or_else(|| GeoError::Conversion {
            message: "No features array in state response".to_string(),
        })?;

    let mut inserted = 0u64;

    for feature in features {
        let props = &feature["properties"];
        let fips = props["STATE"].as_str().unwrap_or_default().to_string();

        if fips.is_empty() {
            continue;
        }

        // Only ingest the 50 states + DC
        if !STATE_FIPS.contains(&fips.as_str()) {
            continue;
        }

        let abbr = state_abbr(&fips);
        let name = state_name(&fips);

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
                "INSERT INTO census_states (fips, name, abbr, boundary, land_area_sq_mi, centroid_lon, centroid_lat)
                 VALUES ($1, $2, $3, ST_Multi(ST_GeomFromGeoJSON($4))::geography, $5, $6, $7)
                 ON CONFLICT (fips) DO UPDATE SET
                     name = EXCLUDED.name,
                     abbr = EXCLUDED.abbr,
                     boundary = EXCLUDED.boundary,
                     land_area_sq_mi = EXCLUDED.land_area_sq_mi,
                     centroid_lon = EXCLUDED.centroid_lon,
                     centroid_lat = EXCLUDED.centroid_lat",
                &[
                    DatabaseValue::String(fips.clone()),
                    DatabaseValue::String(name.to_string()),
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

    // Populate state populations
    if let Err(e) = populate_state_population(db, &client).await {
        log::error!("Failed to populate state populations: {e}");
    }

    log::info!("State boundary ingestion complete: {inserted} states");
    Ok(inserted)
}

/// Fetches ACS 5-year population estimates for all states and updates
/// the `census_states` table.
async fn populate_state_population(
    db: &dyn Database,
    client: &reqwest::Client,
) -> Result<(), GeoError> {
    let url = "https://api.census.gov/data/2023/acs/acs5\
         ?get=B01001_001E\
         &for=state:*";

    log::info!("Fetching ACS state population data...");

    let resp = client.get(url).send().await?;
    let body = resp.text().await?;

    let rows: Vec<Vec<String>> = serde_json::from_str(&body).map_err(|e| GeoError::Conversion {
        message: format!("Failed to parse ACS state response: {e}"),
    })?;

    let mut updated = 0u64;

    for row in rows.iter().skip(1) {
        if row.len() < 2 {
            continue;
        }

        let population: Option<i64> = row[0].parse().ok();
        let state_fips = &row[1];

        if let Some(pop) = population {
            let result = db
                .exec_raw_params(
                    "UPDATE census_states SET population = $1 WHERE fips = $2",
                    &[
                        DatabaseValue::Int64(pop),
                        DatabaseValue::String(state_fips.clone()),
                    ],
                )
                .await?;
            updated += result;
        }
    }

    log::info!("Updated population for {updated} states");
    Ok(())
}
