#![allow(clippy::module_name_repetitions)]

//! Interactive TUI for the crime data ingestion tool.
//!
//! Provides a menu-driven interface using `dialoguer` for running ingest
//! commands without memorizing CLI flags.

use std::time::Instant;

use dialoguer::{Confirm, Input, Select};

use crime_map_cli_utils::{IndicatifProgress, MultiProgress};

/// Top-level actions available in the ingest interactive menu.
enum IngestAction {
    SyncSources,
    ListSources,
    Geocode,
    IngestTracts,
    IngestPlaces,
    IngestCounties,
    IngestStates,
    IngestNeighborhoods,
}

impl IngestAction {
    const ALL: &[Self] = &[
        Self::SyncSources,
        Self::ListSources,
        Self::Geocode,
        Self::IngestTracts,
        Self::IngestPlaces,
        Self::IngestCounties,
        Self::IngestStates,
        Self::IngestNeighborhoods,
    ];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::SyncSources => "Sync sources",
            Self::ListSources => "List sources",
            Self::Geocode => "Geocode missing coordinates",
            Self::IngestTracts => "Ingest census tracts",
            Self::IngestPlaces => "Ingest census places",
            Self::IngestCounties => "Ingest counties",
            Self::IngestStates => "Ingest US state boundaries",
            Self::IngestNeighborhoods => "Ingest neighborhoods",
        }
    }
}

/// Runs the interactive menu loop, prompting the user to select and
/// configure ingest operations.
///
/// The `multi` parameter is the shared [`MultiProgress`] that is also
/// registered with the log bridge, so all `log::info!` output is
/// automatically suspended while progress bars redraw.
///
/// # Errors
///
/// Returns an error if database connection or any selected operation fails.
#[allow(clippy::too_many_lines, clippy::future_not_send)]
pub async fn run(multi: &MultiProgress) -> Result<(), Box<dyn std::error::Error>> {
    let labels: Vec<&str> = IngestAction::ALL.iter().map(IngestAction::label).collect();

    let idx = Select::new()
        .with_prompt("What would you like to do?")
        .items(&labels)
        .default(0)
        .interact()?;

    match IngestAction::ALL[idx] {
        IngestAction::SyncSources => sync_sources(multi).await?,
        IngestAction::ListSources => list_sources(),
        IngestAction::Geocode => geocode_interactive(multi)?,
        IngestAction::IngestTracts => ingest_census_tracts().await?,
        IngestAction::IngestPlaces => ingest_census_places().await?,
        IngestAction::IngestCounties => ingest_census_counties().await?,
        IngestAction::IngestStates => ingest_census_states().await?,
        IngestAction::IngestNeighborhoods => ingest_neighborhoods().await?,
    }

    Ok(())
}

/// Prompts the user to select sources, then syncs using [`crate::run_sync`].
#[allow(clippy::future_not_send)]
async fn sync_sources(multi: &MultiProgress) -> Result<(), Box<dyn std::error::Error>> {
    let source_ids = crime_map_cli_utils::prompt_source_multiselect(
        "Select sources to sync (space=toggle, a=all, enter=confirm)",
    )?;

    if source_ids.is_empty() {
        println!("No sources selected.");
        return Ok(());
    }

    let limit =
        crime_map_cli_utils::prompt_optional_u64("Record limit per source (empty for no limit)")?;
    let force = Confirm::new()
        .with_prompt("Force full sync?")
        .default(false)
        .interact()?;

    let num_sources = source_ids.len();
    let source_bar = IndicatifProgress::steps_bar(multi, "Sources", num_sources as u64);

    let args = crate::SyncArgs {
        source_ids,
        limit,
        force,
    };

    let result = crate::run_sync(&args, Some(&source_bar)).await;
    source_bar.finish(format!("Synced {num_sources} source(s)"));

    if !result.failed.is_empty() {
        log::error!(
            "{} source(s) failed: {}",
            result.failed.len(),
            result.failed.join(", ")
        );
    }

    Ok(())
}

/// Prints a table of all configured sources.
fn list_sources() {
    let sources = crate::all_sources();
    println!("{:<20} NAME", "ID");
    println!("{}", "-".repeat(50));
    for source in &sources {
        println!("{:<20} {}", source.id(), source.name());
    }
}

/// Prompts for geocoding parameters and runs via [`crate::run_geocode`].
fn geocode_interactive(multi: &MultiProgress) -> Result<(), Box<dyn std::error::Error>> {
    let source_ids = crime_map_cli_utils::prompt_source_multiselect(
        "Sources to geocode (space=toggle, a=all, enter=confirm)",
    )?;

    let limit = crime_map_cli_utils::prompt_optional_u64("Total limit (empty for no limit)")?;

    let batch_size_str: String = Input::new()
        .with_prompt("Batch size")
        .default("50000".to_string())
        .interact_text()?;
    let batch_size: u64 = batch_size_str.parse().unwrap_or(50_000);

    let nominatim_only = Confirm::new()
        .with_prompt("Nominatim only (skip Census geocoder)?")
        .default(false)
        .interact()?;

    let start = Instant::now();
    let geocode_bar = IndicatifProgress::batch_bar(multi, "Geocoding");

    let args = crate::GeocodeArgs {
        source_ids,
        batch_size,
        limit,
        nominatim_only,
    };

    let result = crate::run_geocode(&args, Some(geocode_bar.clone()))?;
    geocode_bar.finish("Geocoding complete".to_string());

    let elapsed = start.elapsed();
    log::info!(
        "Geocoding complete: {} incidents ({} missing-coord + {} re-geocoded) in {:.1}s",
        result.total(),
        result.missing_geocoded,
        result.re_geocoded,
        elapsed.as_secs_f64()
    );

    Ok(())
}

/// Prompts for state FIPS codes and ingests census tracts.
#[allow(clippy::future_not_send)]
async fn ingest_census_tracts() -> Result<(), Box<dyn std::error::Error>> {
    let boundaries_conn = crime_map_database::boundaries_db::open_default()?;

    let states_str: String = Input::new()
        .with_prompt("Comma-separated state FIPS codes (empty for all)")
        .allow_empty(true)
        .interact_text()?;

    let start = Instant::now();
    let total = if states_str.trim().is_empty() {
        log::info!("Ingesting census tracts for all states...");
        crime_map_geography::ingest::ingest_all_tracts(&boundaries_conn, false).await?
    } else {
        let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
        log::info!("Ingesting census tracts for states: {states_str}");
        crime_map_geography::ingest::ingest_tracts_for_states(&boundaries_conn, &fips_codes, false)
            .await?
    };

    let elapsed = start.elapsed();
    log::info!(
        "Census tract ingestion complete: {total} tracts in {:.1}s",
        elapsed.as_secs_f64()
    );

    Ok(())
}

/// Prompts for state FIPS codes and ingests census places.
#[allow(clippy::future_not_send)]
async fn ingest_census_places() -> Result<(), Box<dyn std::error::Error>> {
    let boundaries_conn = crime_map_database::boundaries_db::open_default()?;

    let states_str: String = Input::new()
        .with_prompt("Comma-separated state FIPS codes (empty for all)")
        .allow_empty(true)
        .interact_text()?;

    let start = Instant::now();
    let total = if states_str.trim().is_empty() {
        log::info!("Ingesting Census places for all states...");
        crime_map_geography::ingest::ingest_all_places(&boundaries_conn, false).await?
    } else {
        let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
        log::info!("Ingesting Census places for states: {states_str}");
        crime_map_geography::ingest::ingest_places_for_states(&boundaries_conn, &fips_codes, false)
            .await?
    };

    let elapsed = start.elapsed();
    log::info!(
        "Census place ingestion complete: {total} places in {:.1}s",
        elapsed.as_secs_f64()
    );

    Ok(())
}

/// Prompts for state FIPS codes and ingests county boundaries.
#[allow(clippy::future_not_send)]
async fn ingest_census_counties() -> Result<(), Box<dyn std::error::Error>> {
    let boundaries_conn = crime_map_database::boundaries_db::open_default()?;

    let states_str: String = Input::new()
        .with_prompt("Comma-separated state FIPS codes (empty for all)")
        .allow_empty(true)
        .interact_text()?;

    let start = Instant::now();
    let total = if states_str.trim().is_empty() {
        log::info!("Ingesting county boundaries for all states...");
        crime_map_geography::ingest::ingest_all_counties(&boundaries_conn, false).await?
    } else {
        let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
        log::info!("Ingesting county boundaries for states: {states_str}");
        crime_map_geography::ingest::ingest_counties_for_states(
            &boundaries_conn,
            &fips_codes,
            false,
        )
        .await?
    };

    let elapsed = start.elapsed();
    log::info!(
        "County boundary ingestion complete: {total} counties in {:.1}s",
        elapsed.as_secs_f64()
    );

    Ok(())
}

/// Ingests US state boundaries (all 50 states + DC).
#[allow(clippy::future_not_send)]
async fn ingest_census_states() -> Result<(), Box<dyn std::error::Error>> {
    let boundaries_conn = crime_map_database::boundaries_db::open_default()?;

    let start = Instant::now();
    log::info!("Ingesting US state boundaries...");
    let total = crime_map_geography::ingest::ingest_all_states(&boundaries_conn, false).await?;

    let elapsed = start.elapsed();
    log::info!(
        "State boundary ingestion complete: {total} states in {:.1}s",
        elapsed.as_secs_f64()
    );

    Ok(())
}

/// Prompts for source filter and ingests neighborhood boundaries.
#[allow(clippy::future_not_send)]
async fn ingest_neighborhoods() -> Result<(), Box<dyn std::error::Error>> {
    let boundaries_conn = crime_map_database::boundaries_db::open_default()?;

    let all_sources = crime_map_neighborhood::registry::all_sources();
    if all_sources.is_empty() {
        println!("No neighborhood sources configured.");
        return Ok(());
    }

    let labels: Vec<String> = all_sources
        .iter()
        .map(|s| format!("{} \u{2014} {}", s.id(), s.name()))
        .collect();

    let selected = dialoguer::MultiSelect::new()
        .with_prompt("Select neighborhood sources (space=toggle, a=all, enter=confirm)")
        .items(&labels)
        .max_length(20)
        .interact()?;

    if selected.is_empty() {
        println!("No sources selected.");
        return Ok(());
    }

    let sources_to_ingest: Vec<_> = selected.iter().map(|&i| &all_sources[i]).collect();

    log::info!(
        "Ingesting neighborhoods from {} source(s)",
        sources_to_ingest.len()
    );

    let client = reqwest::Client::builder()
        .user_agent("crime-map/1.0")
        .build()?;

    let start = Instant::now();
    let mut total = 0u64;
    let mut new_ingested = false;

    for source in &sources_to_ingest {
        // Skip sources that already have neighborhoods
        let existing: i64 = boundaries_conn.query_row(
            "SELECT COUNT(*) FROM neighborhoods WHERE source_id = ?",
            duckdb::params![source.id()],
            |row| row.get(0),
        )?;
        if existing > 0 {
            log::info!(
                "{}: {existing} neighborhoods already exist, skipping",
                source.id()
            );
            continue;
        }

        match crime_map_neighborhood::ingest::ingest_source(&boundaries_conn, &client, source).await
        {
            Ok(count) => {
                total += count;
                if count > 0 {
                    new_ingested = true;
                }
            }
            Err(e) => {
                log::error!("Failed to ingest {}: {e}", source.id());
            }
        }
    }

    // Build the tract-to-neighborhood crosswalk only if new data was ingested
    if new_ingested {
        if let Err(e) = crime_map_neighborhood::ingest::build_crosswalk(&boundaries_conn) {
            log::error!("Failed to build crosswalk: {e}");
        }
    } else {
        log::info!("No new neighborhoods ingested, skipping crosswalk rebuild");
    }

    let elapsed = start.elapsed();
    log::info!(
        "Neighborhood ingestion complete: {total} neighborhoods in {:.1}s",
        elapsed.as_secs_f64()
    );

    Ok(())
}
