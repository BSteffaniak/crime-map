#![allow(clippy::module_name_repetitions)]

//! Interactive TUI for the crime data ingestion tool.
//!
//! Provides a menu-driven interface using `dialoguer` for running ingest
//! commands without memorizing CLI flags.

use std::time::Instant;

use dialoguer::{Confirm, Input, MultiSelect, Select};

use crime_map_cli_utils::{IndicatifProgress, MultiProgress};
use crime_map_database::{geocode_cache, source_db};

/// Top-level actions available in the ingest interactive menu.
enum IngestAction {
    SyncSources,
    ListSources,
    Geocode,
    IngestTracts,
    IngestPlaces,
    IngestNeighborhoods,
}

impl IngestAction {
    const ALL: &[Self] = &[
        Self::SyncSources,
        Self::ListSources,
        Self::Geocode,
        Self::IngestTracts,
        Self::IngestPlaces,
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
        IngestAction::Geocode => geocode_missing_interactive(multi)?,
        IngestAction::IngestTracts => ingest_census_tracts().await?,
        IngestAction::IngestPlaces => ingest_census_places().await?,
        IngestAction::IngestNeighborhoods => ingest_neighborhoods().await?,
    }

    Ok(())
}

/// Prompts the user to select one or more sources via checkboxes, then
/// syncs each selected source.
#[allow(clippy::future_not_send)]
async fn sync_sources(multi: &MultiProgress) -> Result<(), Box<dyn std::error::Error>> {
    let sources = crate::all_sources();
    if sources.is_empty() {
        println!("No sources configured.");
        return Ok(());
    }

    let labels: Vec<String> = sources
        .iter()
        .map(|s| format!("{} \u{2014} {}", s.id(), s.name()))
        .collect();

    let selected = MultiSelect::new()
        .with_prompt("Select sources to sync (space=toggle, a=all, enter=confirm)")
        .items(&labels)
        .max_length(20)
        .interact()?;

    if selected.is_empty() {
        println!("No sources selected.");
        return Ok(());
    }

    let limit = prompt_optional_u64("Record limit per source (empty for no limit)")?;
    let force = Confirm::new()
        .with_prompt("Force full sync?")
        .default(false)
        .interact()?;

    let num_sources = selected.len();
    log::info!(
        "Syncing {} source(s): {}",
        num_sources,
        selected
            .iter()
            .map(|&i| sources[i].id())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let source_bar = IndicatifProgress::steps_bar(multi, "Sources", num_sources as u64);

    for (i, &idx) in selected.iter().enumerate() {
        let src = &sources[idx];
        let fetch_bar = IndicatifProgress::records_bar(multi, src.name());
        source_bar.set_message(format!("Source {}/{num_sources}: {}", i + 1, src.name()));

        match source_db::open_by_id(src.id()) {
            Ok(conn) => {
                let result =
                    crate::sync_source(&conn, src, limit, force, Some(fetch_bar.clone())).await;
                fetch_bar.finish_and_clear();

                if let Err(e) = result {
                    log::error!("Failed to sync {}: {e}", src.id());
                }
            }
            Err(e) => {
                fetch_bar.finish_and_clear();
                log::error!("Failed to open DB for {}: {e}", src.id());
            }
        }

        source_bar.inc(1);
    }

    source_bar.finish(format!("Synced {num_sources} source(s)"));

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

/// Prompts for geocoding parameters and runs the geocode pipeline.
#[allow(clippy::too_many_lines)]
fn geocode_missing_interactive(multi: &MultiProgress) -> Result<(), Box<dyn std::error::Error>> {
    let limit = prompt_optional_u64("Total limit (empty for no limit)")?;

    let batch_size_str: String = Input::new()
        .with_prompt("Batch size")
        .default("50000".to_string())
        .interact_text()?;
    let batch_size: u64 = batch_size_str.parse().unwrap_or(50_000);

    let nominatim_only = Confirm::new()
        .with_prompt("Nominatim only (skip Census geocoder)?")
        .default(false)
        .interact()?;

    // Source filter — multi-select from all configured sources
    let all_sources = crate::all_sources();
    let source_labels: Vec<String> = all_sources
        .iter()
        .map(|s| format!("{} \u{2014} {}", s.id(), s.name()))
        .collect();

    let selected = MultiSelect::new()
        .with_prompt("Sources to geocode (space=toggle, a=all, enter=confirm)")
        .items(&source_labels)
        .max_length(20)
        .interact()?;

    let all_selected = selected.is_empty() || selected.len() == all_sources.len();

    // Build list of source definitions to process
    let target_sources: Vec<&crime_map_source::source_def::SourceDefinition> = if all_selected {
        all_sources.iter().collect()
    } else {
        selected.iter().map(|&idx| &all_sources[idx]).collect()
    };

    log::info!(
        "Geocoding {} source(s): {}",
        target_sources.len(),
        target_sources
            .iter()
            .map(|s| s.id())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let cache_conn = geocode_cache::open_default()?;
    let rt = tokio::runtime::Handle::current();

    let start = Instant::now();
    let geocode_bar = IndicatifProgress::batch_bar(multi, "Geocoding");

    // Phase 1: Geocode incidents that have no coordinates
    let mut missing_count = 0u64;
    for src in &target_sources {
        let source_conn = source_db::open_by_id(src.id())?;
        let count = crate::geocode_missing(
            &source_conn,
            &cache_conn,
            batch_size,
            limit,
            nominatim_only,
            Some(geocode_bar.clone()),
            &rt,
        )?;
        missing_count += count;

        if limit.is_some_and(|l| missing_count >= l) {
            break;
        }
    }

    // Phase 2: Re-geocode sources with imprecise coords
    let mut re_geocode_count = 0u64;
    let remaining_limit = limit.map(|l| l.saturating_sub(missing_count));
    if remaining_limit.is_none_or(|l| l > 0) {
        let re_geocode_sources: Vec<&&crime_map_source::source_def::SourceDefinition> =
            target_sources.iter().filter(|s| s.re_geocode()).collect();

        if !re_geocode_sources.is_empty() {
            log::info!(
                "Re-geocoding {} source(s) with imprecise coordinates...",
                re_geocode_sources.len()
            );
            for src in re_geocode_sources {
                let source_conn = source_db::open_by_id(src.id())?;
                let count = crate::re_geocode_source(
                    &source_conn,
                    &cache_conn,
                    batch_size,
                    remaining_limit,
                    nominatim_only,
                    Some(geocode_bar.clone()),
                    &rt,
                )?;
                re_geocode_count += count;
            }
        }
    }

    geocode_bar.finish("Geocoding complete".to_string());

    let total = missing_count + re_geocode_count;
    let elapsed = start.elapsed();
    log::info!(
        "Geocoding complete: {total} incidents geocoded ({missing_count} missing-coord + {re_geocode_count} re-geocoded) in {:.1}s",
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

    let selected = MultiSelect::new()
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

    for source in &sources_to_ingest {
        match crime_map_neighborhood::ingest::ingest_source(&boundaries_conn, &client, source).await
        {
            Ok(count) => total += count,
            Err(e) => {
                log::error!("Failed to ingest {}: {e}", source.id());
            }
        }
    }

    // Build the tract-to-neighborhood crosswalk
    if let Err(e) = crime_map_neighborhood::ingest::build_crosswalk(&boundaries_conn) {
        log::error!("Failed to build crosswalk: {e}");
    }

    let elapsed = start.elapsed();
    log::info!(
        "Neighborhood ingestion complete: {total} neighborhoods in {:.1}s",
        elapsed.as_secs_f64()
    );

    Ok(())
}

/// Prompts the user for an optional `u64` value. Returns `None` if the
/// input is empty.
fn prompt_optional_u64(prompt: &str) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let input: String = Input::new()
        .with_prompt(prompt)
        .allow_empty(true)
        .interact_text()?;

    if input.trim().is_empty() {
        Ok(None)
    } else {
        Ok(Some(input.trim().parse()?))
    }
}
