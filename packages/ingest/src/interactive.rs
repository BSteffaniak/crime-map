#![allow(clippy::module_name_repetitions)]

//! Interactive TUI for the crime data ingestion tool.
//!
//! Provides a menu-driven interface using `dialoguer` for running ingest
//! commands without memorizing CLI flags.

use std::time::Instant;

use dialoguer::{Confirm, Input, MultiSelect, Select};

use crime_map_cli_utils::{IndicatifProgress, MultiProgress};

/// Top-level actions available in the ingest interactive menu.
enum IngestAction {
    SyncSources,
    ListSources,
    Geocode,
    Attribute,
    IngestTracts,
    IngestPlaces,
    IngestNeighborhoods,
    RunMigrations,
}

impl IngestAction {
    const ALL: &[Self] = &[
        Self::SyncSources,
        Self::ListSources,
        Self::Geocode,
        Self::Attribute,
        Self::IngestTracts,
        Self::IngestPlaces,
        Self::IngestNeighborhoods,
        Self::RunMigrations,
    ];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::SyncSources => "Sync sources",
            Self::ListSources => "List sources",
            Self::Geocode => "Geocode missing coordinates",
            Self::Attribute => "Attribute census data",
            Self::IngestTracts => "Ingest census tracts",
            Self::IngestPlaces => "Ingest census places",
            Self::IngestNeighborhoods => "Ingest neighborhoods",
            Self::RunMigrations => "Run database migrations",
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
/// Returns an error if database connection, migrations, or any selected
/// operation fails.
#[allow(clippy::too_many_lines)]
pub async fn run(multi: &MultiProgress) -> Result<(), Box<dyn std::error::Error>> {
    let db = crime_map_database::db::connect_from_env().await?;
    crime_map_database::run_migrations(db.as_ref()).await?;

    let labels: Vec<&str> = IngestAction::ALL.iter().map(IngestAction::label).collect();

    let idx = Select::new()
        .with_prompt("What would you like to do?")
        .items(&labels)
        .default(0)
        .interact()?;

    match IngestAction::ALL[idx] {
        IngestAction::SyncSources => sync_sources(db.as_ref(), multi).await?,
        IngestAction::ListSources => list_sources(),
        IngestAction::Geocode => geocode_missing_interactive(db.as_ref(), multi).await?,
        IngestAction::Attribute => attribute_census_data(db.as_ref(), multi).await?,
        IngestAction::IngestTracts => ingest_census_tracts(db.as_ref()).await?,
        IngestAction::IngestPlaces => ingest_census_places(db.as_ref()).await?,
        IngestAction::IngestNeighborhoods => ingest_neighborhoods(db.as_ref()).await?,
        IngestAction::RunMigrations => {
            log::info!("Running database migrations...");
            crime_map_database::run_migrations(db.as_ref()).await?;
            log::info!("Migrations complete.");
        }
    }

    Ok(())
}

/// Prompts the user to select one or more sources via checkboxes, then
/// syncs each selected source.
async fn sync_sources(
    db: &dyn switchy_database::Database,
    multi: &MultiProgress,
) -> Result<(), Box<dyn std::error::Error>> {
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

        let result = crate::sync_source(db, src, limit, force, Some(fetch_bar.clone())).await;
        fetch_bar.finish_and_clear();

        if let Err(e) = result {
            log::error!("Failed to sync {}: {e}", src.id());
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
async fn geocode_missing_interactive(
    db: &dyn switchy_database::Database,
    multi: &MultiProgress,
) -> Result<(), Box<dyn std::error::Error>> {
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

    // Resolve DB IDs and TOML IDs for selected sources
    let all_selected = selected.len() == all_sources.len();
    let mut source_db_ids: Vec<i32> = Vec::new();
    let mut source_toml_ids: Vec<String> = Vec::new();

    if !all_selected {
        for &idx in &selected {
            let src = &all_sources[idx];
            let sid = crime_map_database::queries::get_source_id_by_name(db, src.name()).await?;
            source_db_ids.push(sid);
            source_toml_ids.push(src.id().to_string());
        }
        log::info!(
            "Filtering to {} source(s): {}",
            selected.len(),
            source_toml_ids.join(", ")
        );
    }

    let start = Instant::now();

    let geocode_bar = IndicatifProgress::batch_bar(multi, "Geocoding");

    // Phase 1: Geocode incidents that have no coordinates
    let missing_count = if all_selected {
        crate::geocode_missing(
            db,
            batch_size,
            limit,
            nominatim_only,
            None,
            Some(geocode_bar.clone()),
        )
        .await?
    } else {
        let mut count = 0u64;
        for &sid in &source_db_ids {
            count += crate::geocode_missing(
                db,
                batch_size,
                limit,
                nominatim_only,
                Some(sid),
                Some(geocode_bar.clone()),
            )
            .await?;
        }
        count
    };

    // Phase 2: Re-geocode sources with imprecise coords
    let mut re_geocode_count = 0u64;
    if all_selected {
        let re_geocode_ids = crate::resolve_re_geocode_source_ids(db, None).await?;
        if !re_geocode_ids.is_empty() {
            let remaining_limit = limit.map(|l| l.saturating_sub(missing_count));
            if remaining_limit.is_none_or(|l| l > 0) {
                log::info!(
                    "Re-geocoding {} source(s) with imprecise coordinates...",
                    re_geocode_ids.len()
                );
                for sid in &re_geocode_ids {
                    let count = crate::re_geocode_source(
                        db,
                        batch_size,
                        remaining_limit,
                        nominatim_only,
                        Some(*sid),
                        Some(geocode_bar.clone()),
                    )
                    .await?;
                    re_geocode_count += count;
                }
            }
        }
    } else {
        for toml_id in &source_toml_ids {
            let re_geocode_ids =
                crate::resolve_re_geocode_source_ids(db, Some(toml_id.as_str())).await?;
            if !re_geocode_ids.is_empty() {
                let remaining_limit = limit.map(|l| l.saturating_sub(missing_count));
                if remaining_limit.is_none_or(|l| l > 0) {
                    for sid in &re_geocode_ids {
                        let count = crate::re_geocode_source(
                            db,
                            batch_size,
                            remaining_limit,
                            nominatim_only,
                            Some(*sid),
                            Some(geocode_bar.clone()),
                        )
                        .await?;
                        re_geocode_count += count;
                    }
                }
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

/// Prompts for attribution parameters and runs place/tract attribution.
async fn attribute_census_data(
    db: &dyn switchy_database::Database,
    multi: &MultiProgress,
) -> Result<(), Box<dyn std::error::Error>> {
    let buffer_str: String = Input::new()
        .with_prompt("Buffer distance in meters")
        .default("5".to_string())
        .interact_text()?;
    let buffer: f64 = buffer_str.parse().unwrap_or(5.0);

    let batch_size_str: String = Input::new()
        .with_prompt("Batch size")
        .default("5000".to_string())
        .interact_text()?;
    let batch_size: u32 = batch_size_str.parse().unwrap_or(5000);

    let mode_choices = &["Both places and tracts", "Places only", "Tracts only"];
    let mode = Select::new()
        .with_prompt("What to attribute")
        .items(mode_choices)
        .default(0)
        .interact()?;

    let (places_only, tracts_only) = match mode {
        1 => (true, false),
        2 => (false, true),
        _ => (false, false),
    };

    let start = Instant::now();

    if !tracts_only {
        log::info!(
            "Attributing incidents to census places (buffer={buffer}m, batch={batch_size})..."
        );
        let places_bar = IndicatifProgress::batch_bar(multi, "Place attribution");
        let place_count = crime_map_database::queries::attribute_places(
            db,
            buffer,
            batch_size,
            None,
            Some(places_bar),
        )
        .await?;
        log::info!("Attributed {place_count} incidents to census places");
    }

    if !places_only {
        log::info!("Attributing incidents to census tracts (batch={batch_size})...");
        let tracts_bar = IndicatifProgress::batch_bar(multi, "Tract attribution");
        let tract_count =
            crime_map_database::queries::attribute_tracts(db, batch_size, None, Some(tracts_bar))
                .await?;
        log::info!("Attributed {tract_count} incidents to census tracts");
    }

    let elapsed = start.elapsed();
    log::info!("Attribution complete in {:.1}s", elapsed.as_secs_f64());

    Ok(())
}

/// Prompts for state FIPS codes and ingests census tracts.
async fn ingest_census_tracts(
    db: &dyn switchy_database::Database,
) -> Result<(), Box<dyn std::error::Error>> {
    let states_str: String = Input::new()
        .with_prompt("Comma-separated state FIPS codes (empty for all)")
        .allow_empty(true)
        .interact_text()?;

    let start = Instant::now();
    let total = if states_str.trim().is_empty() {
        log::info!("Ingesting census tracts for all states...");
        crime_map_geography::ingest::ingest_all_tracts(db, false).await?
    } else {
        let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
        log::info!("Ingesting census tracts for states: {states_str}");
        crime_map_geography::ingest::ingest_tracts_for_states(db, &fips_codes, false).await?
    };

    let elapsed = start.elapsed();
    log::info!(
        "Census tract ingestion complete: {total} tracts in {:.1}s",
        elapsed.as_secs_f64()
    );

    Ok(())
}

/// Prompts for state FIPS codes and ingests census places.
async fn ingest_census_places(
    db: &dyn switchy_database::Database,
) -> Result<(), Box<dyn std::error::Error>> {
    let states_str: String = Input::new()
        .with_prompt("Comma-separated state FIPS codes (empty for all)")
        .allow_empty(true)
        .interact_text()?;

    let start = Instant::now();
    let total = if states_str.trim().is_empty() {
        log::info!("Ingesting Census places for all states...");
        crime_map_geography::ingest::ingest_all_places(db, false).await?
    } else {
        let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
        log::info!("Ingesting Census places for states: {states_str}");
        crime_map_geography::ingest::ingest_places_for_states(db, &fips_codes, false).await?
    };

    let elapsed = start.elapsed();
    log::info!(
        "Census place ingestion complete: {total} places in {:.1}s",
        elapsed.as_secs_f64()
    );

    Ok(())
}

/// Prompts for source filter and ingests neighborhood boundaries.
async fn ingest_neighborhoods(
    db: &dyn switchy_database::Database,
) -> Result<(), Box<dyn std::error::Error>> {
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
        match crime_map_neighborhood::ingest::ingest_source(db, &client, source).await {
            Ok(count) => total += count,
            Err(e) => {
                log::error!("Failed to ingest {}: {e}", source.id());
            }
        }
    }

    // Build the tract-to-neighborhood crosswalk
    if let Err(e) = crime_map_neighborhood::ingest::build_crosswalk(db).await {
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
