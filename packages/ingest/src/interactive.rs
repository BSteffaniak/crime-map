#![allow(clippy::module_name_repetitions)]

//! Interactive TUI for the crime data ingestion tool.
//!
//! Provides a menu-driven interface using `dialoguer` for running ingest
//! commands without memorizing CLI flags.

use std::time::Instant;

use crime_map_source::source_def::SourceDefinition;
use dialoguer::{Confirm, Input, Select};

/// Runs the interactive menu loop, prompting the user to select and
/// configure ingest operations.
///
/// # Errors
///
/// Returns an error if database connection, migrations, or any selected
/// operation fails.
#[allow(clippy::too_many_lines)]
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let db = crime_map_database::db::connect_from_env().await?;
    crime_map_database::run_migrations(db.as_ref()).await?;

    let choices = &[
        "Sync a specific source",
        "Sync all sources",
        "List sources",
        "Geocode missing coordinates",
        "Attribute census data",
        "Ingest census tracts",
        "Ingest census places",
        "Ingest neighborhoods",
        "Run database migrations",
    ];

    let selection = Select::new()
        .with_prompt("What would you like to do?")
        .items(choices)
        .default(0)
        .interact()?;

    match selection {
        0 => sync_specific_source(db.as_ref()).await?,
        1 => sync_all_sources(db.as_ref()).await?,
        2 => list_sources(),
        3 => geocode_missing_interactive(db.as_ref()).await?,
        4 => attribute_census_data(db.as_ref()).await?,
        5 => ingest_census_tracts(db.as_ref()).await?,
        6 => ingest_census_places(db.as_ref()).await?,
        7 => ingest_neighborhoods(db.as_ref()).await?,
        8 => {
            log::info!("Running database migrations...");
            crime_map_database::run_migrations(db.as_ref()).await?;
            log::info!("Migrations complete.");
        }
        _ => unreachable!(),
    }

    Ok(())
}

/// Prompts the user to select a source, then syncs it.
async fn sync_specific_source(
    db: &dyn switchy_database::Database,
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

    let idx = Select::new()
        .with_prompt("Select a source")
        .items(&labels)
        .default(0)
        .interact()?;

    let limit = prompt_optional_u64("Record limit (empty for no limit)")?;
    let force = Confirm::new()
        .with_prompt("Force full sync?")
        .default(false)
        .interact()?;

    crate::sync_source(db, &sources[idx], limit, force).await?;
    Ok(())
}

/// Prompts for optional filters and syncs all matching sources.
async fn sync_all_sources(
    db: &dyn switchy_database::Database,
) -> Result<(), Box<dyn std::error::Error>> {
    let filter_str: String = Input::new()
        .with_prompt("Comma-separated source IDs to filter (empty for all)")
        .allow_empty(true)
        .interact_text()?;

    let filter = if filter_str.trim().is_empty() {
        None
    } else {
        Some(filter_str)
    };

    let limit = prompt_optional_u64("Record limit per source (empty for no limit)")?;
    let force = Confirm::new()
        .with_prompt("Force full sync?")
        .default(false)
        .interact()?;

    let sources = crate::enabled_sources(filter);
    log::info!(
        "Syncing {} source(s): {}",
        sources.len(),
        sources
            .iter()
            .map(SourceDefinition::id)
            .collect::<Vec<_>>()
            .join(", ")
    );

    for src in &sources {
        if let Err(e) = crate::sync_source(db, src, limit, force).await {
            log::error!("Failed to sync {}: {e}", src.id());
        }
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

/// Prompts for geocoding parameters and runs the geocode pipeline.
#[allow(clippy::too_many_lines)]
async fn geocode_missing_interactive(
    db: &dyn switchy_database::Database,
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

    // Source filter
    let all_sources = crate::all_sources();
    let mut source_labels: Vec<String> = vec!["All sources".to_string()];
    source_labels.extend(
        all_sources
            .iter()
            .map(|s| format!("{} \u{2014} {}", s.id(), s.name())),
    );

    let source_idx = Select::new()
        .with_prompt("Filter to source")
        .items(&source_labels)
        .default(0)
        .interact()?;

    let source_id = if source_idx == 0 {
        None
    } else {
        let src = &all_sources[source_idx - 1];
        let sid = crime_map_database::queries::get_source_id_by_name(db, src.name()).await?;
        log::info!("Filtering to source '{}' (db id={sid})", src.id());
        Some(sid)
    };

    let source_toml_id = if source_idx == 0 {
        None
    } else {
        Some(all_sources[source_idx - 1].id().to_string())
    };

    let start = Instant::now();

    // Phase 1: Geocode incidents that have no coordinates
    let missing_count =
        crate::geocode_missing(db, batch_size, limit, nominatim_only, source_id).await?;

    // Phase 2: Re-geocode sources with imprecise coords
    let re_geocode_ids =
        crate::resolve_re_geocode_source_ids(db, source_toml_id.as_deref()).await?;

    let mut re_geocode_count = 0u64;
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
                )
                .await?;
                re_geocode_count += count;
            }
        }
    }

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
        let place_count =
            crime_map_database::queries::attribute_places(db, buffer, batch_size).await?;
        log::info!("Attributed {place_count} incidents to census places");
    }

    if !places_only {
        log::info!("Attributing incidents to census tracts (batch={batch_size})...");
        let tract_count = crime_map_database::queries::attribute_tracts(db, batch_size).await?;
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
        crime_map_geography::ingest::ingest_all_tracts(db).await?
    } else {
        let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
        log::info!("Ingesting census tracts for states: {states_str}");
        crime_map_geography::ingest::ingest_tracts_for_states(db, &fips_codes).await?
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
        crime_map_geography::ingest::ingest_all_places(db).await?
    } else {
        let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
        log::info!("Ingesting Census places for states: {states_str}");
        crime_map_geography::ingest::ingest_places_for_states(db, &fips_codes).await?
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
    let filter_str: String = Input::new()
        .with_prompt("Comma-separated source IDs (empty for all)")
        .allow_empty(true)
        .interact_text()?;

    let all_sources = crime_map_neighborhood::registry::all_sources();
    let sources_to_ingest = if filter_str.trim().is_empty() {
        all_sources
    } else {
        let ids: Vec<&str> = filter_str.split(',').map(str::trim).collect();
        all_sources
            .into_iter()
            .filter(|s| ids.contains(&s.id()))
            .collect::<Vec<_>>()
    };

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
