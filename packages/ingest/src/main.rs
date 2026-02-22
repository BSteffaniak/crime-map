#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! CLI entry point for the crime data ingestion tool.

use std::time::Instant;

use clap::{Parser, Subcommand};
use crime_map_cli_utils::IndicatifProgress;
use crime_map_database::{db, queries, run_migrations};
use crime_map_ingest::{
    all_sources, enabled_sources, geocode_missing, re_geocode_source,
    resolve_re_geocode_source_ids, sync_source,
};
use crime_map_source::source_def::SourceDefinition;

#[derive(Parser)]
#[command(name = "crime_map_ingest", about = "Crime data ingestion tool")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Sync data from all configured sources
    SyncAll {
        /// Maximum number of records per source (for testing)
        #[arg(long)]
        limit: Option<u64>,
        /// Comma-separated list of source IDs to sync (overrides `CRIME_MAP_SOURCES` env var)
        #[arg(long)]
        sources: Option<String>,
        /// Force a full sync, ignoring any previously synced data
        #[arg(long)]
        force: bool,
    },
    /// Sync data from a specific source
    Sync {
        /// Source identifier (e.g., "`chicago_pd`")
        source: String,
        /// Maximum number of records to fetch
        #[arg(long)]
        limit: Option<u64>,
        /// Force a full sync, ignoring any previously synced data
        #[arg(long)]
        force: bool,
    },
    /// List all configured data sources
    Sources,
    /// Run database migrations
    Migrate,
    /// Ingest census tract boundaries from the Census Bureau `TIGERweb` API
    Tracts {
        /// Comma-separated list of state FIPS codes (e.g., "11" for DC, "06" for CA).
        /// If not specified, ingests tracts for all 50 states + DC.
        #[arg(long)]
        states: Option<String>,
        /// Force re-import even if tracts already exist for a state.
        #[arg(long)]
        force: bool,
    },
    /// Ingest neighborhood boundaries from city open data portals
    Neighborhoods {
        /// Comma-separated list of source IDs (e.g., `"dc_neighborhoods"`).
        /// If not specified, ingests from all configured sources.
        #[arg(long)]
        sources: Option<String>,
    },
    /// Ingest Census place boundaries (incorporated cities and CDPs) from `TIGERweb`
    Places {
        /// Comma-separated list of state FIPS codes (e.g., "24" for MD, "11" for DC).
        /// If not specified, ingests places for all 50 states + DC.
        #[arg(long)]
        states: Option<String>,
        /// Force re-import even if places already exist for a state.
        #[arg(long)]
        force: bool,
    },
    /// Ingest county boundaries from `TIGERweb`
    Counties {
        /// Comma-separated list of state FIPS codes (e.g., "06" for CA, "36" for NY).
        /// If not specified, ingests counties for all 50 states + DC.
        #[arg(long)]
        states: Option<String>,
        /// Force re-import even if counties already exist for a state.
        #[arg(long)]
        force: bool,
    },
    /// Ingest US state boundaries from `TIGERweb`
    States {
        /// Force re-import even if state boundaries already exist.
        #[arg(long)]
        force: bool,
    },
    /// Assign census place and tract GEOIDs to existing incidents via spatial lookup
    Attribute {
        /// Buffer distance in meters for place matching (handles minor
        /// coordinate rounding in source data). Default: 5 meters.
        /// Keep this small to avoid misattributing incidents to neighboring
        /// places in dense areas.
        #[arg(long, default_value = "5")]
        buffer: f64,
        /// Number of incidents to process per batch.
        #[arg(long, default_value = "5000")]
        batch_size: u32,
        /// Only attribute places (skip tracts).
        #[arg(long)]
        places_only: bool,
        /// Only attribute tracts (skip places).
        #[arg(long)]
        tracts_only: bool,
    },
    /// Geocode incidents that are missing coordinates using block addresses.
    /// Also automatically re-geocodes sources marked with `re_geocode = true`
    /// in their TOML config (e.g., sources with imprecise block-centroid
    /// coordinates).
    Geocode {
        /// Maximum total number of incidents to geocode. If not set, all
        /// eligible incidents are processed.
        #[arg(long)]
        limit: Option<u64>,
        /// Number of incidents to fetch per batch (default: 50,000).
        #[arg(long, default_value = "50000")]
        batch_size: u64,
        /// Skip Census Bureau batch geocoder and only use Nominatim.
        #[arg(long)]
        nominatim_only: bool,
        /// Only geocode incidents from this source (TOML id, e.g., `pg_county_md`).
        #[arg(long)]
        source: Option<String>,
    },
}

#[allow(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let multi = crime_map_cli_utils::init_logger();
    let cli = Cli::parse();

    let Some(command) = cli.command else {
        return crime_map_ingest::interactive::run(&multi).await;
    };

    match command {
        Commands::Migrate => {
            log::info!("Running database migrations...");
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;
            log::info!("Migrations complete.");
        }
        Commands::Sources => {
            let sources = all_sources();
            println!("{:<20} NAME", "ID");
            println!("{}", "-".repeat(50));
            for source in &sources {
                println!("{:<20} {}", source.id(), source.name());
            }
        }
        Commands::Sync {
            source,
            limit,
            force,
        } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;
            let sources = all_sources();
            let src = sources
                .iter()
                .find(|s| s.id() == source)
                .ok_or_else(|| format!("Unknown source: {source}"))?;

            let fetch_bar = IndicatifProgress::records_bar(&multi, src.name());
            let result = sync_source(db.as_ref(), src, limit, force, Some(fetch_bar.clone())).await;
            fetch_bar.finish_and_clear();
            result?;
        }
        Commands::SyncAll {
            limit,
            sources,
            force,
        } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;
            let sources = enabled_sources(sources);
            let num_sources = sources.len();
            log::info!(
                "Syncing {} source(s): {}",
                num_sources,
                sources
                    .iter()
                    .map(SourceDefinition::id)
                    .collect::<Vec<_>>()
                    .join(", ")
            );

            let source_bar = IndicatifProgress::steps_bar(&multi, "Sources", num_sources as u64);

            for (i, src) in sources.iter().enumerate() {
                let fetch_bar = IndicatifProgress::records_bar(&multi, src.name());
                source_bar.set_message(format!("Source {}/{num_sources}: {}", i + 1, src.name()));

                let result =
                    sync_source(db.as_ref(), src, limit, force, Some(fetch_bar.clone())).await;
                fetch_bar.finish_and_clear();

                if let Err(e) = result {
                    log::error!("Failed to sync {}: {e}", src.id());
                }

                source_bar.inc(1);
            }

            source_bar.finish(format!("Synced {num_sources} source(s)"));
        }
        Commands::Tracts { states, force } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;

            let start = Instant::now();
            let total = if let Some(states_str) = states {
                let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
                log::info!("Ingesting census tracts for states: {states_str}");
                crime_map_geography::ingest::ingest_tracts_for_states(
                    db.as_ref(),
                    &fips_codes,
                    force,
                )
                .await?
            } else {
                log::info!("Ingesting census tracts for all states...");
                crime_map_geography::ingest::ingest_all_tracts(db.as_ref(), force).await?
            };

            let elapsed = start.elapsed();
            log::info!(
                "Census tract ingestion complete: {total} tracts in {:.1}s",
                elapsed.as_secs_f64()
            );
        }
        Commands::Neighborhoods { sources } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;

            let all_sources = crime_map_neighborhood::registry::all_sources();
            let sources_to_ingest = if let Some(filter_str) = sources {
                let ids: Vec<&str> = filter_str.split(',').map(str::trim).collect();
                all_sources
                    .into_iter()
                    .filter(|s| ids.contains(&s.id()))
                    .collect::<Vec<_>>()
            } else {
                all_sources
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
                match crime_map_neighborhood::ingest::ingest_source(db.as_ref(), &client, source)
                    .await
                {
                    Ok(count) => total += count,
                    Err(e) => {
                        log::error!("Failed to ingest {}: {e}", source.id());
                    }
                }
            }

            // Build the tract-to-neighborhood crosswalk
            if let Err(e) = crime_map_neighborhood::ingest::build_crosswalk(db.as_ref()).await {
                log::error!("Failed to build crosswalk: {e}");
            }

            let elapsed = start.elapsed();
            log::info!(
                "Neighborhood ingestion complete: {total} neighborhoods in {:.1}s",
                elapsed.as_secs_f64()
            );
        }
        Commands::Places { states, force } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;

            let start = Instant::now();
            let total = if let Some(states_str) = states {
                let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
                log::info!("Ingesting Census places for states: {states_str}");
                crime_map_geography::ingest::ingest_places_for_states(
                    db.as_ref(),
                    &fips_codes,
                    force,
                )
                .await?
            } else {
                log::info!("Ingesting Census places for all states...");
                crime_map_geography::ingest::ingest_all_places(db.as_ref(), force).await?
            };

            let elapsed = start.elapsed();
            log::info!(
                "Census place ingestion complete: {total} places in {:.1}s",
                elapsed.as_secs_f64()
            );
        }
        Commands::Counties { states, force } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;

            let start = Instant::now();
            let total = if let Some(states_str) = states {
                let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
                log::info!("Ingesting county boundaries for states: {states_str}");
                crime_map_geography::ingest::ingest_counties_for_states(
                    db.as_ref(),
                    &fips_codes,
                    force,
                )
                .await?
            } else {
                log::info!("Ingesting county boundaries for all states...");
                crime_map_geography::ingest::ingest_all_counties(db.as_ref(), force).await?
            };

            let elapsed = start.elapsed();
            log::info!(
                "County boundary ingestion complete: {total} counties in {:.1}s",
                elapsed.as_secs_f64()
            );
        }
        Commands::States { force } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;

            let start = Instant::now();
            log::info!("Ingesting US state boundaries...");
            let total = crime_map_geography::ingest::ingest_all_states(db.as_ref(), force).await?;

            let elapsed = start.elapsed();
            log::info!(
                "State boundary ingestion complete: {total} states in {:.1}s",
                elapsed.as_secs_f64()
            );
        }
        Commands::Attribute {
            buffer,
            batch_size,
            places_only,
            tracts_only,
        } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;

            let start = Instant::now();

            if !tracts_only {
                log::info!(
                    "Attributing incidents to census places (buffer={buffer}m, batch={batch_size})..."
                );
                let places_bar = IndicatifProgress::batch_bar(&multi, "Place attribution");
                let place_count =
                    queries::attribute_places(db.as_ref(), buffer, batch_size, Some(places_bar))
                        .await?;
                log::info!("Attributed {place_count} incidents to census places");
            }

            if !places_only {
                log::info!("Attributing incidents to census tracts (batch={batch_size})...");
                let tracts_bar = IndicatifProgress::batch_bar(&multi, "Tract attribution");
                let tract_count =
                    queries::attribute_tracts(db.as_ref(), batch_size, Some(tracts_bar)).await?;
                log::info!("Attributed {tract_count} incidents to census tracts");
            }

            let elapsed = start.elapsed();
            log::info!("Attribution complete in {:.1}s", elapsed.as_secs_f64());
        }
        Commands::Geocode {
            limit,
            batch_size,
            nominatim_only,
            source,
        } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;

            // Resolve --source via TOML registry (deterministic id match)
            let source_id = if let Some(ref toml_id) = source {
                let src = all_sources()
                    .into_iter()
                    .find(|s| s.id() == toml_id)
                    .ok_or_else(|| format!("Unknown source: {toml_id}"))?;
                let sid = queries::get_source_id_by_name(db.as_ref(), src.name()).await?;
                log::info!("Filtering to source '{}' (db id={sid})", src.id());
                Some(sid)
            } else {
                None
            };

            let start = Instant::now();

            let geocode_bar = IndicatifProgress::batch_bar(&multi, "Geocoding");

            // Phase 1: Geocode incidents that have no coordinates
            let missing_count = geocode_missing(
                db.as_ref(),
                batch_size,
                limit,
                nominatim_only,
                source_id,
                Some(geocode_bar.clone()),
            )
            .await?;

            // Phase 2: Re-geocode sources with imprecise coords (re_geocode = true)
            let re_geocode_ids =
                resolve_re_geocode_source_ids(db.as_ref(), source.as_deref()).await?;

            let mut re_geocode_count = 0u64;
            if !re_geocode_ids.is_empty() {
                let remaining_limit = limit.map(|l| l.saturating_sub(missing_count));
                if remaining_limit.is_none_or(|l| l > 0) {
                    log::info!(
                        "Re-geocoding {} source(s) with imprecise coordinates...",
                        re_geocode_ids.len()
                    );
                    for sid in &re_geocode_ids {
                        let count = re_geocode_source(
                            db.as_ref(),
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

            geocode_bar.finish("Geocoding complete".to_string());

            let total = missing_count + re_geocode_count;
            let elapsed = start.elapsed();
            log::info!(
                "Geocoding complete: {total} incidents geocoded ({missing_count} missing-coord + {re_geocode_count} re-geocoded) in {:.1}s",
                elapsed.as_secs_f64()
            );
        }
    }

    Ok(())
}
