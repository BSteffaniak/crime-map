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
        /// Comma-separated state FIPS codes to include (e.g., "24,11" for MD+DC).
        /// Sources whose `state` field matches will be included.
        /// Combined with `--sources` via union if both are provided.
        #[arg(long)]
        states: Option<String>,
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
        /// Force re-import even if neighborhoods already exist.
        #[arg(long)]
        force: bool,
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
        /// Comma-separated list of source IDs to attribute (e.g., "`chicago_pd,dc_mpd`").
        /// If not specified, attributes all unattributed incidents.
        #[arg(long)]
        sources: Option<String>,
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
        /// Comma-separated source IDs to geocode (TOML ids, e.g.,
        /// `"pg_county_md,dc_mpd"`). If not specified, geocodes all
        /// eligible incidents.
        #[arg(long)]
        sources: Option<String>,
    },
}

/// Resolves source IDs from `--sources` and/or `--states` flags to a
/// filtered list of `SourceDefinition`.
fn resolve_source_filter(sources: Option<&str>, states: Option<&str>) -> Vec<SourceDefinition> {
    let all = all_sources();

    if sources.is_none() && states.is_none() {
        return all;
    }

    let mut ids: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    if let Some(s) = sources {
        for id in s.split(',').map(str::trim) {
            if !id.is_empty() {
                ids.insert(id.to_string());
            }
        }
    }

    if let Some(st) = states {
        let fips_codes: Vec<&str> = st.split(',').map(str::trim).collect();
        let abbrs: Vec<String> = fips_codes
            .iter()
            .map(|f| crime_map_geography_models::fips::state_abbr(f).to_string())
            .collect();

        for source in &all {
            if abbrs.iter().any(|a| a.eq_ignore_ascii_case(&source.state)) {
                ids.insert(source.id().to_string());
            }
        }
    }

    let filtered: Vec<SourceDefinition> =
        all.into_iter().filter(|s| ids.contains(s.id())).collect();

    if filtered.is_empty() {
        log::warn!("No matching sources found for the given --sources / --states filters");
    }

    filtered
}

/// Resolves `--sources` CSV to database integer source IDs for use in
/// attribution and geocoding queries.
async fn resolve_source_db_ids(
    db: &dyn switchy_database::Database,
    sources: Option<&str>,
) -> Result<Vec<i32>, Box<dyn std::error::Error>> {
    let Some(sources_str) = sources else {
        return Ok(Vec::new());
    };

    let all = all_sources();
    let mut db_ids = Vec::new();

    for short_id in sources_str.split(',').map(str::trim) {
        if short_id.is_empty() {
            continue;
        }
        let Some(def) = all.iter().find(|s| s.id() == short_id) else {
            log::warn!("Unknown source ID '{short_id}'; skipping");
            continue;
        };
        let sid = queries::get_source_id_by_name(db, def.name()).await?;
        db_ids.push(sid);
    }

    Ok(db_ids)
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
            println!("{:<30} {:<6} NAME", "ID", "STATE");
            println!("{}", "-".repeat(70));
            for source in &sources {
                println!("{:<30} {:<6} {}", source.id(), source.state, source.name());
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
            states,
            force,
        } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;

            let sources = if states.is_some() || sources.is_some() {
                resolve_source_filter(sources.as_deref(), states.as_deref())
            } else {
                enabled_sources(None)
            };

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

            let mut failed_sources: Vec<String> = Vec::new();

            for (i, src) in sources.iter().enumerate() {
                let fetch_bar = IndicatifProgress::records_bar(&multi, src.name());
                source_bar.set_message(format!("Source {}/{num_sources}: {}", i + 1, src.name()));

                let result =
                    sync_source(db.as_ref(), src, limit, force, Some(fetch_bar.clone())).await;
                fetch_bar.finish_and_clear();

                if let Err(e) = result {
                    log::error!("Failed to sync {}: {e}", src.id());
                    failed_sources.push(src.id().to_string());
                }

                source_bar.inc(1);
            }

            source_bar.finish(format!("Synced {num_sources} source(s)"));

            if !failed_sources.is_empty() {
                return Err(format!(
                    "{} source(s) failed to sync: {}",
                    failed_sources.len(),
                    failed_sources.join(", ")
                )
                .into());
            }
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
        Commands::Neighborhoods { sources, force } => {
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
                // Skip sources that already have neighborhoods (unless --force)
                if !force {
                    let rows = db
                        .as_ref()
                        .query_raw_params(
                            "SELECT COUNT(*) as cnt FROM neighborhoods WHERE source_id = $1",
                            &[switchy_database::DatabaseValue::String(
                                source.id().to_string(),
                            )],
                        )
                        .await?;
                    let existing: i64 = rows.first().map_or(0, |r| {
                        moosicbox_json_utils::database::ToValue::to_value(r, "cnt").unwrap_or(0)
                    });
                    if existing > 0 {
                        log::info!(
                            "{}: {existing} neighborhoods already exist, skipping \
                             (use --force to re-import)",
                            source.id()
                        );
                        continue;
                    }
                }

                match crime_map_neighborhood::ingest::ingest_source(db.as_ref(), &client, source)
                    .await
                {
                    Ok(count) => total += count,
                    Err(e) => {
                        log::error!("Failed to ingest {}: {e}", source.id());
                    }
                }
            }

            // Build the tract-to-neighborhood crosswalk only if new data was ingested
            if total > 0 {
                if let Err(e) = crime_map_neighborhood::ingest::build_crosswalk(db.as_ref()).await {
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
            sources,
        } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;

            let source_ids = resolve_source_db_ids(db.as_ref(), sources.as_deref()).await?;
            let source_ids_ref = if source_ids.is_empty() {
                None
            } else {
                log::info!("Attributing only sources: {sources:?} (db ids: {source_ids:?})");
                Some(source_ids.as_slice())
            };

            let start = Instant::now();

            if !tracts_only {
                log::info!(
                    "Attributing incidents to census places (buffer={buffer}m, batch={batch_size})..."
                );
                let places_bar = IndicatifProgress::batch_bar(&multi, "Place attribution");
                let place_count = queries::attribute_places(
                    db.as_ref(),
                    buffer,
                    batch_size,
                    source_ids_ref,
                    Some(places_bar),
                )
                .await?;
                log::info!("Attributed {place_count} incidents to census places");
            }

            if !places_only {
                log::info!("Attributing incidents to census tracts (batch={batch_size})...");
                let tracts_bar = IndicatifProgress::batch_bar(&multi, "Tract attribution");
                let tract_count = queries::attribute_tracts(
                    db.as_ref(),
                    batch_size,
                    source_ids_ref,
                    Some(tracts_bar),
                )
                .await?;
                log::info!("Attributed {tract_count} incidents to census tracts");
            }

            let elapsed = start.elapsed();
            log::info!("Attribution complete in {:.1}s", elapsed.as_secs_f64());
        }
        Commands::Geocode {
            limit,
            batch_size,
            nominatim_only,
            sources,
        } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;

            // Resolve --sources via TOML registry
            let source_ids = resolve_source_db_ids(db.as_ref(), sources.as_deref()).await?;

            let start = Instant::now();
            let geocode_bar = IndicatifProgress::batch_bar(&multi, "Geocoding");

            let mut total_missing = 0u64;
            let mut total_re_geocoded = 0u64;

            if source_ids.is_empty() {
                // No filter: geocode everything
                total_missing = geocode_missing(
                    db.as_ref(),
                    batch_size,
                    limit,
                    nominatim_only,
                    None,
                    Some(geocode_bar.clone()),
                )
                .await?;

                let re_geocode_ids = resolve_re_geocode_source_ids(db.as_ref(), None).await?;
                let remaining_limit = limit.map(|l| l.saturating_sub(total_missing));
                if remaining_limit.is_none_or(|l| l > 0) && !re_geocode_ids.is_empty() {
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
                        total_re_geocoded += count;
                    }
                }
            } else {
                // Per-source geocoding
                for sid in &source_ids {
                    let missing = geocode_missing(
                        db.as_ref(),
                        batch_size,
                        limit,
                        nominatim_only,
                        Some(*sid),
                        Some(geocode_bar.clone()),
                    )
                    .await?;
                    total_missing += missing;
                }

                // Re-geocode for filtered sources
                let all_re_geocode = resolve_re_geocode_source_ids(db.as_ref(), None).await?;
                let filtered_re_geocode: Vec<i32> = all_re_geocode
                    .into_iter()
                    .filter(|id| source_ids.contains(id))
                    .collect();

                let remaining_limit = limit.map(|l| l.saturating_sub(total_missing));
                if remaining_limit.is_none_or(|l| l > 0) && !filtered_re_geocode.is_empty() {
                    for sid in &filtered_re_geocode {
                        let count = re_geocode_source(
                            db.as_ref(),
                            batch_size,
                            remaining_limit,
                            nominatim_only,
                            Some(*sid),
                            Some(geocode_bar.clone()),
                        )
                        .await?;
                        total_re_geocoded += count;
                    }
                }
            }

            geocode_bar.finish("Geocoding complete".to_string());

            let total = total_missing + total_re_geocoded;
            let elapsed = start.elapsed();
            log::info!(
                "Geocoding complete: {total} incidents geocoded ({total_missing} missing-coord + {total_re_geocoded} re-geocoded) in {:.1}s",
                elapsed.as_secs_f64()
            );
        }
    }

    Ok(())
}
