#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! CLI entry point for the crime data ingestion tool.

use std::time::Instant;

use clap::{Parser, Subcommand};
use crime_map_cli_utils::IndicatifProgress;
use crime_map_database::source_db;
use crime_map_ingest::{
    EnrichArgs, GeocodeArgs, SyncArgs, all_sources, enabled_sources, sync_source,
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
        /// Maximum wall-clock time (in minutes) to spend geocoding. When
        /// the limit is reached, geocoding stops gracefully after the
        /// current batch. Progress is preserved in the `DuckDB` files.
        #[arg(long)]
        max_time: Option<u64>,
    },
    /// Enrich incidents with spatial attribution data (census tract,
    /// place, county, state, neighborhood). Loads the boundaries spatial
    /// index and performs point-in-polygon lookups for each un-enriched
    /// incident. Results are stored in the source `DuckDB` so generation
    /// does not need to repeat the spatial lookups.
    Enrich {
        /// Comma-separated source IDs to enrich. If not specified, enriches
        /// all sources with local `DuckDB` files.
        #[arg(long)]
        sources: Option<String>,
        /// Force re-enrichment of all records (not just un-enriched ones).
        /// Use when boundaries have changed.
        #[arg(long)]
        force: bool,
    },
    /// Pull `DuckDB` files from Cloudflare R2 to the local `data/` directory
    Pull {
        /// Comma-separated source IDs to pull (if not specified, pulls all
        /// sources and shared files).
        #[arg(long)]
        sources: Option<String>,
        /// Only pull shared databases (boundaries, geocode cache), skip
        /// per-source files.
        #[arg(long)]
        shared_only: bool,
    },
    /// Push local `DuckDB` files to Cloudflare R2
    Push {
        /// Comma-separated source IDs to push (if not specified, pushes all
        /// sources and shared files).
        #[arg(long)]
        sources: Option<String>,
        /// Only push shared databases (boundaries, geocode cache), skip
        /// per-source files.
        #[arg(long)]
        shared_only: bool,
    },
    /// Pull a cached boundary partition from R2 into the local
    /// `boundaries.duckdb` (used by CI boundary ingestion jobs to reuse
    /// previously-ingested data and avoid redundant Census API calls).
    ///
    /// Downloads `boundaries-part/{name}.duckdb` from R2 into the local
    /// `data/shared/boundaries.duckdb` path. If the partition doesn't
    /// exist on R2 yet (first run), logs a warning and continues.
    PullBoundaryPart {
        /// Partition name (e.g., "states", "tracts-1", "neighborhoods").
        #[arg(long)]
        name: String,
    },
    /// Push the local `boundaries.duckdb` to R2 as a named partition
    /// (used by parallel CI boundary ingestion jobs).
    PushBoundaryPart {
        /// Partition name (e.g., "states", "tracts-1", "neighborhoods").
        /// The file is uploaded to `boundaries-part/{name}.duckdb` in R2.
        #[arg(long)]
        name: String,
    },
    /// Merge boundary partitions from R2 into a single `boundaries.duckdb`.
    ///
    /// Downloads all `boundaries-part/*.duckdb` files from R2, merges them
    /// into the local `boundaries.duckdb`, and pushes the merged result to
    /// R2. Partition files are kept on R2 as cache for future runs.
    MergeBoundaries,
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

/// Parses an optional comma-separated source CSV into a `Vec<String>`.
/// Returns an empty vec when `None` (meaning "all sources").
fn parse_source_csv(csv: Option<&str>) -> Vec<String> {
    csv.map(|s| {
        s.split(',')
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(String::from)
            .collect()
    })
    .unwrap_or_default()
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
            let sources = all_sources();
            let src = sources
                .iter()
                .find(|s| s.id() == source)
                .ok_or_else(|| format!("Unknown source: {source}"))?;

            let conn = source_db::open_by_id(src.id())?;
            let fetch_bar = IndicatifProgress::records_bar(&multi, src.name());
            let result = sync_source(&conn, src, limit, force, Some(fetch_bar.clone())).await;
            fetch_bar.finish_and_clear();
            result?;
        }
        Commands::SyncAll {
            limit,
            sources,
            states,
            force,
        } => {
            let source_ids: Vec<String> = if states.is_some() || sources.is_some() {
                resolve_source_filter(sources.as_deref(), states.as_deref())
                    .into_iter()
                    .map(|s| s.id().to_string())
                    .collect()
            } else {
                enabled_sources(None)
                    .into_iter()
                    .map(|s| s.id().to_string())
                    .collect()
            };

            let num_sources = source_ids.len();
            let source_bar = IndicatifProgress::steps_bar(&multi, "Sources", num_sources as u64);

            let args = SyncArgs {
                source_ids,
                limit,
                force,
            };

            let result = crime_map_ingest::run_sync(&args, Some(&source_bar)).await;
            source_bar.finish(format!("Synced {num_sources} source(s)"));

            if !result.failed.is_empty() {
                return Err(format!(
                    "{} source(s) failed to sync: {}",
                    result.failed.len(),
                    result.failed.join(", ")
                )
                .into());
            }
        }
        Commands::Tracts { states, force } => {
            let boundaries_conn = crime_map_database::boundaries_db::open_default()?;

            let start = Instant::now();
            let total = if let Some(states_str) = states {
                let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
                log::info!("Ingesting census tracts for states: {states_str}");
                crime_map_geography::ingest::ingest_tracts_for_states(
                    &boundaries_conn,
                    &fips_codes,
                    force,
                )
                .await?
            } else {
                log::info!("Ingesting census tracts for all states...");
                crime_map_geography::ingest::ingest_all_tracts(&boundaries_conn, force).await?
            };

            let elapsed = start.elapsed();
            log::info!(
                "Census tract ingestion complete: {total} tracts in {:.1}s",
                elapsed.as_secs_f64()
            );
        }
        Commands::Neighborhoods { sources, force } => {
            let boundaries_conn = crime_map_database::boundaries_db::open_default()?;

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
                    let existing: i64 = boundaries_conn.query_row(
                        "SELECT COUNT(*) FROM neighborhoods WHERE source_id = ?",
                        duckdb::params![source.id()],
                        |row| row.get(0),
                    )?;
                    if existing > 0 {
                        log::info!(
                            "{}: {existing} neighborhoods already exist, skipping \
                             (use --force to re-import)",
                            source.id()
                        );
                        continue;
                    }
                }

                match crime_map_neighborhood::ingest::ingest_source(
                    &boundaries_conn,
                    &client,
                    source,
                )
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
        }
        Commands::Places { states, force } => {
            let boundaries_conn = crime_map_database::boundaries_db::open_default()?;

            let start = Instant::now();
            let total = if let Some(states_str) = states {
                let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
                log::info!("Ingesting Census places for states: {states_str}");
                crime_map_geography::ingest::ingest_places_for_states(
                    &boundaries_conn,
                    &fips_codes,
                    force,
                )
                .await?
            } else {
                log::info!("Ingesting Census places for all states...");
                crime_map_geography::ingest::ingest_all_places(&boundaries_conn, force).await?
            };

            let elapsed = start.elapsed();
            log::info!(
                "Census place ingestion complete: {total} places in {:.1}s",
                elapsed.as_secs_f64()
            );
        }
        Commands::Counties { states, force } => {
            let boundaries_conn = crime_map_database::boundaries_db::open_default()?;

            let start = Instant::now();
            let total = if let Some(states_str) = states {
                let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
                log::info!("Ingesting county boundaries for states: {states_str}");
                crime_map_geography::ingest::ingest_counties_for_states(
                    &boundaries_conn,
                    &fips_codes,
                    force,
                )
                .await?
            } else {
                log::info!("Ingesting county boundaries for all states...");
                crime_map_geography::ingest::ingest_all_counties(&boundaries_conn, force).await?
            };

            let elapsed = start.elapsed();
            log::info!(
                "County boundary ingestion complete: {total} counties in {:.1}s",
                elapsed.as_secs_f64()
            );
        }
        Commands::States { force } => {
            let boundaries_conn = crime_map_database::boundaries_db::open_default()?;

            let start = Instant::now();
            log::info!("Ingesting US state boundaries...");
            let total =
                crime_map_geography::ingest::ingest_all_states(&boundaries_conn, force).await?;

            let elapsed = start.elapsed();
            log::info!(
                "State boundary ingestion complete: {total} states in {:.1}s",
                elapsed.as_secs_f64()
            );
        }
        Commands::Geocode {
            limit,
            batch_size,
            nominatim_only,
            sources,
            max_time,
        } => {
            let start = Instant::now();
            let geocode_bar = IndicatifProgress::batch_bar(&multi, "Geocoding");

            let args = GeocodeArgs {
                source_ids: parse_source_csv(sources.as_deref()),
                batch_size,
                limit,
                nominatim_only,
            };

            let geocode_future = crime_map_ingest::run_geocode(&args, Some(geocode_bar.clone()));

            let result = if let Some(minutes) = max_time {
                let duration = std::time::Duration::from_secs(minutes * 60);
                match tokio::time::timeout(duration, geocode_future).await {
                    Ok(inner) => inner?,
                    Err(_elapsed) => {
                        log::info!(
                            "Geocode time limit ({minutes}m) reached, stopping gracefully. \
                             Progress is preserved in DuckDB files."
                        );
                        geocode_bar.finish("Geocoding timed out (progress saved)".to_string());

                        let elapsed = start.elapsed();
                        log::info!("Geocoding stopped after {:.1}s", elapsed.as_secs_f64());
                        return Ok(());
                    }
                }
            } else {
                geocode_future.await?
            };

            geocode_bar.finish("Geocoding complete".to_string());

            let elapsed = start.elapsed();
            log::info!(
                "Geocoding complete: {} incidents geocoded ({} missing-coord + {} re-geocoded) in {:.1}s",
                result.total(),
                result.missing_geocoded,
                result.re_geocoded,
                elapsed.as_secs_f64()
            );
        }
        Commands::Enrich { sources, force } => {
            let start = Instant::now();
            let enrich_bar = IndicatifProgress::batch_bar(&multi, "Enriching");

            let args = EnrichArgs {
                source_ids: parse_source_csv(sources.as_deref()),
                force,
            };

            let result = crime_map_ingest::run_enrich(&args, Some(enrich_bar.clone()))?;
            enrich_bar.finish("Enrichment complete".to_string());

            let elapsed = start.elapsed();
            log::info!(
                "Enrichment complete: {} incidents enriched across {} source(s) in {:.1}s",
                result.enriched,
                result.sources_processed,
                elapsed.as_secs_f64()
            );
        }
        Commands::Pull {
            sources,
            shared_only,
        } => {
            let r2 = crime_map_r2::R2Client::from_env()?;
            let start = Instant::now();
            let mut total = 0u64;

            if !shared_only {
                let ids = parse_source_csv(sources.as_deref());
                total += r2.pull_sources(&ids).await?;
            }

            total += r2.pull_shared().await?;

            let elapsed = start.elapsed();
            log::info!(
                "Pull complete: {total} file(s) downloaded in {:.1}s",
                elapsed.as_secs_f64()
            );
        }
        Commands::Push {
            sources,
            shared_only,
        } => {
            let r2 = crime_map_r2::R2Client::from_env()?;
            let start = Instant::now();
            let mut total = 0u64;

            if !shared_only {
                let ids = parse_source_csv(sources.as_deref());
                total += r2.push_sources(&ids).await?;
            }

            total += r2.push_shared().await?;

            let elapsed = start.elapsed();
            log::info!(
                "Push complete: {total} file(s) uploaded in {:.1}s",
                elapsed.as_secs_f64()
            );
        }
        Commands::PullBoundaryPart { name } => {
            let r2 = crime_map_r2::R2Client::from_env()?;
            let local = crime_map_database::paths::boundaries_db_path();
            crime_map_database::paths::ensure_dir(
                local.parent().expect("boundaries path has parent"),
            )?;
            let key = format!("boundaries-part/{name}.duckdb");
            if r2.download(&key, &local).await? {
                log::info!("Pulled boundary partition '{name}' from R2");
            } else {
                log::info!(
                    "No cached boundary partition '{name}' on R2 (first run), starting fresh"
                );
            }
        }
        Commands::PushBoundaryPart { name } => {
            let r2 = crime_map_r2::R2Client::from_env()?;
            let local = crime_map_database::paths::boundaries_db_path();
            let key = format!("boundaries-part/{name}.duckdb");
            r2.upload(&key, &local).await?;
            log::info!("Pushed boundary partition '{name}' to R2");
        }
        Commands::MergeBoundaries => {
            let r2 = crime_map_r2::R2Client::from_env()?;
            let start = Instant::now();

            // List all boundary partition files in R2
            let keys = r2.list_keys("boundaries-part/").await?;
            if keys.is_empty() {
                log::warn!("No boundary partitions found in R2, nothing to merge");
                return Ok(());
            }
            log::info!("Found {} boundary partition(s) to merge", keys.len());

            // Download each partition to a temp directory
            let tmp_dir = std::env::temp_dir().join("boundary-parts");
            std::fs::create_dir_all(&tmp_dir)?;

            let mut local_parts: Vec<std::path::PathBuf> = Vec::new();
            for key in &keys {
                let filename = key.rsplit('/').next().unwrap_or(key);
                let local_path = tmp_dir.join(filename);
                r2.download(key, &local_path).await?;
                local_parts.push(local_path);
            }

            // Open (or create) the target boundaries DB and merge
            let target = crime_map_database::boundaries_db::open_default()?;
            let mut total_rows = 0u64;

            for part_path in &local_parts {
                log::info!("Merging {}", part_path.display());
                total_rows += crime_map_database::boundaries_db::merge_from(&target, part_path)?;
            }

            drop(target);

            // Push merged boundaries.duckdb to R2
            r2.push_shared().await?;

            // Partition files are intentionally kept on R2 as cache —
            // future boundary ingestion jobs pull their partition first
            // so the skip-if-exists logic avoids redundant Census API calls.

            // Clean up local temp files
            if let Err(e) = std::fs::remove_dir_all(&tmp_dir) {
                log::warn!("Failed to clean up temp dir: {e}");
            }

            let elapsed = start.elapsed();
            log::info!(
                "Merge complete: {total_rows} rows merged from {} partitions in {:.1}s",
                keys.len(),
                elapsed.as_secs_f64()
            );
        }
    }

    Ok(())
}
