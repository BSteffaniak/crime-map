#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! CLI tool for ingesting crime data from public sources into the `PostGIS`
//! database.

use std::time::Instant;

use clap::{Parser, Subcommand};
use crime_map_database::{db, queries, run_migrations};
use crime_map_source::FetchOptions;
use crime_map_source::source_def::SourceDefinition;

/// Safety buffer subtracted from the latest `occurred_at` timestamp when
/// performing incremental syncs. This ensures we re-fetch a window of
/// recent data to catch any records that were backfilled or updated after
/// our previous sync. Duplicates are harmlessly skipped by the
/// `ON CONFLICT DO NOTHING` clause.
const INCREMENTAL_BUFFER_DAYS: i64 = 7;

#[derive(Parser)]
#[command(name = "crime_map_ingest", about = "Crime data ingestion tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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
    /// Geocode incidents that are missing coordinates using block addresses
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
        /// Re-geocode incidents that already have source-provided coordinates.
        /// Useful for sources with block-centroid geocoding where address-level
        /// precision would improve census place attribution.
        #[arg(long)]
        re_geocode: bool,
        /// Only geocode incidents from this source ID (e.g., `pg_county_md`).
        #[arg(long)]
        source: Option<String>,
    },
}

#[allow(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let cli = Cli::parse();

    match cli.command {
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
            sync_source(db.as_ref(), src, limit, force).await?;
        }
        Commands::SyncAll {
            limit,
            sources,
            force,
        } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;
            let sources = enabled_sources(sources);
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
                if let Err(e) = sync_source(db.as_ref(), src, limit, force).await {
                    log::error!("Failed to sync {}: {e}", src.id());
                }
            }
        }
        Commands::Tracts { states } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;

            let start = Instant::now();
            let total = if let Some(states_str) = states {
                let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
                log::info!("Ingesting census tracts for states: {states_str}");
                crime_map_geography::ingest::ingest_tracts_for_states(db.as_ref(), &fips_codes)
                    .await?
            } else {
                log::info!("Ingesting census tracts for all states...");
                crime_map_geography::ingest::ingest_all_tracts(db.as_ref()).await?
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
        Commands::Places { states } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;

            let start = Instant::now();
            let total = if let Some(states_str) = states {
                let fips_codes: Vec<&str> = states_str.split(',').map(str::trim).collect();
                log::info!("Ingesting Census places for states: {states_str}");
                crime_map_geography::ingest::ingest_places_for_states(db.as_ref(), &fips_codes)
                    .await?
            } else {
                log::info!("Ingesting Census places for all states...");
                crime_map_geography::ingest::ingest_all_places(db.as_ref()).await?
            };

            let elapsed = start.elapsed();
            log::info!(
                "Census place ingestion complete: {total} places in {:.1}s",
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
                let place_count =
                    queries::attribute_places(db.as_ref(), buffer, batch_size).await?;
                log::info!("Attributed {place_count} incidents to census places");
            }

            if !places_only {
                log::info!("Attributing incidents to census tracts (batch={batch_size})...");
                let tract_count = queries::attribute_tracts(db.as_ref(), batch_size).await?;
                log::info!("Attributed {tract_count} incidents to census tracts");
            }

            let elapsed = start.elapsed();
            log::info!("Attribution complete in {:.1}s", elapsed.as_secs_f64());
        }
        Commands::Geocode {
            limit,
            batch_size,
            nominatim_only,
            re_geocode,
            source,
        } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;

            // Resolve source name to database source_id if provided
            let source_id = if let Some(ref name) = source {
                let sid = queries::get_source_id_by_name(db.as_ref(), name).await?;
                log::info!("Filtering to source '{name}' (id={sid})");
                Some(sid)
            } else {
                None
            };

            let start = Instant::now();
            let geocoded = if re_geocode {
                re_geocode_source(db.as_ref(), batch_size, limit, nominatim_only, source_id).await?
            } else {
                geocode_missing(db.as_ref(), batch_size, limit, nominatim_only, source_id).await?
            };

            let elapsed = start.elapsed();
            log::info!(
                "Geocoding complete: {geocoded} incidents geocoded in {:.1}s",
                elapsed.as_secs_f64()
            );
        }
    }

    Ok(())
}

/// Returns all configured data sources from the TOML registry.
fn all_sources() -> Vec<SourceDefinition> {
    crime_map_source::registry::all_sources()
}

/// Returns the sources to sync, filtered by the `--sources` CLI flag or the
/// `CRIME_MAP_SOURCES` environment variable. If neither is set, all sources
/// are returned.
fn enabled_sources(cli_filter: Option<String>) -> Vec<SourceDefinition> {
    let filter = cli_filter.or_else(|| std::env::var("CRIME_MAP_SOURCES").ok());

    let all = all_sources();

    let Some(filter_str) = filter else {
        return all;
    };

    let ids: Vec<&str> = filter_str.split(',').map(str::trim).collect();

    let filtered: Vec<SourceDefinition> =
        all.into_iter().filter(|s| ids.contains(&s.id())).collect();

    if filtered.is_empty() {
        log::warn!(
            "No matching sources found for filter {:?}. Available: {}",
            ids,
            all_sources()
                .iter()
                .map(|s| s.id().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    filtered
}

/// Fetches, normalizes, and inserts data from a single source, processing
/// one page at a time to minimize memory usage and provide incremental
/// progress.
///
/// By default performs an incremental sync, fetching only records newer than
/// `MAX(occurred_at) - 7 days` for the source. Pass `force = true` to
/// ignore the previous sync point and fetch everything.
async fn sync_source(
    db: &dyn switchy_database::Database,
    source: &SourceDefinition,
    limit: Option<u64>,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    log::info!("Syncing source: {} ({})", source.name(), source.id());

    // Register/upsert the source in the database
    let source_id = queries::upsert_source(
        db,
        source.name(),
        "CITY_API",
        Option::None,
        &format!("{} data", source.name()),
    )
    .await?;

    // Determine the `since` timestamp for incremental syncing.
    //
    // Incremental mode only activates when:
    //   1. --force is NOT set
    //   2. The source has completed at least one full (non-limited) sync
    //   3. Records exist in the database for this source
    //
    // A source that was only partially synced (via --limit or a cancelled run)
    // will have fully_synced = false and will do a full fetch, but with a
    // resume_offset to skip already-ingested pages.
    let (since, resume_offset) = if force {
        log::info!("{}: full sync (--force)", source.name());
        (None, 0)
    } else {
        let fully_synced = queries::get_source_fully_synced(db, source_id).await?;
        let max_occurred = queries::get_source_max_occurred_at(db, source_id).await?;

        if fully_synced {
            let since = max_occurred.map_or_else(
                || {
                    log::info!("{}: full sync (no records found)", source.name());
                    None
                },
                |latest| {
                    let buffer = chrono::Duration::days(INCREMENTAL_BUFFER_DAYS);
                    let since = latest - buffer;
                    log::info!(
                        "{}: incremental sync from {} ({INCREMENTAL_BUFFER_DAYS}-day buffer from latest {})",
                        source.name(),
                        since.format("%Y-%m-%d"),
                        latest.format("%Y-%m-%d"),
                    );
                    Some(since)
                },
            );
            (since, 0)
        } else if max_occurred.is_some() {
            let record_count = queries::get_source_record_count(db, source_id).await?;
            if record_count > 0 {
                log::info!(
                    "{}: resuming full sync from offset {record_count} ({record_count} records already in DB)",
                    source.name(),
                );
            } else {
                log::info!("{}: full sync (first run)", source.name());
            }
            (None, record_count)
        } else {
            log::info!("{}: full sync (first run)", source.name());
            (None, 0)
        }
    };

    // Get category ID mapping (needed for insertion)
    let category_ids = queries::get_all_category_ids(db).await?;

    // Start streaming pages from the fetcher
    let options = FetchOptions {
        since,
        limit,
        resume_offset,
    };

    let (mut rx, fetch_handle) = source.fetch_pages(&options);

    let mut total_raw: u64 = 0;
    let mut total_normalized: u64 = 0;
    let mut total_inserted: u64 = 0;
    let page_size = source.page_size();
    let mut page_num: u64 = if page_size > 0 {
        resume_offset / page_size
    } else {
        0
    };

    // Process pages as they arrive
    while let Some(page) = rx.recv().await {
        page_num += 1;
        let raw_count = page.len() as u64;
        total_raw += raw_count;

        // Normalize this page
        let incidents = source.normalize_page(&page);
        let norm_count = incidents.len() as u64;
        total_normalized += norm_count;

        // Insert this page into the database
        let inserted = queries::insert_incidents(db, source_id, &incidents, &category_ids).await?;
        total_inserted += inserted;

        log::info!(
            "{}: page {page_num} — normalized {norm_count}/{raw_count}, inserted {inserted}",
            source.name(),
        );
    }

    // Wait for the fetcher task to finish and check for errors
    let fetch_result = fetch_handle.await?;
    if let Err(e) = fetch_result {
        return Err(format!("Fetch error for {}: {e}", source.name()).into());
    }

    // Update source stats
    queries::update_source_stats(db, source_id).await?;

    // Mark the source as fully synced only if we didn't cap with --limit.
    // A limited sync is intentionally partial (for testing), so we don't
    // want incremental mode to kick in on the next run.
    queries::set_source_fully_synced(db, source_id, limit.is_none()).await?;

    let elapsed = start.elapsed();
    log::info!(
        "Sync complete for {}: {} inserted ({} normalized from {} raw), took {:.1}s",
        source.name(),
        total_inserted,
        total_normalized,
        total_raw,
        elapsed.as_secs_f64()
    );

    Ok(())
}

/// Builds a SQL source filter clause and corresponding parameter list.
///
/// When a `source_id` is provided, returns `" AND source_id = $2"` with
/// the limit and source ID as parameters. Otherwise returns an empty clause
/// with just the limit parameter.
fn source_filter_params(
    limit_val: u64,
    source_id: Option<i32>,
) -> (&'static str, Vec<switchy_database::DatabaseValue>) {
    use switchy_database::DatabaseValue;

    source_id.map_or_else(
        || {
            (
                "",
                vec![DatabaseValue::Int64(
                    i64::try_from(limit_val).unwrap_or(i64::MAX),
                )],
            )
        },
        |sid| {
            (
                " AND source_id = $2",
                vec![
                    DatabaseValue::Int64(i64::try_from(limit_val).unwrap_or(i64::MAX)),
                    DatabaseValue::Int32(sid),
                ],
            )
        },
    )
}

/// Maximum number of parameters `PostgreSQL` allows per statement.
const PG_MAX_PARAMS: usize = 65_535;

/// Applies geocoded coordinates to incidents using batch `UPDATE … FROM
/// (VALUES …)` statements instead of individual row updates.
///
/// When `clear_attribution` is `true` (used by re-geocode), the census
/// place and tract GEOIDs are also cleared so the next `attribute` run
/// reassigns them based on the new coordinates.
///
/// Returns the number of rows updated.
async fn batch_update_geocoded(
    db: &dyn switchy_database::Database,
    updates: &[(i64, f64, f64)],
    clear_attribution: bool,
) -> Result<u64, Box<dyn std::error::Error>> {
    use std::fmt::Write as _;
    use switchy_database::DatabaseValue;

    if updates.is_empty() {
        return Ok(0);
    }

    let mut total_updated = 0u64;

    // Each row in the VALUES clause uses 3 parameters: (id, lng, lat).
    // If columns are added to the VALUES clause, update this constant so
    // the chunk size adjusts automatically.
    let params_per_row: usize = 3;
    let chunk_size = PG_MAX_PARAMS / params_per_row;

    for chunk in updates.chunks(chunk_size) {
        let set_clause = if clear_attribution {
            "SET location = ST_SetSRID(ST_MakePoint(d.lng, d.lat), 4326)::geography,
                 geocoded = TRUE,
                 census_place_geoid = NULL,
                 census_tract_geoid = NULL"
        } else {
            "SET location = ST_SetSRID(ST_MakePoint(d.lng, d.lat), 4326)::geography,
                 has_coordinates = TRUE,
                 geocoded = TRUE"
        };

        let mut sql = format!("UPDATE crime_incidents i {set_clause}\nFROM (VALUES ");
        let mut params: Vec<DatabaseValue> = Vec::with_capacity(chunk.len() * 3);
        let mut idx = 1u32;

        for (i, &(id, lng, lat)) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            write!(
                sql,
                "(${idx}::bigint, ${e1}::float8, ${e2}::float8)",
                e1 = idx + 1,
                e2 = idx + 2,
            )
            .unwrap();
            params.push(DatabaseValue::Int64(id));
            params.push(DatabaseValue::Real64(lng));
            params.push(DatabaseValue::Real64(lat));
            idx += 3;
        }

        sql.push_str(") AS d(id, lng, lat) WHERE i.id = d.id");

        let rows_affected = db.exec_raw_params(&sql, &params).await?;
        total_updated += rows_affected;
    }

    Ok(total_updated)
}

/// Geocodes incidents that have block addresses but no coordinates.
///
/// Fetches un-geocoded incidents from the database in batches, deduplicates
/// by address, runs them through the Census Bureau batch geocoder (and
/// Nominatim as fallback), then updates the incidents with the resolved
/// coordinates. Loops until all eligible incidents have been processed.
#[allow(clippy::too_many_lines)]
async fn geocode_missing(
    db: &dyn switchy_database::Database,
    batch_size: u64,
    limit: Option<u64>,
    nominatim_only: bool,
    source_id: Option<i32>,
) -> Result<u64, Box<dyn std::error::Error>> {
    use crime_map_geocoder::AddressInput;
    use crime_map_geocoder::address::{
        CleanedAddress, build_one_line_address, clean_block_address,
    };
    use moosicbox_json_utils::database::ToValue as _;
    use std::collections::BTreeMap;

    let (source_clause, _) = source_filter_params(batch_size, source_id);

    let client = reqwest::Client::builder()
        .user_agent("crime-map/1.0 (https://github.com/BSteffaniak/crime-map)")
        .build()?;

    let mut grand_total = 0u64;
    let mut batch_num = 0u64;

    loop {
        batch_num += 1;

        // Compute effective fetch size respecting the total limit
        let effective_size = limit.map_or(batch_size, |l| batch_size.min(l - grand_total));
        if effective_size == 0 {
            break;
        }

        // Fetch next batch of incidents missing coordinates
        let (_, base_params) = source_filter_params(effective_size, source_id);
        let query = format!(
            "SELECT id, block_address, city, state
             FROM crime_incidents
             WHERE has_coordinates = FALSE
               AND block_address IS NOT NULL
               AND block_address != ''
               AND geocoded = FALSE{source_clause}
             LIMIT $1"
        );
        let rows = db.query_raw_params(&query, &base_params).await?;

        if rows.is_empty() {
            if batch_num == 1 {
                log::info!("No un-geocoded incidents with addresses found");
            }
            break;
        }

        log::info!(
            "Batch {batch_num}: found {} incidents needing geocoding",
            rows.len()
        );

        // Deduplicate by (cleaned_address, city, state)
        let mut addr_groups: BTreeMap<(String, String, String), Vec<i64>> = BTreeMap::new();

        for row in &rows {
            let id: i64 = row.to_value("id").unwrap_or(0);
            let block: String = row.to_value("block_address").unwrap_or_default();
            let city: String = row.to_value("city").unwrap_or_default();
            let state: String = row.to_value("state").unwrap_or_default();

            let cleaned = clean_block_address(&block);
            let street = match cleaned {
                CleanedAddress::Street(s) => s,
                CleanedAddress::Intersection { street1, street2 } => {
                    format!("{street1} and {street2}")
                }
                CleanedAddress::NotGeocodable => continue,
            };

            addr_groups
                .entry((street, city, state))
                .or_default()
                .push(id);
        }

        log::info!(
            "Deduplicated to {} unique addresses from {} incidents",
            addr_groups.len(),
            rows.len()
        );

        let mut batch_geocoded = 0u64;
        let mut pending_updates: Vec<(i64, f64, f64)> = Vec::new();

        if !nominatim_only {
            // --- Census Bureau batch geocoding ---
            let inputs: Vec<(AddressInput, Vec<i64>)> = addr_groups
                .iter()
                .enumerate()
                .map(|(i, ((street, city, state), ids))| {
                    (
                        AddressInput {
                            id: i.to_string(),
                            street: street.clone(),
                            city: city.clone(),
                            state: state.clone(),
                            zip: None,
                        },
                        ids.clone(),
                    )
                })
                .collect();

            for chunk in inputs.chunks(crime_map_geocoder::census::MAX_BATCH_SIZE) {
                let batch_inputs: Vec<AddressInput> =
                    chunk.iter().map(|(input, _)| input.clone()).collect();

                log::info!(
                    "Sending batch of {} addresses to Census geocoder...",
                    batch_inputs.len()
                );

                match crime_map_geocoder::census::geocode_batch(&client, &batch_inputs).await {
                    Ok(result) => {
                        log::info!(
                            "Census batch: {} matched, {} unmatched",
                            result.matched.len(),
                            result.unmatched.len()
                        );

                        for (id_str, geocoded) in &result.matched {
                            let idx: usize = id_str.parse().unwrap_or(usize::MAX);
                            if let Some((_, incident_ids)) = chunk.get(idx) {
                                for &inc_id in incident_ids {
                                    pending_updates.push((
                                        inc_id,
                                        geocoded.longitude,
                                        geocoded.latitude,
                                    ));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Census batch geocoding failed: {e}");
                    }
                }
            }

            // Flush Census results
            if !pending_updates.is_empty() {
                log::info!(
                    "Writing {} geocoded incidents to database...",
                    pending_updates.len()
                );
                batch_geocoded += batch_update_geocoded(db, &pending_updates, false).await?;
                pending_updates.clear();
            }
        }

        // --- Nominatim fallback for remaining un-geocoded in this batch ---
        let (_, remaining_params) = source_filter_params(effective_size, source_id);
        let remaining_query = format!(
            "SELECT id, block_address, city, state
             FROM crime_incidents
             WHERE has_coordinates = FALSE
               AND block_address IS NOT NULL
               AND block_address != ''
               AND geocoded = FALSE{source_clause}
             LIMIT $1"
        );
        let remaining = db
            .query_raw_params(&remaining_query, &remaining_params)
            .await?;

        if !remaining.is_empty() {
            log::info!(
                "Attempting Nominatim fallback for {} remaining incidents...",
                remaining.len()
            );

            let mut nom_groups: BTreeMap<String, Vec<i64>> = BTreeMap::new();

            for row in &remaining {
                let id: i64 = row.to_value("id").unwrap_or(0);
                let block: String = row.to_value("block_address").unwrap_or_default();
                let city: String = row.to_value("city").unwrap_or_default();
                let state: String = row.to_value("state").unwrap_or_default();

                let cleaned = clean_block_address(&block);
                let query = match cleaned {
                    CleanedAddress::Street(s) => build_one_line_address(&s, &city, &state),
                    CleanedAddress::Intersection { street1, street2 } => {
                        format!("{street1} and {street2}, {city}, {state}")
                    }
                    CleanedAddress::NotGeocodable => continue,
                };

                nom_groups.entry(query).or_default().push(id);
            }

            for (query, incident_ids) in &nom_groups {
                tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

                match crime_map_geocoder::nominatim::geocode_freeform(&client, query).await {
                    Ok(Some(geocoded)) => {
                        for &inc_id in incident_ids {
                            pending_updates.push((inc_id, geocoded.longitude, geocoded.latitude));
                        }
                    }
                    Ok(None) => {
                        log::debug!("Nominatim: no match for '{query}'");
                    }
                    Err(e) => {
                        log::warn!("Nominatim error for '{query}': {e}");
                        if matches!(e, crime_map_geocoder::GeocodeError::RateLimited) {
                            log::warn!("Rate limited by Nominatim, waiting 60s...");
                            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                        }
                    }
                }
            }

            // Flush Nominatim results
            if !pending_updates.is_empty() {
                log::info!(
                    "Writing {} Nominatim-geocoded incidents to database...",
                    pending_updates.len()
                );
                batch_geocoded += batch_update_geocoded(db, &pending_updates, false).await?;
                pending_updates.clear();
            }
        }

        grand_total += batch_geocoded;
        log::info!(
            "Batch {batch_num} complete: {batch_geocoded} geocoded this batch, {grand_total} cumulative"
        );

        if limit.is_some_and(|l| grand_total >= l) {
            log::info!("Reached --limit of {}, stopping", limit.unwrap_or(0));
            break;
        }
    }

    Ok(grand_total)
}

/// Re-geocodes incidents that already have source-provided coordinates.
///
/// This is useful for sources like PG County where coordinates are block
/// centroids and address-level geocoding would produce more precise locations,
/// improving census place attribution for small municipalities.
///
/// Only re-geocodes incidents that have `geocoded = FALSE` (i.e., coordinates
/// came from the source, not from a previous geocoding run). Processes all
/// eligible incidents in batches.
#[allow(clippy::too_many_lines)]
async fn re_geocode_source(
    db: &dyn switchy_database::Database,
    batch_size: u64,
    limit: Option<u64>,
    nominatim_only: bool,
    source_id: Option<i32>,
) -> Result<u64, Box<dyn std::error::Error>> {
    use crime_map_geocoder::AddressInput;
    use crime_map_geocoder::address::{
        CleanedAddress, build_one_line_address, clean_block_address,
    };
    use moosicbox_json_utils::database::ToValue as _;
    use std::collections::BTreeMap;

    let (source_clause, _) = source_filter_params(batch_size, source_id);

    let client = reqwest::Client::builder()
        .user_agent("crime-map/1.0 (https://github.com/BSteffaniak/crime-map)")
        .build()?;

    let mut grand_total = 0u64;
    let mut batch_num = 0u64;

    loop {
        batch_num += 1;

        // Compute effective fetch size respecting the total limit
        let effective_size = limit.map_or(batch_size, |l| batch_size.min(l - grand_total));
        if effective_size == 0 {
            break;
        }

        // Fetch next batch of incidents with source coordinates
        let (_, params) = source_filter_params(effective_size, source_id);
        let query = format!(
            "SELECT id, block_address, city, state
             FROM crime_incidents
             WHERE has_coordinates = TRUE
               AND geocoded = FALSE
               AND block_address IS NOT NULL
               AND block_address != ''{source_clause}
             LIMIT $1"
        );

        let rows = db.query_raw_params(&query, &params).await?;

        if rows.is_empty() {
            if batch_num == 1 {
                log::info!("No incidents eligible for re-geocoding");
            }
            break;
        }

        log::info!(
            "Batch {batch_num}: found {} incidents eligible for re-geocoding",
            rows.len()
        );

        // Deduplicate by (cleaned_address, city, state)
        let mut addr_groups: BTreeMap<(String, String, String), Vec<i64>> = BTreeMap::new();

        for row in &rows {
            let id: i64 = row.to_value("id").unwrap_or(0);
            let block: String = row.to_value("block_address").unwrap_or_default();
            let city: String = row.to_value("city").unwrap_or_default();
            let state: String = row.to_value("state").unwrap_or_default();

            let cleaned = clean_block_address(&block);
            let street = match cleaned {
                CleanedAddress::Street(s) => s,
                CleanedAddress::Intersection { street1, street2 } => {
                    format!("{street1} and {street2}")
                }
                CleanedAddress::NotGeocodable => continue,
            };

            addr_groups
                .entry((street, city, state))
                .or_default()
                .push(id);
        }

        log::info!(
            "Deduplicated to {} unique addresses from {} incidents",
            addr_groups.len(),
            rows.len()
        );

        let mut batch_geocoded = 0u64;
        let mut pending_updates: Vec<(i64, f64, f64)> = Vec::new();

        if !nominatim_only {
            // --- Census Bureau batch geocoding ---
            let inputs: Vec<(AddressInput, Vec<i64>)> = addr_groups
                .iter()
                .enumerate()
                .map(|(i, ((street, city, state), ids))| {
                    (
                        AddressInput {
                            id: i.to_string(),
                            street: street.clone(),
                            city: city.clone(),
                            state: state.clone(),
                            zip: None,
                        },
                        ids.clone(),
                    )
                })
                .collect();

            for chunk in inputs.chunks(crime_map_geocoder::census::MAX_BATCH_SIZE) {
                let batch_inputs: Vec<AddressInput> =
                    chunk.iter().map(|(input, _)| input.clone()).collect();

                log::info!(
                    "Sending batch of {} addresses to Census geocoder for re-geocoding...",
                    batch_inputs.len()
                );

                match crime_map_geocoder::census::geocode_batch(&client, &batch_inputs).await {
                    Ok(result) => {
                        log::info!(
                            "Census batch: {} matched, {} unmatched",
                            result.matched.len(),
                            result.unmatched.len()
                        );

                        for (id_str, geocoded) in &result.matched {
                            let idx: usize = id_str.parse().unwrap_or(usize::MAX);
                            if let Some((_, incident_ids)) = chunk.get(idx) {
                                for &inc_id in incident_ids {
                                    pending_updates.push((
                                        inc_id,
                                        geocoded.longitude,
                                        geocoded.latitude,
                                    ));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Census batch geocoding failed: {e}");
                    }
                }
            }

            // Flush Census results (clear_attribution = true for re-geocode)
            if !pending_updates.is_empty() {
                log::info!(
                    "Writing {} re-geocoded incidents to database...",
                    pending_updates.len()
                );
                batch_geocoded += batch_update_geocoded(db, &pending_updates, true).await?;
                pending_updates.clear();
            }
        }

        // --- Nominatim fallback for remaining in this batch ---
        let (_, remaining_params) = source_filter_params(effective_size, source_id);
        let remaining_query = format!(
            "SELECT id, block_address, city, state
             FROM crime_incidents
             WHERE has_coordinates = TRUE
               AND geocoded = FALSE
               AND block_address IS NOT NULL
               AND block_address != ''{source_clause}
             LIMIT $1"
        );

        let remaining = db
            .query_raw_params(&remaining_query, &remaining_params)
            .await?;

        if !remaining.is_empty() {
            log::info!(
                "Attempting Nominatim fallback for {} remaining re-geocode targets...",
                remaining.len()
            );

            let mut nom_groups: BTreeMap<String, Vec<i64>> = BTreeMap::new();

            for row in &remaining {
                let id: i64 = row.to_value("id").unwrap_or(0);
                let block: String = row.to_value("block_address").unwrap_or_default();
                let city: String = row.to_value("city").unwrap_or_default();
                let state: String = row.to_value("state").unwrap_or_default();

                let cleaned = clean_block_address(&block);
                let query = match cleaned {
                    CleanedAddress::Street(s) => build_one_line_address(&s, &city, &state),
                    CleanedAddress::Intersection { street1, street2 } => {
                        format!("{street1} and {street2}, {city}, {state}")
                    }
                    CleanedAddress::NotGeocodable => continue,
                };

                nom_groups.entry(query).or_default().push(id);
            }

            for (query, incident_ids) in &nom_groups {
                tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

                match crime_map_geocoder::nominatim::geocode_freeform(&client, query).await {
                    Ok(Some(geocoded)) => {
                        for &inc_id in incident_ids {
                            pending_updates.push((inc_id, geocoded.longitude, geocoded.latitude));
                        }
                    }
                    Ok(None) => {
                        log::debug!("Nominatim: no match for '{query}'");
                    }
                    Err(e) => {
                        log::warn!("Nominatim error for '{query}': {e}");
                        if matches!(e, crime_map_geocoder::GeocodeError::RateLimited) {
                            log::warn!("Rate limited by Nominatim, waiting 60s...");
                            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                        }
                    }
                }
            }

            // Flush Nominatim results (clear_attribution = true for re-geocode)
            if !pending_updates.is_empty() {
                log::info!(
                    "Writing {} Nominatim re-geocoded incidents to database...",
                    pending_updates.len()
                );
                batch_geocoded += batch_update_geocoded(db, &pending_updates, true).await?;
                pending_updates.clear();
            }
        }

        grand_total += batch_geocoded;
        log::info!(
            "Batch {batch_num} complete: {batch_geocoded} geocoded this batch, {grand_total} cumulative"
        );

        if limit.is_some_and(|l| grand_total >= l) {
            log::info!("Reached --limit of {}, stopping", limit.unwrap_or(0));
            break;
        }
    }

    Ok(grand_total)
}
