#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Library for ingesting crime data from public sources into per-source
//! `DuckDB` files.

pub mod interactive;

use std::sync::Arc;
use std::time::Instant;

use crime_map_database::{geocode_cache, source_db};
use crime_map_source::FetchOptions;
use crime_map_source::progress::ProgressCallback;
use crime_map_source::source_def::SourceDefinition;
use duckdb::Connection;

/// Safety buffer (in days) for incremental syncs.
///
/// Subtracted from the latest `occurred_at` timestamp to re-fetch a
/// window of recent data, catching records that were backfilled or
/// updated after our previous sync. Duplicates are harmlessly skipped
/// by the `ON CONFLICT DO NOTHING` clause.
pub const INCREMENTAL_BUFFER_DAYS: i64 = 7;

/// A cached geocoding result: `(address_key, provider, lat, lng, matched_address)`.
pub type CacheEntry = geocode_cache::CacheEntry;

/// An address group key and its associated incident IDs, paired with the
/// normalized cache key string.
pub type AddressGroup<'a> = (String, &'a (String, String, String), &'a Vec<String>);

// ── High-level orchestration args ────────────────────────────────

/// Arguments for [`run_ingest_boundaries`].
pub struct IngestBoundariesArgs {
    /// State FIPS codes to ingest boundaries for. Empty means all states.
    pub state_fips: Vec<String>,
    /// Force re-import even if boundaries already exist.
    pub force: bool,
}

/// Result of a [`run_ingest_boundaries`] call.
pub struct IngestBoundariesResult {
    /// Number of census tracts ingested.
    pub tracts: u64,
    /// Number of census places ingested.
    pub places: u64,
    /// Number of counties ingested.
    pub counties: u64,
    /// Number of states ingested.
    pub states: u64,
    /// Number of neighborhoods ingested.
    pub neighborhoods: u64,
}

/// Arguments for [`run_sync`].
pub struct SyncArgs {
    /// Source IDs to sync. Empty means all enabled sources.
    pub source_ids: Vec<String>,
    /// Maximum number of records per source (for testing).
    pub limit: Option<u64>,
    /// Force a full sync, ignoring any previously synced data.
    pub force: bool,
}

/// Arguments for [`run_geocode`].
pub struct GeocodeArgs {
    /// Source IDs to geocode. Empty means all sources.
    pub source_ids: Vec<String>,
    /// Number of incidents to fetch per batch.
    pub batch_size: u64,
    /// Maximum total incidents to geocode across all sources.
    pub limit: Option<u64>,
    /// Skip Census Bureau batch geocoder and only use Nominatim.
    pub nominatim_only: bool,
}

/// Arguments for [`run_enrich`].
pub struct EnrichArgs {
    /// Source IDs to enrich. Empty means all sources with local `DuckDB` files.
    pub source_ids: Vec<String>,
    /// Force re-enrichment of all records (not just un-enriched ones).
    pub force: bool,
}

/// Result of a [`run_sync`] call.
pub struct SyncResult {
    /// Number of sources that synced successfully.
    pub succeeded: u64,
    /// Source IDs that failed to sync.
    pub failed: Vec<String>,
}

/// Result of a [`run_geocode`] call.
pub struct GeocodeResult {
    /// Number of incidents geocoded (missing coordinates).
    pub missing_geocoded: u64,
    /// Number of incidents re-geocoded (imprecise coordinates).
    pub re_geocoded: u64,
}

impl GeocodeResult {
    /// Total incidents processed.
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.missing_geocoded + self.re_geocoded
    }
}

/// Result of a [`run_enrich`] call.
pub struct EnrichResult {
    /// Total incidents enriched across all sources.
    pub enriched: u64,
    /// Number of sources processed.
    pub sources_processed: u64,
}

// ── High-level orchestration functions ───────────────────────────

/// Syncs data from the specified sources (or all enabled sources if
/// `args.source_ids` is empty).
///
/// Opens each source's `DuckDB` file, calls [`sync_source`], and
/// collects results. Returns a [`SyncResult`] with the list of any
/// sources that failed so the caller can decide how to handle them.
///
/// # Errors
///
/// Only returns `Err` for fatal/unrecoverable errors (none currently).
/// Per-source failures are captured in [`SyncResult::failed`].
#[allow(clippy::future_not_send)]
pub async fn run_sync(args: &SyncArgs, progress: Option<&Arc<dyn ProgressCallback>>) -> SyncResult {
    let sources = resolve_source_defs(&args.source_ids);

    log::info!(
        "Syncing {} source(s): {}",
        sources.len(),
        sources
            .iter()
            .map(SourceDefinition::id)
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut result = SyncResult {
        succeeded: 0,
        failed: Vec::new(),
    };

    for (i, src) in sources.iter().enumerate() {
        if let Some(p) = progress {
            p.set_message(format!(
                "Source {}/{}: {}",
                i + 1,
                sources.len(),
                src.name()
            ));
        }

        match source_db::open_by_id(src.id()) {
            Ok(conn) => {
                if let Err(e) = sync_source(&conn, src, args.limit, args.force, None).await {
                    log::error!("Failed to sync {}: {e}", src.id());
                    result.failed.push(src.id().to_string());
                } else {
                    result.succeeded += 1;
                }
            }
            Err(e) => {
                log::error!("Failed to open DB for {}: {e}", src.id());
                result.failed.push(src.id().to_string());
            }
        }

        if let Some(p) = progress {
            p.inc(1);
        }
    }

    result
}

/// Runs the two-phase geocode pipeline: first geocodes incidents missing
/// coordinates, then re-geocodes sources with imprecise block-centroid
/// coordinates.
///
/// # Errors
///
/// Returns an error if database connections, geocoding, or batch updates
/// fail.
#[allow(clippy::needless_pass_by_value, clippy::future_not_send)]
pub async fn run_geocode(
    args: &GeocodeArgs,
    progress: Option<Arc<dyn ProgressCallback>>,
) -> Result<GeocodeResult, Box<dyn std::error::Error>> {
    let all_defs = all_sources();
    let target_sources: Vec<&SourceDefinition> = if args.source_ids.is_empty() {
        all_defs.iter().collect()
    } else {
        all_defs
            .iter()
            .filter(|s| args.source_ids.contains(&s.id().to_string()))
            .collect()
    };

    let cache_conn = geocode_cache::open_default()?;

    let mut missing_geocoded = 0u64;

    // Phase 1: Geocode incidents missing coordinates
    for src in &target_sources {
        let source_conn = source_db::open_by_id(src.id())?;
        let count = geocode_missing(
            &source_conn,
            &cache_conn,
            args.batch_size,
            args.limit,
            args.nominatim_only,
            progress.clone(),
        )
        .await?;
        missing_geocoded += count;

        if args.limit.is_some_and(|l| missing_geocoded >= l) {
            break;
        }
    }

    // Phase 2: Re-geocode sources with imprecise coords
    let mut re_geocoded = 0u64;
    let remaining_limit = args.limit.map(|l| l.saturating_sub(missing_geocoded));
    if remaining_limit.is_none_or(|l| l > 0) {
        let re_geocode_sources: Vec<&&SourceDefinition> =
            target_sources.iter().filter(|s| s.re_geocode()).collect();

        if !re_geocode_sources.is_empty() {
            log::info!(
                "Re-geocoding {} source(s) with imprecise coordinates...",
                re_geocode_sources.len()
            );
            for src in re_geocode_sources {
                let source_conn = source_db::open_by_id(src.id())?;
                let count = re_geocode_source(
                    &source_conn,
                    &cache_conn,
                    args.batch_size,
                    remaining_limit,
                    args.nominatim_only,
                    progress.clone(),
                )
                .await?;
                re_geocoded += count;
            }
        }
    }

    Ok(GeocodeResult {
        missing_geocoded,
        re_geocoded,
    })
}

/// Batch size for spatial enrichment (rows per UPDATE round-trip).
const ENRICH_BATCH_SIZE: i64 = 50_000;

/// Enriches source `DuckDB` incidents with spatial attribution data.
///
/// For each source, queries un-enriched incidents (or all incidents if
/// `args.force` is `true`), performs point-in-polygon lookups against
/// the boundaries `SpatialIndex`, and writes the results
/// (`census_tract_geoid`, `census_place_geoid`, `state_fips`,
/// `county_geoid`, `neighborhood_id`) back to the source `DuckDB`.
///
/// Records that fall outside all known census tracts / places are still
/// marked `enriched = TRUE` with `NULL` geo fields so they are not
/// re-processed on subsequent runs.
///
/// # Errors
///
/// Returns an error if the boundaries database cannot be opened, the
/// spatial index fails to load, or any source database operation fails.
#[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
pub fn run_enrich(
    args: &EnrichArgs,
    progress: Option<Arc<dyn ProgressCallback>>,
) -> Result<EnrichResult, Box<dyn std::error::Error>> {
    use crime_map_spatial::SpatialIndex;

    // Determine which source IDs to process
    let target_ids: Vec<String> = if args.source_ids.is_empty() {
        source_db::discover_source_ids()
    } else {
        args.source_ids.clone()
    };

    if target_ids.is_empty() {
        log::info!("No source DuckDB files found to enrich");
        return Ok(EnrichResult {
            enriched: 0,
            sources_processed: 0,
        });
    }

    // Load spatial index from boundaries DB
    log::info!("Loading spatial index from boundaries database...");
    let boundaries_conn = crime_map_database::boundaries_db::open_default()?;
    let geo_index = SpatialIndex::load(&boundaries_conn)?;
    drop(boundaries_conn);

    let mut total_enriched = 0u64;
    let mut sources_processed = 0u64;

    for sid in &target_ids {
        let source_conn = match source_db::open_by_id(sid) {
            Ok(c) => c,
            Err(e) => {
                log::warn!("Skipping source '{sid}': {e}");
                continue;
            }
        };

        let filter = if args.force {
            "WHERE has_coordinates = TRUE \
                AND longitude BETWEEN -180 AND 180 \
                AND latitude BETWEEN -90 AND 90"
        } else {
            "WHERE has_coordinates = TRUE \
                AND enriched = FALSE \
                AND longitude BETWEEN -180 AND 180 \
                AND latitude BETWEEN -90 AND 90"
        };

        // Count eligible rows for progress
        let count_sql = format!("SELECT COUNT(*) FROM incidents {filter}");
        let mut count_stmt = source_conn.prepare(&count_sql)?;
        let eligible: i64 = count_stmt.query_row([], |row| row.get(0))?;

        if eligible == 0 {
            log::info!("{sid}: no un-enriched records, skipping");
            continue;
        }

        #[allow(clippy::cast_sign_loss)]
        {
            log::info!("{sid}: enriching {eligible} record(s)");
            if let Some(ref p) = progress {
                p.set_total(eligible as u64);
            }
        }

        // Keyset pagination using source_incident_id ordering
        let query_sql = format!(
            "SELECT source_incident_id, longitude, latitude \
             FROM incidents {filter} \
                AND source_incident_id > ? \
             ORDER BY source_incident_id ASC \
             LIMIT ?"
        );

        let mut last_id = String::new();
        let mut source_enriched = 0u64;

        loop {
            let mut stmt = source_conn.prepare(&query_sql)?;
            let mut rows = stmt.query(duckdb::params![&last_id, ENRICH_BATCH_SIZE])?;

            let mut batch: Vec<source_db::AttributionUpdate> = Vec::new();
            while let Some(row) = rows.next()? {
                let incident_id: String = row.get(0)?;
                let lng: f64 = row.get(1)?;
                let lat: f64 = row.get(2)?;

                let tract_geoid = geo_index.lookup_tract(lng, lat).map(str::to_owned);
                let place_geoid = geo_index.lookup_place(lng, lat).map(str::to_owned);
                let state_fips = tract_geoid
                    .as_deref()
                    .and_then(SpatialIndex::derive_state_fips)
                    .map(str::to_owned);
                let county_geoid = tract_geoid
                    .as_deref()
                    .and_then(SpatialIndex::derive_county_geoid)
                    .map(str::to_owned);
                let neighborhood_id = tract_geoid
                    .as_deref()
                    .and_then(|g| geo_index.lookup_neighborhood(g))
                    .map(str::to_owned);

                last_id.clone_from(&incident_id);

                batch.push(source_db::AttributionUpdate {
                    source_incident_id: incident_id,
                    census_tract_geoid: tract_geoid,
                    census_place_geoid: place_geoid,
                    state_fips,
                    county_geoid,
                    neighborhood_id,
                });
            }

            if batch.is_empty() {
                break;
            }

            #[allow(clippy::cast_possible_truncation)]
            let batch_len = batch.len() as u64;
            source_db::batch_update_attribution(&source_conn, &batch)?;
            source_enriched += batch_len;

            if let Some(ref p) = progress {
                p.inc(batch_len);
            }

            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            if (batch_len as i64) < ENRICH_BATCH_SIZE {
                break;
            }
        }

        log::info!("{sid}: enriched {source_enriched} record(s)");
        total_enriched += source_enriched;
        sources_processed += 1;
    }

    if let Some(ref p) = progress {
        p.finish(format!("Enriched {total_enriched} record(s)"));
    }

    Ok(EnrichResult {
        enriched: total_enriched,
        sources_processed,
    })
}

/// Ingests census boundaries (tracts, places, counties, states) and
/// neighborhoods into the shared `boundaries.duckdb`.
///
/// Each boundary type has fast skip logic: a single `COUNT(*)` query per
/// state checks whether data already exists, skipping states that are
/// already populated (no network I/O). Pass `force = true` to re-fetch
/// everything.
///
/// If `args.state_fips` is empty, ingests all 51 states + DC.
///
/// # Errors
///
/// Returns an error if database connections or any ingestion step fails.
#[allow(clippy::too_many_lines, clippy::future_not_send)]
pub async fn run_ingest_boundaries(
    args: &IngestBoundariesArgs,
) -> Result<IngestBoundariesResult, Box<dyn std::error::Error>> {
    let boundaries_conn = crime_map_database::boundaries_db::open_default()?;

    let fips_refs: Vec<&str> = args.state_fips.iter().map(String::as_str).collect();
    let has_filter = !fips_refs.is_empty();

    // --- Tracts ---
    let tracts = if has_filter {
        log::info!(
            "Ingesting census tracts for states: {}",
            fips_refs.join(",")
        );
        crime_map_geography::ingest::ingest_tracts_for_states(
            &boundaries_conn,
            &fips_refs,
            args.force,
        )
        .await?
    } else {
        log::info!("Ingesting census tracts for all states...");
        crime_map_geography::ingest::ingest_all_tracts(&boundaries_conn, args.force).await?
    };
    log::info!("Census tracts: {tracts} ingested");

    // --- Places ---
    let places = if has_filter {
        log::info!(
            "Ingesting census places for states: {}",
            fips_refs.join(",")
        );
        crime_map_geography::ingest::ingest_places_for_states(
            &boundaries_conn,
            &fips_refs,
            args.force,
        )
        .await?
    } else {
        log::info!("Ingesting census places for all states...");
        crime_map_geography::ingest::ingest_all_places(&boundaries_conn, args.force).await?
    };
    log::info!("Census places: {places} ingested");

    // --- Counties ---
    let counties = if has_filter {
        log::info!(
            "Ingesting county boundaries for states: {}",
            fips_refs.join(",")
        );
        crime_map_geography::ingest::ingest_counties_for_states(
            &boundaries_conn,
            &fips_refs,
            args.force,
        )
        .await?
    } else {
        log::info!("Ingesting county boundaries for all states...");
        crime_map_geography::ingest::ingest_all_counties(&boundaries_conn, args.force).await?
    };
    log::info!("Counties: {counties} ingested");

    // --- States (always all — there's no per-state filter for state boundaries) ---
    log::info!("Ingesting US state boundaries...");
    let states =
        crime_map_geography::ingest::ingest_all_states(&boundaries_conn, args.force).await?;
    log::info!("States: {states} ingested");

    // --- Neighborhoods ---
    let all_nbhd_sources = crime_map_neighborhood::registry::all_sources();
    let mut neighborhoods = 0u64;

    if !all_nbhd_sources.is_empty() {
        let client = reqwest::Client::builder()
            .user_agent("crime-map/1.0")
            .build()?;

        let mut new_ingested = false;
        for source in &all_nbhd_sources {
            // Skip sources that already have neighborhoods (unless force)
            if !args.force {
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
            }

            match crime_map_neighborhood::ingest::ingest_source(&boundaries_conn, &client, source)
                .await
            {
                Ok(count) => {
                    neighborhoods += count;
                    if count > 0 {
                        new_ingested = true;
                    }
                }
                Err(e) => {
                    log::error!("Failed to ingest neighborhoods from {}: {e}", source.id());
                }
            }
        }

        // Build crosswalk only if new data was ingested
        if new_ingested
            && let Err(e) = crime_map_neighborhood::ingest::build_crosswalk(&boundaries_conn)
        {
            log::error!("Failed to build tract-neighborhood crosswalk: {e}");
        }
    }
    log::info!("Neighborhoods: {neighborhoods} ingested");

    Ok(IngestBoundariesResult {
        tracts,
        places,
        counties,
        states,
        neighborhoods,
    })
}

/// Returns the number of census tracts in `boundaries.duckdb` that have
/// geometry data. A zero return means boundary counts and fills will be
/// empty after generation.
///
/// # Errors
///
/// Returns an error if the database connection or query fails.
pub fn boundary_tract_count() -> Result<u64, Box<dyn std::error::Error>> {
    let conn = crime_map_database::boundaries_db::open_default()?;
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM census_tracts WHERE boundary_geojson IS NOT NULL",
        [],
        |row| row.get(0),
    )?;
    #[allow(clippy::cast_sign_loss)]
    Ok(count as u64)
}

/// Resolves source IDs to definitions. If `source_ids` is empty, returns
/// all enabled sources (respecting `CRIME_MAP_SOURCES` env var).
fn resolve_source_defs(source_ids: &[String]) -> Vec<SourceDefinition> {
    if source_ids.is_empty() {
        enabled_sources(None)
    } else {
        let all = all_sources();
        all.into_iter()
            .filter(|s| source_ids.contains(&s.id().to_string()))
            .collect()
    }
}

/// Returns all configured data sources from the TOML registry.
#[must_use]
pub fn all_sources() -> Vec<SourceDefinition> {
    crime_map_source::registry::all_sources()
}

/// Returns the sources to sync, filtered by the `--sources` CLI flag or the
/// `CRIME_MAP_SOURCES` environment variable. If neither is set, all sources
/// are returned.
#[must_use]
pub fn enabled_sources(cli_filter: Option<String>) -> Vec<SourceDefinition> {
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
/// The `conn` parameter is the per-source `DuckDB` connection (already
/// opened via `source_db::open_by_id`).
///
/// By default performs an incremental sync, fetching only records newer than
/// `MAX(occurred_at) - 7 days` for the source. Pass `force = true` to
/// ignore the previous sync point and fetch everything.
///
/// # Errors
///
/// Returns an error if database queries, source fetching, or page
/// normalization/insertion fails.
#[allow(clippy::too_many_lines, clippy::future_not_send)]
pub async fn sync_source(
    conn: &Connection,
    source: &SourceDefinition,
    limit: Option<u64>,
    force: bool,
    progress: Option<Arc<dyn ProgressCallback>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    log::info!("Syncing source: {} ({})", source.name(), source.id());

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
        let fully_synced = source_db::get_fully_synced(conn)?;
        let max_occurred = source_db::get_max_occurred_at(conn)?;

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
        } else {
            // Not fully synced yet — check if we can resume from a
            // previous partial run by counting existing records.
            let record_count = source_db::get_record_count(conn)?;
            if record_count > 0 {
                log::info!(
                    "{}: resuming full sync from offset {record_count} ({record_count} records already in DB)",
                    source.name(),
                );
            } else {
                log::info!("{}: full sync (first run)", source.name());
            }
            (None, record_count)
        }
    };

    // Start streaming pages from the fetcher
    let options = FetchOptions {
        since,
        limit,
        resume_offset,
    };

    let fetch_progress = progress.unwrap_or_else(crime_map_source::progress::null_progress);
    let (mut rx, fetch_handle) = source.fetch_pages(&options, fetch_progress);

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

        // Insert this page into the per-source DuckDB
        let inserted = source_db::insert_incidents(conn, &incidents)?;
        total_inserted += inserted;

        log::info!(
            "{}: page {page_num} — normalized {norm_count}/{raw_count}, inserted {inserted}",
            source.name(),
        );
    }

    // Wait for the fetcher task to finish and check for errors
    let fetch_result = fetch_handle.await?;
    if let Err(e) = fetch_result {
        // Save progress so the next run can resume from where we left off
        // (don't mark as fully_synced since we didn't finish).
        if let Err(meta_err) = source_db::update_sync_metadata(conn, source.name()) {
            log::warn!("Failed to save sync metadata after fetch error: {meta_err}");
        }
        return Err(format!("Fetch error for {}: {e}", source.name()).into());
    }

    // Update source metadata
    source_db::update_sync_metadata(conn, source.name())?;

    // Mark the source as fully synced only if we didn't cap with --limit.
    // A limited sync is intentionally partial (for testing), so we don't
    // want incremental mode to kick in on the next run.
    source_db::set_fully_synced(conn, limit.is_none())?;

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

/// Resolves addresses through the geocoding pipeline: cache → Census → Nominatim.
///
/// For each unique address in `addr_groups`:
/// 1. Check the geocode cache for existing results (hits or known misses)
/// 2. Iterate enabled geocoding services (from the TOML registry) in
///    priority order, sending unresolved addresses to each provider
/// 3. Write all results (hits and misses) to cache
///
/// Returns `(updates, all_incident_ids)` where `updates` are
/// `(source_incident_id, lng, lat)` tuples for successfully geocoded
/// incidents, and `all_incident_ids` is every incident ID that was
/// processed (for marking as attempted).
///
/// # Errors
///
/// Returns an error if cache lookups, geocoder requests, or cache writes fail.
#[allow(
    clippy::too_many_lines,
    clippy::type_complexity,
    clippy::future_not_send
)]
pub async fn resolve_addresses(
    cache_conn: &Connection,
    client: &reqwest::Client,
    addr_groups: &std::collections::BTreeMap<(String, String, String), Vec<String>>,
    nominatim_only: bool,
    progress: &Option<Arc<dyn ProgressCallback>>,
) -> Result<(Vec<(String, f64, f64)>, Vec<String>), Box<dyn std::error::Error>> {
    use crime_map_geocoder::address::build_one_line_address;
    use crime_map_geocoder::service_registry::{ProviderConfig, enabled_services};

    let mut pending_updates: Vec<(String, f64, f64)> = Vec::new();
    let mut all_ids: Vec<String> = Vec::new();
    let cache_writes: Vec<CacheEntry> = Vec::new();

    // Collect all incident IDs
    for ids in addr_groups.values() {
        all_ids.extend_from_slice(ids);
    }

    // Build address keys for cache lookup
    let keys_and_groups: Vec<AddressGroup<'_>> = addr_groups
        .iter()
        .map(|(key, ids)| {
            let address_key = build_one_line_address(&key.0, &key.1, &key.2);
            (address_key, key, ids)
        })
        .collect();

    let all_keys: Vec<String> = keys_and_groups.iter().map(|(k, _, _)| k.clone()).collect();

    // --- Phase 0: Cache lookup ---
    let (cache_hits, cache_tried) = geocode_cache::cache_lookup(cache_conn, &all_keys)?;

    let mut resolved_keys: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    // Apply cache hits
    let mut cache_resolved_incidents = 0u64;
    for (address_key, _, ids) in &keys_and_groups {
        if let Some(&(lat, lng)) = cache_hits.get(address_key) {
            for id in *ids {
                pending_updates.push((id.clone(), lng, lat));
            }
            cache_resolved_incidents += ids.len() as u64;
            resolved_keys.insert(address_key.clone());
        }
    }

    // Also count incidents whose addresses were already tried (and failed)
    // by all providers — they won't be sent to any provider
    let mut cache_tried_incidents = 0u64;
    for (address_key, _, ids) in &keys_and_groups {
        if !resolved_keys.contains(address_key) && cache_tried.contains(address_key) {
            cache_tried_incidents += ids.len() as u64;
        }
    }

    if let Some(p) = progress {
        p.inc(cache_resolved_incidents + cache_tried_incidents);
    }

    if !cache_hits.is_empty() {
        log::info!(
            "Cache: {} addresses resolved from cache ({} already tried and failed)",
            cache_hits.len(),
            cache_tried.len() - cache_hits.len()
        );
    }

    // --- Provider pipeline: iterate services in priority order ---
    let services = enabled_services();
    let filtered_services: Vec<_> = if nominatim_only {
        services
            .into_iter()
            .filter(|s| s.id == "nominatim")
            .collect()
    } else {
        services
    };

    let mut state = ResolveState {
        resolved_keys,
        pending_updates,
        cache_writes,
    };

    for service in &filtered_services {
        // Collect unresolved addresses for this provider
        let unresolved: Vec<AddressGroup<'_>> = keys_and_groups
            .iter()
            .filter(|(key, _, _)| !state.resolved_keys.contains(key) && !cache_tried.contains(key))
            .cloned()
            .collect();

        if unresolved.is_empty() {
            break;
        }

        match &service.provider {
            ProviderConfig::Census {
                base_url,
                benchmark,
                max_batch_size,
            } => {
                resolve_via_census(
                    client,
                    base_url,
                    benchmark,
                    *max_batch_size,
                    &unresolved,
                    &mut state,
                    progress.as_ref(),
                )
                .await?;
            }
            ProviderConfig::Pelias {
                base_url,
                country_code,
                concurrent_requests,
            } => {
                // Allow overriding the compile-time base URL via env var
                // (e.g. for CI connecting through a Cloudflare Tunnel).
                let base_url = std::env::var("PELIAS_URL").unwrap_or_else(|_| base_url.clone());

                // Load optional Cloudflare Access credentials for tunneled
                // instances protected by Zero Trust.
                let cf_access = crime_map_geocoder::pelias::cf_access_credentials_from_env();

                // Health check — skip if the instance is unreachable
                if !crime_map_geocoder::pelias::is_available(client, &base_url, cf_access.as_ref())
                    .await
                {
                    log::info!("Pelias at {base_url} is not reachable, skipping");
                    continue;
                }
                resolve_via_pelias(
                    client,
                    &base_url,
                    country_code,
                    *concurrent_requests,
                    &unresolved,
                    &mut state,
                    progress.as_ref(),
                    cf_access.as_ref(),
                )
                .await?;
            }
            ProviderConfig::Nominatim {
                base_url,
                rate_limit_ms,
            } => {
                resolve_via_nominatim(
                    client,
                    base_url,
                    *rate_limit_ms,
                    &unresolved,
                    &mut state,
                    progress.as_ref(),
                )
                .await?;
            }
            ProviderConfig::TantivyIndex => {
                if !crime_map_geocoder::tantivy_index::is_available() {
                    log::info!("Tantivy geocoder index not found, skipping");
                    continue;
                }
                resolve_via_tantivy(&unresolved, &mut state, progress.as_ref()).await?;
            }
        }
    }

    // --- Flush cache writes ---
    if !state.cache_writes.is_empty() {
        log::info!(
            "Writing {} entries to geocode cache...",
            state.cache_writes.len()
        );
        geocode_cache::cache_insert(cache_conn, &state.cache_writes)?;
    }

    Ok((state.pending_updates, all_ids))
}

/// Shared mutable state threaded through provider-specific resolve functions.
struct ResolveState {
    resolved_keys: std::collections::BTreeSet<String>,
    pending_updates: Vec<(String, f64, f64)>,
    cache_writes: Vec<CacheEntry>,
}

/// Resolves addresses via the US Census Bureau batch geocoder.
#[allow(clippy::too_many_lines)]
async fn resolve_via_census(
    client: &reqwest::Client,
    base_url: &str,
    benchmark: &str,
    max_batch_size: usize,
    unresolved: &[AddressGroup<'_>],
    state: &mut ResolveState,
    progress: Option<&Arc<dyn ProgressCallback>>,
) -> Result<(), Box<dyn std::error::Error>> {
    use crime_map_geocoder::AddressInput;
    use std::collections::BTreeSet;

    let inputs: Vec<(AddressInput, &str, &Vec<String>)> = unresolved
        .iter()
        .enumerate()
        .map(|(i, (address_key, (street, city, addr_state), ids))| {
            (
                AddressInput {
                    id: i.to_string(),
                    street: street.clone(),
                    city: city.clone(),
                    state: addr_state.clone(),
                    zip: None,
                },
                address_key.as_str(),
                *ids,
            )
        })
        .collect();

    for chunk in inputs.chunks(max_batch_size) {
        let batch_inputs: Vec<AddressInput> =
            chunk.iter().map(|(input, _, _)| input.clone()).collect();

        log::info!(
            "Sending batch of {} addresses to Census geocoder...",
            batch_inputs.len()
        );

        let mut matched_keys: BTreeSet<String> = BTreeSet::new();

        match crime_map_geocoder::census::geocode_batch(client, base_url, benchmark, &batch_inputs)
            .await
        {
            Ok(result) => {
                log::info!(
                    "Census batch: {} matched, {} unmatched",
                    result.matched.len(),
                    result.unmatched.len()
                );

                for (id_str, geocoded) in &result.matched {
                    let idx: usize = id_str.parse().unwrap_or(usize::MAX);
                    if let Some(&(_, address_key, ids)) = chunk.get(idx) {
                        matched_keys.insert(address_key.to_string());
                        state.resolved_keys.insert(address_key.to_string());

                        state.cache_writes.push((
                            address_key.to_string(),
                            "census".to_string(),
                            Some(geocoded.latitude),
                            Some(geocoded.longitude),
                            geocoded.matched_address.clone(),
                        ));

                        for id in ids {
                            state.pending_updates.push((
                                id.clone(),
                                geocoded.longitude,
                                geocoded.latitude,
                            ));
                        }

                        if let Some(p) = progress {
                            p.inc(ids.len() as u64);
                        }
                    }
                }
            }
            Err(e) => {
                log::error!("Census batch geocoding failed: {e}");
            }
        }

        // Cache misses for Census
        for &(_, address_key, _) in chunk {
            if !matched_keys.contains(address_key) {
                state.cache_writes.push((
                    address_key.to_string(),
                    "census".to_string(),
                    None,
                    None,
                    None,
                ));
            }
        }
    }

    Ok(())
}

/// Resolves addresses via a self-hosted Pelias instance.
#[allow(clippy::too_many_arguments)]
async fn resolve_via_pelias(
    client: &reqwest::Client,
    base_url: &str,
    country_code: &str,
    concurrent_requests: usize,
    unresolved: &[AddressGroup<'_>],
    state: &mut ResolveState,
    progress: Option<&Arc<dyn ProgressCallback>>,
    cf_access: Option<&crime_map_geocoder::pelias::CfAccessCredentials>,
) -> Result<(), Box<dyn std::error::Error>> {
    use futures::stream::{self, StreamExt as _};

    log::info!(
        "Sending {} addresses to Pelias (concurrency={concurrent_requests})...",
        unresolved.len()
    );

    let cf_access_owned = cf_access.cloned();

    // Fire concurrent requests via buffered stream
    let results: Vec<_> = stream::iter(unresolved.iter().map(|(address_key, _, ids)| {
        let key = address_key.clone();
        let ids_clone = (*ids).clone();
        let creds = cf_access_owned.clone();
        async move {
            let result = crime_map_geocoder::pelias::geocode_freeform(
                client,
                base_url,
                country_code,
                &key,
                creds.as_ref(),
            )
            .await;
            (key, ids_clone, result)
        }
    }))
    .buffer_unordered(concurrent_requests)
    .collect()
    .await;

    for (address_key, ids, result) in results {
        match result {
            Ok(Some(geocoded)) => {
                state.resolved_keys.insert(address_key.clone());
                state.cache_writes.push((
                    address_key,
                    "pelias".to_string(),
                    Some(geocoded.latitude),
                    Some(geocoded.longitude),
                    geocoded.matched_address.clone(),
                ));
                for id in &ids {
                    state
                        .pending_updates
                        .push((id.clone(), geocoded.longitude, geocoded.latitude));
                }
            }
            Ok(None) => {
                log::debug!("Pelias: no match for '{address_key}'");
                state
                    .cache_writes
                    .push((address_key, "pelias".to_string(), None, None, None));
            }
            Err(e) => {
                log::warn!("Pelias error for '{address_key}': {e}");
            }
        }

        if let Some(p) = progress {
            p.inc(ids.len() as u64);
        }
    }

    Ok(())
}

/// Resolves addresses via the local Tantivy geocoder index.
async fn resolve_via_tantivy(
    unresolved: &[AddressGroup<'_>],
    state: &mut ResolveState,
    progress: Option<&Arc<dyn ProgressCallback>>,
) -> Result<(), Box<dyn std::error::Error>> {
    use futures::stream::{self, StreamExt as _};

    log::info!(
        "Searching {} addresses against Tantivy index...",
        unresolved.len()
    );

    let geocoder = crime_map_geocoder::tantivy_index::TantivyGeocoder::open_default()
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;

    // Use a generous concurrency — Tantivy searches are in-process
    // and CPU-bound, so we let tokio's blocking pool manage threads.
    let concurrent_requests = 50;

    let results: Vec<_> = stream::iter(unresolved.iter().map(|(address_key, _, ids)| {
        let key = address_key.clone();
        let ids_clone = (*ids).clone();
        let geocoder = geocoder.clone();
        async move {
            let result = crime_map_geocoder::tantivy_index::geocode_freeform(&geocoder, &key).await;
            (key, ids_clone, result)
        }
    }))
    .buffer_unordered(concurrent_requests)
    .collect()
    .await;

    for (address_key, ids, result) in results {
        match result {
            Ok(Some(hit)) => {
                state.resolved_keys.insert(address_key.clone());
                state.cache_writes.push((
                    address_key,
                    "tantivy".to_string(),
                    Some(hit.latitude),
                    Some(hit.longitude),
                    hit.matched_address.clone(),
                ));
                for id in &ids {
                    state
                        .pending_updates
                        .push((id.clone(), hit.longitude, hit.latitude));
                }
            }
            Ok(None) => {
                log::debug!("Tantivy: no match for '{address_key}'");
                state
                    .cache_writes
                    .push((address_key, "tantivy".to_string(), None, None, None));
            }
            Err(e) => {
                log::warn!("Tantivy error for '{address_key}': {e}");
            }
        }

        if let Some(p) = progress {
            p.inc(ids.len() as u64);
        }
    }

    Ok(())
}

/// Resolves addresses via Nominatim (rate-limited, one at a time).
async fn resolve_via_nominatim(
    client: &reqwest::Client,
    base_url: &str,
    rate_limit_ms: u64,
    unresolved: &[AddressGroup<'_>],
    state: &mut ResolveState,
    progress: Option<&Arc<dyn ProgressCallback>>,
) -> Result<(), Box<dyn std::error::Error>> {
    log::info!(
        "Attempting Nominatim fallback for {} remaining addresses...",
        unresolved.len()
    );

    for (address_key, _, ids) in unresolved {
        tokio::time::sleep(std::time::Duration::from_millis(rate_limit_ms)).await;

        match crime_map_geocoder::nominatim::geocode_freeform(client, base_url, address_key).await {
            Ok(Some(geocoded)) => {
                state.resolved_keys.insert(address_key.clone());
                state.cache_writes.push((
                    address_key.clone(),
                    "nominatim".to_string(),
                    Some(geocoded.latitude),
                    Some(geocoded.longitude),
                    geocoded.matched_address.clone(),
                ));

                for id in *ids {
                    state
                        .pending_updates
                        .push((id.clone(), geocoded.longitude, geocoded.latitude));
                }
            }
            Ok(None) => {
                log::debug!("Nominatim: no match for '{address_key}'");
                state.cache_writes.push((
                    address_key.clone(),
                    "nominatim".to_string(),
                    None,
                    None,
                    None,
                ));
            }
            Err(e) => {
                log::warn!("Nominatim error for '{address_key}': {e}");
                if matches!(e, crime_map_geocoder::GeocodeError::RateLimited) {
                    log::warn!("Rate limited by Nominatim, waiting 60s...");
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                }
                // Don't cache errors — we'll retry next time
            }
        }

        // Progress: each address attempt (hit, miss, or error) advances the bar
        if let Some(p) = progress {
            p.inc(ids.len() as u64);
        }
    }

    Ok(())
}

/// Geocodes incidents that have block addresses but no coordinates.
///
/// Fetches un-geocoded incidents from the per-source `DuckDB` in batches,
/// deduplicates by address, resolves through the geocoding pipeline
/// (cache → Census → Nominatim), then updates the incidents with the
/// resolved coordinates. Loops until all eligible incidents have been
/// processed.
///
/// # Errors
///
/// Returns an error if database queries, geocoding, or batch updates fail.
#[allow(
    clippy::too_many_lines,
    clippy::needless_pass_by_value,
    clippy::future_not_send
)]
pub async fn geocode_missing(
    source_conn: &Connection,
    cache_conn: &Connection,
    batch_size: u64,
    limit: Option<u64>,
    nominatim_only: bool,
    progress: Option<Arc<dyn ProgressCallback>>,
) -> Result<u64, Box<dyn std::error::Error>> {
    use crime_map_geocoder::address::{CleanedAddress, clean_block_address};
    use std::collections::BTreeMap;

    // Query total un-geocoded count for progress reporting
    if let Some(ref p) = progress {
        let mut stmt = source_conn.prepare(
            "SELECT COUNT(*) FROM incidents
             WHERE has_coordinates = FALSE
               AND block_address IS NOT NULL
               AND block_address != ''
               AND geocoded = FALSE",
        )?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        #[allow(clippy::cast_sign_loss)]
        p.set_total(count as u64);
    }

    let client = reqwest::Client::builder()
        .user_agent("crime-map/1.0 (https://github.com/BSteffaniak/crime-map)")
        .build()?;

    let mut grand_total = 0u64;
    let mut batch_num = 0u64;

    loop {
        batch_num += 1;

        let effective_size = limit.map_or(batch_size, |l| batch_size.min(l - grand_total));
        if effective_size == 0 {
            break;
        }

        let mut stmt = source_conn.prepare(
            "SELECT source_incident_id, block_address, city, state
             FROM incidents
             WHERE has_coordinates = FALSE
               AND block_address IS NOT NULL
               AND block_address != ''
               AND geocoded = FALSE
             LIMIT ?",
        )?;

        let rows: Vec<(String, String, String, String)> = {
            let effective_i64 = i64::try_from(effective_size).unwrap_or(i64::MAX);
            let mut raw_rows = stmt.query([effective_i64])?;
            let mut collected = Vec::new();
            while let Some(row) = raw_rows.next()? {
                collected.push((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ));
            }
            collected
        };

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

        let mut addr_groups: BTreeMap<(String, String, String), Vec<String>> = BTreeMap::new();
        let mut skipped_count = 0u64;

        for (incident_id, block, city, state) in &rows {
            let cleaned = clean_block_address(block);
            let street = match cleaned {
                CleanedAddress::Street(s) => s,
                CleanedAddress::Intersection { street1, street2 } => {
                    format!("{street1} and {street2}")
                }
                CleanedAddress::NotGeocodable => {
                    skipped_count += 1;
                    continue;
                }
            };

            addr_groups
                .entry((street, city.clone(), state.clone()))
                .or_default()
                .push(incident_id.clone());
        }

        // Progress: count incidents with un-geocodable addresses
        if let Some(ref p) = progress
            && skipped_count > 0
        {
            p.inc(skipped_count);
        }

        log::info!(
            "Deduplicated to {} unique addresses from {} incidents",
            addr_groups.len(),
            rows.len()
        );

        let (pending_updates, all_ids) =
            resolve_addresses(cache_conn, &client, &addr_groups, nominatim_only, &progress).await?;

        let mut batch_geocoded = 0u64;

        if !pending_updates.is_empty() {
            log::info!(
                "Writing {} geocoded incidents to database...",
                pending_updates.len()
            );
            batch_geocoded +=
                source_db::batch_update_geocoded(source_conn, &pending_updates, false)?;
        }

        // Mark all processed incidents as geocoded = TRUE so they're not
        // re-fetched in the next iteration (even if geocoding failed)
        let failed_ids: Vec<String> = all_ids
            .iter()
            .filter(|id| !pending_updates.iter().any(|(uid, _, _)| uid == *id))
            .cloned()
            .collect();

        if !failed_ids.is_empty() {
            log::info!(
                "Marking {} incidents as attempted (no match found)",
                failed_ids.len()
            );
            source_db::batch_mark_geocoded(source_conn, &failed_ids)?;
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
///
/// # Errors
///
/// Returns an error if database queries, geocoding, or batch updates fail.
#[allow(
    clippy::too_many_lines,
    clippy::needless_pass_by_value,
    clippy::future_not_send
)]
pub async fn re_geocode_source(
    source_conn: &Connection,
    cache_conn: &Connection,
    batch_size: u64,
    limit: Option<u64>,
    nominatim_only: bool,
    progress: Option<Arc<dyn ProgressCallback>>,
) -> Result<u64, Box<dyn std::error::Error>> {
    use crime_map_geocoder::address::{CleanedAddress, clean_block_address};
    use std::collections::BTreeMap;

    // Query total eligible count for progress reporting
    if let Some(ref p) = progress {
        let mut stmt = source_conn.prepare(
            "SELECT COUNT(*) FROM incidents
             WHERE has_coordinates = TRUE
               AND geocoded = FALSE
               AND block_address IS NOT NULL
               AND block_address != ''",
        )?;
        let count: i64 = stmt.query_row([], |row| row.get(0))?;
        #[allow(clippy::cast_sign_loss)]
        p.set_total(count as u64);
    }

    let client = reqwest::Client::builder()
        .user_agent("crime-map/1.0 (https://github.com/BSteffaniak/crime-map)")
        .build()?;

    let mut grand_total = 0u64;
    let mut batch_num = 0u64;

    loop {
        batch_num += 1;

        let effective_size = limit.map_or(batch_size, |l| batch_size.min(l - grand_total));
        if effective_size == 0 {
            break;
        }

        let mut stmt = source_conn.prepare(
            "SELECT source_incident_id, block_address, city, state
             FROM incidents
             WHERE has_coordinates = TRUE
               AND geocoded = FALSE
               AND block_address IS NOT NULL
               AND block_address != ''
             LIMIT ?",
        )?;

        let rows: Vec<(String, String, String, String)> = {
            let effective_i64 = i64::try_from(effective_size).unwrap_or(i64::MAX);
            let mut raw_rows = stmt.query([effective_i64])?;
            let mut collected = Vec::new();
            while let Some(row) = raw_rows.next()? {
                collected.push((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ));
            }
            collected
        };

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

        let mut addr_groups: BTreeMap<(String, String, String), Vec<String>> = BTreeMap::new();
        let mut skipped_count = 0u64;

        for (incident_id, block, city, state) in &rows {
            let cleaned = clean_block_address(block);
            let street = match cleaned {
                CleanedAddress::Street(s) => s,
                CleanedAddress::Intersection { street1, street2 } => {
                    format!("{street1} and {street2}")
                }
                CleanedAddress::NotGeocodable => {
                    skipped_count += 1;
                    continue;
                }
            };

            addr_groups
                .entry((street, city.clone(), state.clone()))
                .or_default()
                .push(incident_id.clone());
        }

        // Progress: count incidents with un-geocodable addresses
        if let Some(ref p) = progress
            && skipped_count > 0
        {
            p.inc(skipped_count);
        }

        log::info!(
            "Deduplicated to {} unique addresses from {} incidents",
            addr_groups.len(),
            rows.len()
        );

        let (pending_updates, all_ids) =
            resolve_addresses(cache_conn, &client, &addr_groups, nominatim_only, &progress).await?;

        let mut batch_geocoded = 0u64;

        if !pending_updates.is_empty() {
            log::info!(
                "Writing {} re-geocoded incidents to database...",
                pending_updates.len()
            );
            batch_geocoded +=
                source_db::batch_update_geocoded(source_conn, &pending_updates, true)?;
        }

        // Mark all processed incidents as geocoded = TRUE
        let failed_ids: Vec<String> = all_ids
            .iter()
            .filter(|id| !pending_updates.iter().any(|(uid, _, _)| uid == *id))
            .cloned()
            .collect();

        if !failed_ids.is_empty() {
            log::info!(
                "Marking {} incidents as attempted (no match found)",
                failed_ids.len()
            );
            source_db::batch_mark_geocoded(source_conn, &failed_ids)?;
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
