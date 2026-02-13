#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Library for ingesting crime data from public sources into the `PostGIS`
//! database.

pub mod interactive;
pub mod progress;

use std::sync::Arc;
use std::time::Instant;

use crime_map_database::queries;
use crime_map_source::FetchOptions;
use crime_map_source::progress::ProgressCallback;
use crime_map_source::source_def::SourceDefinition;

/// Safety buffer (in days) for incremental syncs.
///
/// Subtracted from the latest `occurred_at` timestamp to re-fetch a
/// window of recent data, catching records that were backfilled or
/// updated after our previous sync. Duplicates are harmlessly skipped
/// by the `ON CONFLICT DO NOTHING` clause.
pub const INCREMENTAL_BUFFER_DAYS: i64 = 7;

/// A cached geocoding result: `(address_key, provider, lat, lng, matched_address)`.
pub type CacheEntry = (String, String, Option<f64>, Option<f64>, Option<String>);

/// An address group key and its associated incident IDs, paired with the
/// normalized cache key string.
pub type AddressGroup<'a> = (String, &'a (String, String, String), &'a Vec<i64>);

/// Maximum number of parameters `PostgreSQL` allows per statement.
pub const PG_MAX_PARAMS: usize = 65_535;

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
/// By default performs an incremental sync, fetching only records newer than
/// `MAX(occurred_at) - 7 days` for the source. Pass `force = true` to
/// ignore the previous sync point and fetch everything.
///
/// # Errors
///
/// Returns an error if database queries, source fetching, or page
/// normalization/insertion fails.
#[allow(clippy::too_many_lines)]
pub async fn sync_source(
    db: &dyn switchy_database::Database,
    source: &SourceDefinition,
    limit: Option<u64>,
    force: bool,
    progress: Option<Arc<dyn ProgressCallback>>,
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
#[must_use]
pub fn source_filter_params(
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

/// Resolves database source IDs for all TOML sources that have
/// `re_geocode = true`.
///
/// If `filter_toml_id` is provided, only returns the matching source
/// (if it also has `re_geocode = true`).
///
/// # Errors
///
/// Returns an error if a database lookup for a source name fails.
pub async fn resolve_re_geocode_source_ids(
    db: &dyn switchy_database::Database,
    filter_toml_id: Option<&str>,
) -> Result<Vec<i32>, Box<dyn std::error::Error>> {
    let sources = all_sources();
    let re_geocode_sources: Vec<&SourceDefinition> = sources
        .iter()
        .filter(|s| s.re_geocode())
        .filter(|s| filter_toml_id.is_none_or(|id| s.id() == id))
        .collect();

    let mut db_ids = Vec::new();
    for src in &re_geocode_sources {
        match queries::get_source_id_by_name(db, src.name()).await {
            Ok(sid) => {
                log::info!(
                    "Source '{}' ({}) is marked for re-geocoding (db id={sid})",
                    src.id(),
                    src.name()
                );
                db_ids.push(sid);
            }
            Err(e) => {
                log::warn!(
                    "Source '{}' has re_geocode=true but not found in DB: {e}",
                    src.id()
                );
            }
        }
    }

    Ok(db_ids)
}

/// Applies geocoded coordinates to incidents using batch `UPDATE … FROM
/// (VALUES …)` statements instead of individual row updates.
///
/// When `clear_attribution` is `true` (used by re-geocode), the census
/// place and tract GEOIDs are also cleared so the next `attribute` run
/// reassigns them based on the new coordinates.
///
/// Returns the number of rows updated.
///
/// # Errors
///
/// Returns an error if the batch UPDATE statement fails.
pub async fn batch_update_geocoded(
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

/// Marks incidents as `geocoded = TRUE` without changing their location.
///
/// Used after all geocoding providers have been exhausted for an incident
/// so it won't be re-fetched in the next batch iteration.
///
/// # Errors
///
/// Returns an error if the batch UPDATE statement fails.
pub async fn batch_mark_geocoded(
    db: &dyn switchy_database::Database,
    ids: &[i64],
) -> Result<u64, Box<dyn std::error::Error>> {
    use std::fmt::Write as _;
    use switchy_database::DatabaseValue;

    if ids.is_empty() {
        return Ok(0);
    }

    let mut total = 0u64;
    let params_per_row: usize = 1;
    let chunk_size = PG_MAX_PARAMS / params_per_row;

    for chunk in ids.chunks(chunk_size) {
        let mut sql = String::from("UPDATE crime_incidents SET geocoded = TRUE WHERE id IN (");
        let mut params: Vec<DatabaseValue> = Vec::with_capacity(chunk.len());

        for (i, &id) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            write!(sql, "${}", i + 1).unwrap();
            params.push(DatabaseValue::Int64(id));
        }

        sql.push(')');
        total += db.exec_raw_params(&sql, &params).await?;
    }

    Ok(total)
}

/// Looks up cached geocoding results for the given address keys.
///
/// Returns a map from `address_key` to `(lat, lng)` for cache hits
/// (only entries where coordinates are not null — i.e., successful
/// geocodes). Entries where coordinates are null (failed lookups)
/// are *not* returned as hits but their existence means we should
/// skip re-querying that provider.
///
/// # Errors
///
/// Returns an error if the cache query fails.
pub async fn cache_lookup(
    db: &dyn switchy_database::Database,
    address_keys: &[String],
) -> Result<
    (
        std::collections::BTreeMap<String, (f64, f64)>,
        std::collections::BTreeSet<String>,
    ),
    Box<dyn std::error::Error>,
> {
    use moosicbox_json_utils::database::ToValue as _;
    use std::collections::{BTreeMap, BTreeSet};
    use std::fmt::Write as _;
    use switchy_database::DatabaseValue;

    let mut hits: BTreeMap<String, (f64, f64)> = BTreeMap::new();
    let mut tried: BTreeSet<String> = BTreeSet::new();

    if address_keys.is_empty() {
        return Ok((hits, tried));
    }

    let params_per_row: usize = 1;
    let chunk_size = PG_MAX_PARAMS / params_per_row;

    for chunk in address_keys.chunks(chunk_size) {
        let mut sql =
            String::from("SELECT address_key, lat, lng FROM geocode_cache WHERE address_key IN (");
        let mut params: Vec<DatabaseValue> = Vec::with_capacity(chunk.len());

        for (i, key) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            write!(sql, "${}", i + 1).unwrap();
            params.push(DatabaseValue::String(key.clone()));
        }
        sql.push(')');

        let rows = db.query_raw_params(&sql, &params).await?;

        for row in &rows {
            let key: String = row.to_value("address_key").unwrap_or_default();
            tried.insert(key.clone());

            let lat: Option<f64> = row.to_value("lat").ok();
            let lng: Option<f64> = row.to_value("lng").ok();

            if let (Some(lat_v), Some(lng_v)) = (lat, lng) {
                hits.insert(key, (lat_v, lng_v));
            }
        }
    }

    Ok((hits, tried))
}

/// Inserts geocoding results (both hits and misses) into the cache.
///
/// # Errors
///
/// Returns an error if the INSERT statement fails.
pub async fn cache_insert(
    db: &dyn switchy_database::Database,
    entries: &[CacheEntry],
) -> Result<(), Box<dyn std::error::Error>> {
    use std::fmt::Write as _;
    use switchy_database::DatabaseValue;

    if entries.is_empty() {
        return Ok(());
    }

    // 5 params per row: address_key, provider, lat, lng, matched_address
    let params_per_row: usize = 5;
    let chunk_size = PG_MAX_PARAMS / params_per_row;

    for chunk in entries.chunks(chunk_size) {
        let mut sql = String::from(
            "INSERT INTO geocode_cache (address_key, provider, lat, lng, matched_address) VALUES ",
        );
        let mut params: Vec<DatabaseValue> = Vec::with_capacity(chunk.len() * params_per_row);
        let mut idx = 1u32;

        for (i, (key, provider, lat, lng, matched)) in chunk.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            write!(
                sql,
                "(${idx}, ${p}, ${la}, ${lo}, ${m})",
                p = idx + 1,
                la = idx + 2,
                lo = idx + 3,
                m = idx + 4,
            )
            .unwrap();
            params.push(DatabaseValue::String(key.clone()));
            params.push(DatabaseValue::String(provider.clone()));
            params.push(lat.map_or(DatabaseValue::Null, DatabaseValue::Real64));
            params.push(lng.map_or(DatabaseValue::Null, DatabaseValue::Real64));
            params.push(
                matched
                    .as_ref()
                    .map_or(DatabaseValue::Null, |s| DatabaseValue::String(s.clone())),
            );
            idx += 5;
        }

        sql.push_str(" ON CONFLICT (address_key, provider) DO NOTHING");
        db.exec_raw_params(&sql, &params).await?;
    }

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
/// Returns `(updates, all_incident_ids)` where `updates` are `(id, lng, lat)`
/// tuples for successfully geocoded incidents, and `all_incident_ids` is every
/// incident ID that was processed (for marking as attempted).
///
/// # Errors
///
/// Returns an error if cache lookups, geocoder requests, or cache writes fail.
#[allow(clippy::too_many_lines)]
pub async fn resolve_addresses(
    db: &dyn switchy_database::Database,
    client: &reqwest::Client,
    addr_groups: &std::collections::BTreeMap<(String, String, String), Vec<i64>>,
    nominatim_only: bool,
    progress: &Option<Arc<dyn ProgressCallback>>,
) -> Result<(Vec<(i64, f64, f64)>, Vec<i64>), Box<dyn std::error::Error>> {
    use crime_map_geocoder::address::build_one_line_address;
    use crime_map_geocoder::service_registry::{ProviderConfig, enabled_services};

    let mut pending_updates: Vec<(i64, f64, f64)> = Vec::new();
    let mut all_ids: Vec<i64> = Vec::new();
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
    let (cache_hits, cache_tried) = cache_lookup(db, &all_keys).await?;

    let mut resolved_keys: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    // Apply cache hits
    let mut cache_resolved_incidents = 0u64;
    for (address_key, _, ids) in &keys_and_groups {
        if let Some(&(lat, lng)) = cache_hits.get(address_key) {
            for &id in *ids {
                pending_updates.push((id, lng, lat));
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
                // Health check — skip if the instance is unreachable
                if !crime_map_geocoder::pelias::is_available(client, base_url).await {
                    log::info!("Pelias at {base_url} is not reachable, skipping");
                    continue;
                }
                resolve_via_pelias(
                    client,
                    base_url,
                    country_code,
                    *concurrent_requests,
                    &unresolved,
                    &mut state,
                    progress.as_ref(),
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
        }
    }

    // --- Flush cache writes ---
    if !state.cache_writes.is_empty() {
        log::info!(
            "Writing {} entries to geocode cache...",
            state.cache_writes.len()
        );
        cache_insert(db, &state.cache_writes).await?;
    }

    Ok((state.pending_updates, all_ids))
}

/// Shared mutable state threaded through provider-specific resolve functions.
struct ResolveState {
    resolved_keys: std::collections::BTreeSet<String>,
    pending_updates: Vec<(i64, f64, f64)>,
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

    let inputs: Vec<(AddressInput, &str, &Vec<i64>)> = unresolved
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

                        for &id in ids {
                            state
                                .pending_updates
                                .push((id, geocoded.longitude, geocoded.latitude));
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
async fn resolve_via_pelias(
    client: &reqwest::Client,
    base_url: &str,
    country_code: &str,
    concurrent_requests: usize,
    unresolved: &[AddressGroup<'_>],
    state: &mut ResolveState,
    progress: Option<&Arc<dyn ProgressCallback>>,
) -> Result<(), Box<dyn std::error::Error>> {
    use futures::stream::{self, StreamExt as _};

    log::info!(
        "Sending {} addresses to Pelias (concurrency={concurrent_requests})...",
        unresolved.len()
    );

    // Fire concurrent requests via buffered stream
    let results: Vec<_> = stream::iter(unresolved.iter().map(|(address_key, _, ids)| {
        let key = address_key.clone();
        let ids_clone = (*ids).clone();
        async move {
            let result =
                crime_map_geocoder::pelias::geocode_freeform(client, base_url, country_code, &key)
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
                for &id in &ids {
                    state
                        .pending_updates
                        .push((id, geocoded.longitude, geocoded.latitude));
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

                for &id in *ids {
                    state
                        .pending_updates
                        .push((id, geocoded.longitude, geocoded.latitude));
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
/// Fetches un-geocoded incidents from the database in batches, deduplicates
/// by address, resolves through the geocoding pipeline (cache → Census →
/// Nominatim), then updates the incidents with the resolved coordinates.
/// Loops until all eligible incidents have been processed.
///
/// # Errors
///
/// Returns an error if database queries, geocoding, or batch updates fail.
#[allow(clippy::too_many_lines)]
pub async fn geocode_missing(
    db: &dyn switchy_database::Database,
    batch_size: u64,
    limit: Option<u64>,
    nominatim_only: bool,
    source_id: Option<i32>,
    progress: Option<Arc<dyn ProgressCallback>>,
) -> Result<u64, Box<dyn std::error::Error>> {
    use crime_map_geocoder::address::{CleanedAddress, clean_block_address};
    use moosicbox_json_utils::database::ToValue as _;
    use std::collections::BTreeMap;

    let (source_clause, _) = source_filter_params(batch_size, source_id);

    // Query total un-geocoded count for progress reporting
    if let Some(ref p) = progress {
        let (count_clause, count_params) = source_id.map_or_else(
            || ("", vec![]),
            |sid| {
                (
                    " AND source_id = $1",
                    vec![switchy_database::DatabaseValue::Int32(sid)],
                )
            },
        );
        let count_query = format!(
            "SELECT COUNT(*) as cnt FROM crime_incidents
             WHERE has_coordinates = FALSE
               AND block_address IS NOT NULL
               AND block_address != ''
               AND geocoded = FALSE{count_clause}"
        );
        let rows = db.query_raw_params(&count_query, &count_params).await?;
        if let Some(row) = rows.first() {
            let count: i64 = row.to_value("cnt").unwrap_or(0);
            #[allow(clippy::cast_sign_loss)]
            p.set_total(count as u64);
        }
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

        let mut addr_groups: BTreeMap<(String, String, String), Vec<i64>> = BTreeMap::new();
        let mut skipped_count = 0u64;

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
                CleanedAddress::NotGeocodable => {
                    skipped_count += 1;
                    continue;
                }
            };

            addr_groups
                .entry((street, city, state))
                .or_default()
                .push(id);
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
            resolve_addresses(db, &client, &addr_groups, nominatim_only, &progress).await?;

        let mut batch_geocoded = 0u64;

        if !pending_updates.is_empty() {
            log::info!(
                "Writing {} geocoded incidents to database...",
                pending_updates.len()
            );
            batch_geocoded += batch_update_geocoded(db, &pending_updates, false).await?;
        }

        // Mark all processed incidents as geocoded = TRUE so they're not
        // re-fetched in the next iteration (even if geocoding failed)
        let failed_ids: Vec<i64> = all_ids
            .iter()
            .copied()
            .filter(|id| !pending_updates.iter().any(|(uid, _, _)| uid == id))
            .collect();

        if !failed_ids.is_empty() {
            log::info!(
                "Marking {} incidents as attempted (no match found)",
                failed_ids.len()
            );
            batch_mark_geocoded(db, &failed_ids).await?;
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
#[allow(clippy::too_many_lines)]
pub async fn re_geocode_source(
    db: &dyn switchy_database::Database,
    batch_size: u64,
    limit: Option<u64>,
    nominatim_only: bool,
    source_id: Option<i32>,
    progress: Option<Arc<dyn ProgressCallback>>,
) -> Result<u64, Box<dyn std::error::Error>> {
    use crime_map_geocoder::address::{CleanedAddress, clean_block_address};
    use moosicbox_json_utils::database::ToValue as _;
    use std::collections::BTreeMap;

    let (source_clause, _) = source_filter_params(batch_size, source_id);

    // Query total eligible count for progress reporting
    if let Some(ref p) = progress {
        let (count_clause, count_params) = source_id.map_or_else(
            || ("", vec![]),
            |sid| {
                (
                    " AND source_id = $1",
                    vec![switchy_database::DatabaseValue::Int32(sid)],
                )
            },
        );
        let count_query = format!(
            "SELECT COUNT(*) as cnt FROM crime_incidents
             WHERE has_coordinates = TRUE
               AND geocoded = FALSE
               AND block_address IS NOT NULL
               AND block_address != ''{count_clause}"
        );
        let rows = db.query_raw_params(&count_query, &count_params).await?;
        if let Some(row) = rows.first() {
            let count: i64 = row.to_value("cnt").unwrap_or(0);
            #[allow(clippy::cast_sign_loss)]
            p.set_total(count as u64);
        }
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

        let mut addr_groups: BTreeMap<(String, String, String), Vec<i64>> = BTreeMap::new();
        let mut skipped_count = 0u64;

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
                CleanedAddress::NotGeocodable => {
                    skipped_count += 1;
                    continue;
                }
            };

            addr_groups
                .entry((street, city, state))
                .or_default()
                .push(id);
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
            resolve_addresses(db, &client, &addr_groups, nominatim_only, &progress).await?;

        let mut batch_geocoded = 0u64;

        if !pending_updates.is_empty() {
            log::info!(
                "Writing {} re-geocoded incidents to database...",
                pending_updates.len()
            );
            batch_geocoded += batch_update_geocoded(db, &pending_updates, true).await?;
        }

        // Mark all processed incidents as geocoded = TRUE
        let failed_ids: Vec<i64> = all_ids
            .iter()
            .copied()
            .filter(|id| !pending_updates.iter().any(|(uid, _, _)| uid == id))
            .collect();

        if !failed_ids.is_empty() {
            log::info!(
                "Marking {} incidents as attempted (no match found)",
                failed_ids.len()
            );
            batch_mark_geocoded(db, &failed_ids).await?;
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
