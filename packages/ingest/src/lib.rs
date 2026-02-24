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
        } else if max_occurred.is_some() {
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
        } else {
            log::info!("{}: full sync (first run)", source.name());
            (None, 0)
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
#[allow(clippy::too_many_lines, clippy::type_complexity)]
pub fn resolve_addresses(
    cache_conn: &Connection,
    client: &reqwest::Client,
    addr_groups: &std::collections::BTreeMap<(String, String, String), Vec<String>>,
    nominatim_only: bool,
    progress: &Option<Arc<dyn ProgressCallback>>,
    rt: &tokio::runtime::Handle,
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
                rt.block_on(resolve_via_census(
                    client,
                    base_url,
                    benchmark,
                    *max_batch_size,
                    &unresolved,
                    &mut state,
                    progress.as_ref(),
                ))?;
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
                if !rt.block_on(crime_map_geocoder::pelias::is_available(
                    client,
                    &base_url,
                    cf_access.as_ref(),
                )) {
                    log::info!("Pelias at {base_url} is not reachable, skipping");
                    continue;
                }
                rt.block_on(resolve_via_pelias(
                    client,
                    &base_url,
                    country_code,
                    *concurrent_requests,
                    &unresolved,
                    &mut state,
                    progress.as_ref(),
                    cf_access.as_ref(),
                ))?;
            }
            ProviderConfig::Nominatim {
                base_url,
                rate_limit_ms,
            } => {
                rt.block_on(resolve_via_nominatim(
                    client,
                    base_url,
                    *rate_limit_ms,
                    &unresolved,
                    &mut state,
                    progress.as_ref(),
                ))?;
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
#[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
pub fn geocode_missing(
    source_conn: &Connection,
    cache_conn: &Connection,
    batch_size: u64,
    limit: Option<u64>,
    nominatim_only: bool,
    progress: Option<Arc<dyn ProgressCallback>>,
    rt: &tokio::runtime::Handle,
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

        let (pending_updates, all_ids) = resolve_addresses(
            cache_conn,
            &client,
            &addr_groups,
            nominatim_only,
            &progress,
            rt,
        )?;

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
#[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
pub fn re_geocode_source(
    source_conn: &Connection,
    cache_conn: &Connection,
    batch_size: u64,
    limit: Option<u64>,
    nominatim_only: bool,
    progress: Option<Arc<dyn ProgressCallback>>,
    rt: &tokio::runtime::Handle,
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

        let (pending_updates, all_ids) = resolve_addresses(
            cache_conn,
            &client,
            &addr_groups,
            nominatim_only,
            &progress,
            rt,
        )?;

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
