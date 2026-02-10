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
}

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
    // will have fully_synced = false and will do a full fetch.
    let since = if force {
        log::info!("{}: full sync (--force)", source.name());
        None
    } else {
        let fully_synced = queries::get_source_fully_synced(db, source_id).await?;
        let max_occurred = queries::get_source_max_occurred_at(db, source_id).await?;

        if fully_synced {
            max_occurred.map_or_else(
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
            )
        } else {
            if max_occurred.is_some() {
                log::info!("{}: full sync (not yet fully synced)", source.name());
            } else {
                log::info!("{}: full sync (first run)", source.name());
            }
            None
        }
    };

    // Get category ID mapping (needed for insertion)
    let category_ids = queries::get_all_category_ids(db).await?;

    // Start streaming pages from the fetcher
    let options = FetchOptions { since, limit };

    let (mut rx, fetch_handle) = source.fetch_pages(&options);

    let mut total_raw: u64 = 0;
    let mut total_normalized: u64 = 0;
    let mut total_inserted: u64 = 0;
    let mut page_num: u64 = 0;

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
