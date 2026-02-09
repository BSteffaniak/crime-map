#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions)]

//! CLI tool for ingesting crime data from public sources into the `PostGIS`
//! database.

use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, Subcommand};
use crime_map_database::{db, queries, run_migrations};
use crime_map_source::sources::boston::BostonSource;
use crime_map_source::sources::chicago::ChicagoSource;
use crime_map_source::sources::dc::DcSource;
use crime_map_source::sources::denver::DenverSource;
use crime_map_source::sources::la::LaSource;
use crime_map_source::sources::nyc::NycSource;
use crime_map_source::sources::philly::PhillySource;
use crime_map_source::sources::seattle::SeattleSource;
use crime_map_source::sources::sf::SfSource;
use crime_map_source::{CrimeSource, FetchOptions};

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
        /// Comma-separated list of source IDs to sync (overrides CRIME_MAP_SOURCES env var)
        #[arg(long)]
        sources: Option<String>,
    },
    /// Sync data from a specific source
    Sync {
        /// Source identifier (e.g., "`chicago_pd`")
        source: String,
        /// Maximum number of records to fetch
        #[arg(long)]
        limit: Option<u64>,
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
        Commands::Sync { source, limit } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;
            let sources = all_sources();
            let src = sources
                .iter()
                .find(|s| s.id() == source)
                .ok_or_else(|| format!("Unknown source: {source}"))?;
            sync_source(db.as_ref(), src.as_ref(), limit).await?;
        }
        Commands::SyncAll { limit, sources } => {
            let db = db::connect_from_env().await?;
            run_migrations(db.as_ref()).await?;
            let sources = enabled_sources(sources);
            log::info!(
                "Syncing {} source(s): {}",
                sources.len(),
                sources
                    .iter()
                    .map(|s| s.id())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            for src in &sources {
                if let Err(e) = sync_source(db.as_ref(), src.as_ref(), limit).await {
                    log::error!("Failed to sync {}: {e}", src.id());
                }
            }
        }
    }

    Ok(())
}

/// Returns all configured data sources.
fn all_sources() -> Vec<Box<dyn CrimeSource>> {
    vec![
        Box::new(ChicagoSource::new()),
        Box::new(LaSource::new()),
        Box::new(SfSource::new()),
        Box::new(SeattleSource::new()),
        Box::new(NycSource::new()),
        Box::new(DenverSource::new()),
        Box::new(DcSource::new()),
        Box::new(PhillySource::new()),
        Box::new(BostonSource::new()),
    ]
}

/// Returns the sources to sync, filtered by the `--sources` CLI flag or the
/// `CRIME_MAP_SOURCES` environment variable. If neither is set, all sources
/// are returned.
fn enabled_sources(cli_filter: Option<String>) -> Vec<Box<dyn CrimeSource>> {
    let filter = cli_filter.or_else(|| std::env::var("CRIME_MAP_SOURCES").ok());

    let all = all_sources();

    let Some(filter_str) = filter else {
        return all;
    };

    let ids: Vec<&str> = filter_str.split(',').map(str::trim).collect();

    let filtered: Vec<Box<dyn CrimeSource>> =
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

/// Fetches, normalizes, and inserts data from a single source.
async fn sync_source(
    db: &dyn switchy_database::Database,
    source: &dyn CrimeSource,
    limit: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    log::info!("Syncing source: {} ({})", source.name(), source.id());

    // Register/upsert the source in the database
    let source_id = queries::upsert_source(
        db,
        source.name(),
        "CITY_API",
        None,
        &format!("{} data", source.name()),
    )
    .await?;

    // Fetch raw data
    let options = FetchOptions {
        since: None,
        limit,
        output_dir: PathBuf::from("data/downloads"),
    };

    log::info!("Fetching data from {}...", source.name());
    let raw_path = source.fetch(&options).await?;

    // Normalize
    log::info!("Normalizing data...");
    let incidents = source.normalize(&raw_path).await?;
    log::info!("Normalized {} incidents", incidents.len());

    // Get category ID mapping
    let category_ids = queries::get_all_category_ids(db).await?;

    // Insert into database
    log::info!("Inserting into database...");
    let inserted = queries::insert_incidents(db, source_id, &incidents, &category_ids).await?;

    // Update source stats
    queries::update_source_stats(db, source_id).await?;

    let elapsed = start.elapsed();
    log::info!(
        "Sync complete for {}: {} inserted, {} total incidents, took {:.1}s",
        source.name(),
        inserted,
        incidents.len(),
        elapsed.as_secs_f64()
    );

    Ok(())
}
