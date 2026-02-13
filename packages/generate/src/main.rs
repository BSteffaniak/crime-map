#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! CLI entry point for the tile generation tool.
//!
//! Parses command-line arguments and delegates to the library functions in
//! `crime_map_generate`. When invoked without a subcommand, launches the
//! interactive menu.

use clap::{Args, Parser, Subcommand};
use crime_map_database::db;
use crime_map_generate::{
    GenerateArgs, OUTPUT_CLUSTERS_PMTILES, OUTPUT_COUNT_DB, OUTPUT_INCIDENTS_DB,
    OUTPUT_INCIDENTS_PMTILES, output_dir, resolve_source_ids, run_with_cache,
};

#[derive(Parser)]
#[command(name = "crime_map_generate", about = "Tile generation tool")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

/// Shared CLI arguments for all generate subcommands.
#[derive(Args)]
struct CliGenerateArgs {
    /// Maximum number of records to export (useful for testing).
    #[arg(long)]
    limit: Option<u64>,

    /// Comma-separated list of source IDs to include (e.g., "chicago,la,sf").
    /// Only incidents from these sources will be exported.
    #[arg(long)]
    sources: Option<String>,

    /// Keep the intermediate `.geojsonseq` file after generation instead of
    /// deleting it.
    #[arg(long)]
    keep_intermediate: bool,

    /// Force regeneration even if source data hasn't changed.
    #[arg(long)]
    force: bool,
}

impl From<CliGenerateArgs> for GenerateArgs {
    fn from(cli: CliGenerateArgs) -> Self {
        Self {
            limit: cli.limit,
            sources: cli.sources,
            keep_intermediate: cli.keep_intermediate,
            force: cli.force,
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Generate `PMTiles` from `PostGIS` data (heatmap + individual points)
    Pmtiles {
        #[command(flatten)]
        args: CliGenerateArgs,
    },
    /// Generate clustered `PMTiles` for mid-zoom (zoom 8-11) via tippecanoe
    Clusters {
        #[command(flatten)]
        args: CliGenerateArgs,
    },
    /// Generate sidebar `SQLite` database for server-side queries
    Sidebar {
        #[command(flatten)]
        args: CliGenerateArgs,
    },
    /// Generate `DuckDB` count database with pre-aggregated summary table
    CountDb {
        #[command(flatten)]
        args: CliGenerateArgs,
    },
    /// Generate all output files (`PMTiles`, clusters, sidebar `SQLite`, and count `DuckDB`)
    All {
        #[command(flatten)]
        args: CliGenerateArgs,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let cli = Cli::parse();

    let Some(command) = cli.command else {
        return crime_map_generate::interactive::run().await;
    };

    let db = db::connect_from_env().await?;
    let dir = output_dir();
    std::fs::create_dir_all(&dir)?;

    match command {
        Commands::Pmtiles { args } => {
            let args = GenerateArgs::from(args);
            let source_ids = resolve_source_ids(db.as_ref(), &args).await?;
            run_with_cache(
                db.as_ref(),
                &args,
                &source_ids,
                &dir,
                &[OUTPUT_INCIDENTS_PMTILES],
                None,
            )
            .await?;
        }
        Commands::Clusters { args } => {
            let args = GenerateArgs::from(args);
            let source_ids = resolve_source_ids(db.as_ref(), &args).await?;
            run_with_cache(
                db.as_ref(),
                &args,
                &source_ids,
                &dir,
                &[OUTPUT_CLUSTERS_PMTILES],
                None,
            )
            .await?;
        }
        Commands::Sidebar { args } => {
            let args = GenerateArgs::from(args);
            let source_ids = resolve_source_ids(db.as_ref(), &args).await?;
            run_with_cache(
                db.as_ref(),
                &args,
                &source_ids,
                &dir,
                &[OUTPUT_INCIDENTS_DB],
                None,
            )
            .await?;
        }
        Commands::CountDb { args } => {
            let args = GenerateArgs::from(args);
            let source_ids = resolve_source_ids(db.as_ref(), &args).await?;
            run_with_cache(
                db.as_ref(),
                &args,
                &source_ids,
                &dir,
                &[OUTPUT_COUNT_DB],
                None,
            )
            .await?;
        }
        Commands::All { args } => {
            let args = GenerateArgs::from(args);
            let source_ids = resolve_source_ids(db.as_ref(), &args).await?;
            run_with_cache(
                db.as_ref(),
                &args,
                &source_ids,
                &dir,
                &[
                    OUTPUT_INCIDENTS_PMTILES,
                    OUTPUT_CLUSTERS_PMTILES,
                    OUTPUT_INCIDENTS_DB,
                    OUTPUT_COUNT_DB,
                ],
                None,
            )
            .await?;
        }
    }

    Ok(())
}
