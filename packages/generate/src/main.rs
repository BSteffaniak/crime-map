#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! CLI entry point for the tile generation tool.
//!
//! Parses command-line arguments and delegates to the library functions in
//! `crime_map_generate`. When invoked without a subcommand, launches the
//! interactive menu.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use crime_map_generate::{
    GenerateArgs, OUTPUT_ANALYTICS_DB, OUTPUT_BOUNDARIES_DB, OUTPUT_BOUNDARIES_PMTILES,
    OUTPUT_COUNT_DB, OUTPUT_H3_DB, OUTPUT_INCIDENTS_DB, OUTPUT_INCIDENTS_PMTILES, OUTPUT_METADATA,
    output_dir, resolve_source_ids, run_with_cache,
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

    /// Comma-separated state FIPS codes to include (e.g., "24,11" for MD+DC).
    /// Sources whose `state` field matches the given FIPS codes will be
    /// included. Combined with `--sources` via union if both are provided.
    #[arg(long)]
    states: Option<String>,

    /// Output directory for generated files. Defaults to `data/generated/`
    /// in the workspace root.
    #[arg(long)]
    output_dir: Option<PathBuf>,

    /// Keep the intermediate `.geojsonseq` file after generation instead of
    /// deleting it.
    #[arg(long)]
    keep_intermediate: bool,

    /// Force regeneration even if source data hasn't changed.
    #[arg(long)]
    force: bool,

    /// Skip boundary outputs (boundaries `PMTiles` and boundaries search DB).
    /// Useful for partition jobs where boundaries are generated separately.
    #[arg(long)]
    skip_boundaries: bool,
}

impl From<&CliGenerateArgs> for GenerateArgs {
    fn from(cli: &CliGenerateArgs) -> Self {
        Self {
            limit: cli.limit,
            sources: cli.sources.clone(),
            states: cli.states.clone(),
            keep_intermediate: cli.keep_intermediate,
            force: cli.force,
        }
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Generate `PMTiles` from `DuckDB` source data (heatmap + individual points)
    Pmtiles {
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
    /// Generate `DuckDB` H3 hexbin database with pre-aggregated H3 cell counts
    H3Db {
        #[command(flatten)]
        args: CliGenerateArgs,
    },
    /// Generate administrative boundary `PMTiles` (states, counties, places, tracts, neighborhoods)
    Boundaries {
        #[command(flatten)]
        args: CliGenerateArgs,
    },
    /// Generate all output files (`PMTiles`, sidebar `SQLite`, count `DuckDB`, H3 `DuckDB`, boundaries, and metadata)
    All {
        #[command(flatten)]
        args: CliGenerateArgs,
    },
    /// Merge partitioned artifacts from multiple directories into unified output files
    Merge {
        /// Comma-separated list of partition directories to merge.
        #[arg(long)]
        partition_dirs: String,

        /// Directory containing pre-generated boundary artifacts
        /// (`boundaries.pmtiles`, `boundaries.db`). If not provided,
        /// boundaries are skipped in the merge output.
        #[arg(long)]
        boundaries_dir: Option<PathBuf>,

        /// Output directory for merged files. Defaults to `data/generated/`
        /// in the workspace root.
        #[arg(long)]
        output_dir: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let cli = Cli::parse();

    let Some(command) = cli.command else {
        return crime_map_generate::interactive::run().await;
    };

    match command {
        Commands::Merge {
            partition_dirs,
            boundaries_dir,
            output_dir: out_dir,
        } => {
            let dirs: Vec<PathBuf> = partition_dirs
                .split(',')
                .map(|s| PathBuf::from(s.trim()))
                .collect();
            let out = out_dir.unwrap_or_else(output_dir);
            std::fs::create_dir_all(&out)?;
            crime_map_generate::merge::run(&dirs, boundaries_dir.as_deref(), &out).await?;
        }
        cmd => {
            run_generate_command(cmd).await?;
        }
    }

    Ok(())
}

#[allow(clippy::future_not_send)]
async fn run_generate_command(command: Commands) -> Result<(), Box<dyn std::error::Error>> {
    let (cli_args, base_outputs): (&CliGenerateArgs, &[&str]) = match &command {
        Commands::Pmtiles { args } => (args, &[OUTPUT_INCIDENTS_PMTILES]),
        Commands::Sidebar { args } => (args, &[OUTPUT_INCIDENTS_DB]),
        Commands::CountDb { args } => (args, &[OUTPUT_COUNT_DB]),
        Commands::H3Db { args } => (args, &[OUTPUT_H3_DB]),
        Commands::Boundaries { args } => (args, &[OUTPUT_BOUNDARIES_PMTILES, OUTPUT_BOUNDARIES_DB]),
        Commands::All { args } => (
            args,
            &[
                OUTPUT_INCIDENTS_PMTILES,
                OUTPUT_INCIDENTS_DB,
                OUTPUT_COUNT_DB,
                OUTPUT_H3_DB,
                OUTPUT_METADATA,
                OUTPUT_BOUNDARIES_PMTILES,
                OUTPUT_BOUNDARIES_DB,
                OUTPUT_ANALYTICS_DB,
            ][..],
        ),
        Commands::Merge { .. } => unreachable!("Merge handled separately"),
    };

    // Filter out boundary outputs if --skip-boundaries is set
    let outputs: Vec<&str> = if cli_args.skip_boundaries {
        base_outputs
            .iter()
            .copied()
            .filter(|&o| o != OUTPUT_BOUNDARIES_PMTILES && o != OUTPUT_BOUNDARIES_DB)
            .collect()
    } else {
        base_outputs.to_vec()
    };

    let dir = cli_args.output_dir.clone().unwrap_or_else(output_dir);
    std::fs::create_dir_all(&dir)?;

    let args = GenerateArgs::from(cli_args);

    // Boundary-only outputs don't need per-source DuckDB files — they read
    // exclusively from boundaries.duckdb. Skip source resolution so the
    // `boundaries` subcommand works without any files in data/sources/.
    let needs_sources = outputs
        .iter()
        .any(|&o| o != OUTPUT_BOUNDARIES_PMTILES && o != OUTPUT_BOUNDARIES_DB);

    let source_ids = if needs_sources {
        resolve_source_ids(&args)?
    } else {
        Vec::new()
    };

    run_with_cache(&args, &source_ids, &dir, &outputs, None).await?;

    Ok(())
}
