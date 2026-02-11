#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! CLI entry point for the crime data source discovery tool.
//!
//! Dispatches to library command handlers or launches the interactive TUI
//! when no subcommand is provided.

use std::path::PathBuf;

use clap::Parser;
use crime_map_discover::Commands;

/// Discover and track crime data sources.
#[derive(Parser)]
#[command(name = "crime_map_discover")]
#[command(about = "Discover and track crime data sources")]
struct Cli {
    /// Path to the discovery `SQLite` database.
    #[arg(long, default_value = "data/discovery.db")]
    db_path: PathBuf,

    /// Subcommand to execute (launches interactive mode if omitted).
    #[command(subcommand)]
    command: Option<Commands>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let cli = Cli::parse();

    // Open (or create) the discovery `SQLite` database. This also ensures the
    // parent directory exists and runs schema migrations.
    let database = crime_map_discover::db::open_db(&cli.db_path).await?;

    let Some(command) = cli.command else {
        return crime_map_discover::interactive::run_with_db(database.as_ref()).await;
    };

    match command {
        Commands::Status => crime_map_discover::cmd_status(database.as_ref()).await,
        Commands::Leads { action } => {
            crime_map_discover::cmd_leads(database.as_ref(), action).await
        }
        Commands::Sources { action } => {
            crime_map_discover::cmd_sources(database.as_ref(), action).await
        }
        Commands::SearchLog { action } => {
            crime_map_discover::cmd_search_log(database.as_ref(), action).await
        }
        Commands::Legal { action } => {
            crime_map_discover::cmd_legal(database.as_ref(), action).await
        }
        Commands::Scrape { action } => {
            crime_map_discover::cmd_scrape(database.as_ref(), action).await
        }
        Commands::Seed => crime_map_discover::cmd_seed(database.as_ref()).await,
        Commands::Integrate {
            id,
            source_id,
            dry_run,
        } => crime_map_discover::cmd_integrate(database.as_ref(), id, source_id, dry_run).await,
        Commands::Verify { source_id } => {
            crime_map_discover::cmd_verify(database.as_ref(), source_id).await
        }
        Commands::Suggest { region } => {
            crime_map_discover::cmd_suggest(database.as_ref(), region).await
        }
    }
}
