#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Interactive CLI orchestrator for the crime map toolchain.
//!
//! Provides a unified entry point (`cargo crime-map`) that lets users
//! interactively select which tool to run (ingest, generate, server,
//! discover, conversations) and guides them through the configuration
//! for each.
//!
//! Uses `indicatif-log-bridge` (via [`crime_map_cli_utils::init_logger`])
//! to route `log` output through `indicatif::MultiProgress` so that log
//! lines and progress bars never fight for the terminal.

mod pipeline;

use dialoguer::Select;

/// Top-level tool selection for the crime map toolchain.
enum Tool {
    RunPipeline,
    Ingest,
    Generate,
    Conversations,
    Server,
    Discover,
}

impl Tool {
    const ALL: &[Self] = &[
        Self::RunPipeline,
        Self::Ingest,
        Self::Generate,
        Self::Conversations,
        Self::Server,
        Self::Discover,
    ];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::RunPipeline => "Run full pipeline",
            Self::Ingest => "Ingest data",
            Self::Generate => "Generate tiles & databases",
            Self::Conversations => "Browse AI conversations",
            Self::Server => "Start server",
            Self::Discover => "Discover sources",
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let multi = crime_map_cli_utils::init_logger();

    println!("Crime Map Toolchain");
    println!();

    let labels: Vec<&str> = Tool::ALL.iter().map(Tool::label).collect();

    let idx = Select::new()
        .with_prompt("What would you like to do?")
        .items(&labels)
        .default(0)
        .interact()?;

    match Tool::ALL[idx] {
        Tool::RunPipeline => pipeline::run(&multi).await?,
        Tool::Ingest => crime_map_ingest::interactive::run(&multi).await?,
        Tool::Generate => crime_map_generate::interactive::run().await?,
        Tool::Conversations => crime_map_conversations::interactive::run().await?,
        Tool::Server => {
            // The server uses actix-web's runtime, so we need to run it
            // in a blocking task to avoid nesting tokio runtimes.
            tokio::task::spawn_blocking(|| {
                actix_web::rt::System::new().block_on(crime_map_server::interactive::run())
            })
            .await??;
        }
        Tool::Discover => crime_map_discover::interactive::run().await?,
    }

    Ok(())
}
