#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Interactive CLI orchestrator for the crime map toolchain.
//!
//! Provides a unified entry point (`cargo crime-map`) that lets users
//! interactively select which tool to run (ingest, generate, server,
//! discover) and guides them through the configuration for each.

use dialoguer::Select;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();

    println!("Crime Map Toolchain");
    println!();

    let tools = &[
        "Ingest data",
        "Generate tiles & databases",
        "Start server",
        "Discover sources",
    ];

    let selection = Select::new()
        .with_prompt("What would you like to do?")
        .items(tools)
        .default(0)
        .interact()?;

    match selection {
        0 => crime_map_ingest::interactive::run().await?,
        1 => crime_map_generate::interactive::run().await?,
        2 => {
            // The server uses actix-web's runtime, so we need to run it
            // in a blocking task to avoid nesting tokio runtimes.
            tokio::task::spawn_blocking(|| {
                actix_web::rt::System::new().block_on(crime_map_server::interactive::run())
            })
            .await??;
        }
        3 => crime_map_discover::interactive::run().await?,
        _ => unreachable!(),
    }

    Ok(())
}
