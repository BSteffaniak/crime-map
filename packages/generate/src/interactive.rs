//! Interactive menu for the generate tool.
//!
//! Provides a terminal-based UI using `dialoguer` that lets users select
//! which outputs to generate and configure parameters without memorizing
//! CLI flags.

use dialoguer::{Confirm, Input, Select};

use crate::{
    GenerateArgs, OUTPUT_CLUSTERS_PMTILES, OUTPUT_COUNT_DB, OUTPUT_INCIDENTS_DB,
    OUTPUT_INCIDENTS_PMTILES, output_dir, resolve_source_ids, run_with_cache,
};
use crime_map_database::db;

/// Runs the interactive generation menu.
///
/// Connects to the database, presents a selection menu for output types,
/// prompts for generation parameters, and executes the chosen pipeline.
///
/// # Errors
///
/// Returns an error if the database connection, user input, or generation
/// pipeline fails.
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let db = db::connect_from_env().await?;
    let dir = output_dir();
    std::fs::create_dir_all(&dir)?;

    let choices = &[
        "Generate all outputs",
        "Generate PMTiles (heatmap + points)",
        "Generate cluster tiles",
        "Generate sidebar SQLite",
        "Generate count DuckDB",
    ];

    let selection = Select::new()
        .with_prompt("What would you like to generate?")
        .items(choices)
        .default(0)
        .interact()?;

    let requested_outputs: &[&str] = match selection {
        0 => &[
            OUTPUT_INCIDENTS_PMTILES,
            OUTPUT_CLUSTERS_PMTILES,
            OUTPUT_INCIDENTS_DB,
            OUTPUT_COUNT_DB,
        ],
        1 => &[OUTPUT_INCIDENTS_PMTILES],
        2 => &[OUTPUT_CLUSTERS_PMTILES],
        3 => &[OUTPUT_INCIDENTS_DB],
        4 => &[OUTPUT_COUNT_DB],
        _ => unreachable!(),
    };

    let limit_str: String = Input::new()
        .with_prompt("Record limit (leave empty for unlimited)")
        .allow_empty(true)
        .interact_text()?;

    let limit: Option<u64> = if limit_str.trim().is_empty() {
        None
    } else {
        Some(
            limit_str
                .trim()
                .parse()
                .map_err(|e| format!("Invalid limit '{limit_str}': {e}"))?,
        )
    };

    let sources_str: String = Input::new()
        .with_prompt("Source IDs to include (comma-separated, leave empty for all)")
        .allow_empty(true)
        .interact_text()?;

    let sources: Option<String> = if sources_str.trim().is_empty() {
        None
    } else {
        Some(sources_str.trim().to_string())
    };

    let keep_intermediate = Confirm::new()
        .with_prompt("Keep intermediate files?")
        .default(false)
        .interact()?;

    let force = Confirm::new()
        .with_prompt("Force regeneration?")
        .default(false)
        .interact()?;

    let args = GenerateArgs {
        limit,
        sources,
        keep_intermediate,
        force,
    };

    let source_ids = resolve_source_ids(db.as_ref(), &args).await?;
    run_with_cache(db.as_ref(), &args, &source_ids, &dir, requested_outputs).await?;

    Ok(())
}
