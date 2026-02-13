//! Interactive menu for the generate tool.
//!
//! Provides a terminal-based UI using `dialoguer` that lets users select
//! which outputs to generate and configure parameters without memorizing
//! CLI flags.

use dialoguer::{Confirm, Input, MultiSelect};

use crate::{
    GenerateArgs, OUTPUT_CLUSTERS_PMTILES, OUTPUT_COUNT_DB, OUTPUT_INCIDENTS_DB,
    OUTPUT_INCIDENTS_PMTILES, output_dir, resolve_source_ids, run_with_cache,
};
use crime_map_database::db;

/// All available output types, paired with their internal constant name.
const OUTPUT_CHOICES: &[(&str, &str)] = &[
    ("PMTiles (heatmap + points)", OUTPUT_INCIDENTS_PMTILES),
    ("Cluster tiles", OUTPUT_CLUSTERS_PMTILES),
    ("Sidebar SQLite", OUTPUT_INCIDENTS_DB),
    ("Count DuckDB", OUTPUT_COUNT_DB),
];

/// Runs the interactive generation menu.
///
/// Connects to the database, presents a multi-select for output types and
/// source filtering, prompts for generation parameters, and executes the
/// chosen pipeline.
///
/// # Errors
///
/// Returns an error if the database connection, user input, or generation
/// pipeline fails.
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let db = db::connect_from_env().await?;
    let dir = output_dir();
    std::fs::create_dir_all(&dir)?;

    // --- Output type selection (multi-select, all checked by default) ---
    let output_labels: Vec<&str> = OUTPUT_CHOICES.iter().map(|(label, _)| *label).collect();
    let selected_outputs = MultiSelect::new()
        .with_prompt("Outputs to generate (space=toggle, a=all, enter=confirm)")
        .items(&output_labels)
        .interact()?;

    if selected_outputs.is_empty() {
        println!("No outputs selected.");
        return Ok(());
    }

    let requested_outputs: Vec<&str> = selected_outputs
        .iter()
        .map(|&i| OUTPUT_CHOICES[i].1)
        .collect();

    // --- Record limit ---
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

    // --- Source filter (multi-select from all configured sources) ---
    let all_sources = crime_map_source::registry::all_sources();
    let source_labels: Vec<String> = all_sources
        .iter()
        .map(|s| format!("{} \u{2014} {}", s.id(), s.name()))
        .collect();

    let selected_sources = MultiSelect::new()
        .with_prompt("Sources to include (space=toggle, a=all, enter=confirm)")
        .items(&source_labels)
        .max_length(20)
        .interact()?;

    let sources: Option<String> = if selected_sources.len() == all_sources.len() {
        None // all selected = no filter
    } else if selected_sources.is_empty() {
        println!("No sources selected.");
        return Ok(());
    } else {
        Some(
            selected_sources
                .iter()
                .map(|&i| all_sources[i].id().to_string())
                .collect::<Vec<_>>()
                .join(","),
        )
    };

    // --- Other options ---
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
    run_with_cache(
        db.as_ref(),
        &args,
        &source_ids,
        &dir,
        &requested_outputs,
        None,
    )
    .await?;

    Ok(())
}
