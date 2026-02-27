//! Full pipeline orchestrator for the crime map toolchain.
//!
//! Chains sync -> geocode -> generate in a single interactive flow,
//! prompting for sources, outputs, and optional advanced parameters.
//! Uses `indicatif` progress bars for real-time visual feedback.
//!
//! All orchestration logic is delegated to the shared [`crime_map_ingest`]
//! and [`crime_map_generate`] library functions so that capabilities
//! stay in sync with the individual CLI tools automatically.

use std::time::Instant;

use crime_map_cli_utils::{IndicatifProgress, MultiProgress};
use crime_map_generate::{
    GenerateArgs, OUTPUT_ANALYTICS_DB, OUTPUT_BOUNDARIES_DB, OUTPUT_BOUNDARIES_PMTILES,
    OUTPUT_COUNT_DB, OUTPUT_H3_DB, OUTPUT_INCIDENTS_DB, OUTPUT_INCIDENTS_PMTILES, OUTPUT_METADATA,
};
use crime_map_ingest::{EnrichArgs, GeocodeArgs, IngestBoundariesArgs, SyncArgs};
use dialoguer::{Confirm, Input, MultiSelect, Select};

/// Steps available in the pipeline.
enum PipelineStep {
    Sync,
    Geocode,
    Enrich,
    IngestBoundaries,
    Generate,
    R2Pull,
    R2Push,
}

impl PipelineStep {
    const ALL: &[Self] = &[
        Self::Sync,
        Self::Geocode,
        Self::Enrich,
        Self::IngestBoundaries,
        Self::Generate,
        Self::R2Pull,
        Self::R2Push,
    ];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::Sync => "Sync sources",
            Self::Geocode => "Geocode",
            Self::Enrich => "Enrich spatial attribution",
            Self::IngestBoundaries => "Ingest boundaries (tracts, places, counties, states)",
            Self::Generate => "Generate tiles & databases",
            Self::R2Pull => "Pull from R2 (before sync)",
            Self::R2Push => "Push to R2 (after pipeline)",
        }
    }

    /// Whether this step is enabled by default in the multi-select.
    #[must_use]
    const fn default_enabled(&self) -> bool {
        match self {
            Self::Sync | Self::Geocode | Self::Enrich | Self::IngestBoundaries | Self::Generate => {
                true
            }
            Self::R2Pull | Self::R2Push => false,
        }
    }
}

/// Runs the full pipeline orchestrator.
///
/// Prompts the user for pipeline steps, source selection, and optional
/// advanced configuration, then executes each step sequentially using
/// the shared library orchestration functions.
///
/// # Errors
///
/// Returns an error if user prompts or any pipeline step fails.
#[allow(clippy::too_many_lines, clippy::future_not_send)]
pub async fn run(multi: &MultiProgress) -> Result<(), Box<dyn std::error::Error>> {
    let pipeline_start = Instant::now();

    // --- 1. Select pipeline steps ---
    let step_labels: Vec<&str> = PipelineStep::ALL.iter().map(PipelineStep::label).collect();
    let defaults: Vec<bool> = PipelineStep::ALL
        .iter()
        .map(PipelineStep::default_enabled)
        .collect();

    let selected_steps = MultiSelect::new()
        .with_prompt("Pipeline steps (space=toggle, a=all, enter=confirm)")
        .items(&step_labels)
        .defaults(&defaults)
        .interact()?;

    if selected_steps.is_empty() {
        println!("No steps selected.");
        return Ok(());
    }

    let has_sync = selected_steps
        .iter()
        .any(|&i| matches!(PipelineStep::ALL[i], PipelineStep::Sync));
    let has_geocode = selected_steps
        .iter()
        .any(|&i| matches!(PipelineStep::ALL[i], PipelineStep::Geocode));
    let has_enrich = selected_steps
        .iter()
        .any(|&i| matches!(PipelineStep::ALL[i], PipelineStep::Enrich));
    let has_ingest_boundaries = selected_steps
        .iter()
        .any(|&i| matches!(PipelineStep::ALL[i], PipelineStep::IngestBoundaries));
    let has_generate = selected_steps
        .iter()
        .any(|&i| matches!(PipelineStep::ALL[i], PipelineStep::Generate));
    let has_r2_pull = selected_steps
        .iter()
        .any(|&i| matches!(PipelineStep::ALL[i], PipelineStep::R2Pull));
    let has_r2_push = selected_steps
        .iter()
        .any(|&i| matches!(PipelineStep::ALL[i], PipelineStep::R2Push));

    // --- 2. Source selection (shared across all steps) ---
    let source_ids = crime_map_cli_utils::prompt_source_multiselect(
        "Sources (space=toggle, a=all, enter=confirm)",
    )?;

    if source_ids.is_empty() {
        println!("No sources selected.");
        return Ok(());
    }

    // --- 3. Sync options ---
    let (sync_force, sync_limit) = if has_sync {
        let force = Confirm::new()
            .with_prompt("Force full sync?")
            .default(false)
            .interact()?;
        let limit = crime_map_cli_utils::prompt_optional_u64(
            "Record limit per source (empty for no limit)",
        )?;
        (force, limit)
    } else {
        (false, None)
    };

    // --- 4. Boundary ingestion scope ---
    let boundary_state_fips: Vec<String> = if has_ingest_boundaries {
        let scope = Select::new()
            .with_prompt("Boundary ingestion scope")
            .items([
                "All states (50 + DC)",
                "Only states with active sources",
                "Specific state FIPS codes",
            ])
            .default(1)
            .interact()?;

        match scope {
            0 => Vec::new(), // empty = all states
            1 => {
                // Derive state FIPS from the selected source definitions
                let all_defs = crime_map_ingest::all_sources();
                let selected_defs: Vec<_> = all_defs
                    .iter()
                    .filter(|s| source_ids.contains(&s.id().to_string()))
                    .collect();
                let mut fips: std::collections::BTreeSet<String> =
                    std::collections::BTreeSet::new();
                for def in &selected_defs {
                    if let Some(code) = crime_map_geography_models::fips::abbr_to_fips(&def.state) {
                        fips.insert(code.to_string());
                    }
                }
                if fips.is_empty() {
                    log::warn!(
                        "No state FIPS codes derived from selected sources, falling back to all"
                    );
                }
                fips.into_iter().collect()
            }
            _ => {
                let input: String = Input::new()
                    .with_prompt("Comma-separated state FIPS codes (e.g., 11,24,06)")
                    .interact_text()?;
                input
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            }
        }
    } else {
        Vec::new()
    };

    // --- 5. Advanced options gate ---
    let mut geocode_batch_size = 50_000u64;
    let mut geocode_nominatim_only = false;
    let mut generate_force = false;
    let mut boundary_force = false;
    let mut r2_pull_shared = true; // default: pull shared before sync
    let mut r2_push_shared = false; // default: don't push shared (unchanged by pipeline)

    let advanced = Confirm::new()
        .with_prompt("Configure advanced options?")
        .default(false)
        .interact()?;

    if advanced {
        if has_geocode {
            let batch_str: String = Input::new()
                .with_prompt("Geocode batch size")
                .default("50000".to_string())
                .interact_text()?;
            geocode_batch_size = batch_str.parse().unwrap_or(50_000);

            geocode_nominatim_only = Confirm::new()
                .with_prompt("Nominatim only (skip Census geocoder)?")
                .default(false)
                .interact()?;
        }

        if has_generate {
            generate_force = Confirm::new()
                .with_prompt("Force regeneration?")
                .default(false)
                .interact()?;
        }

        if has_ingest_boundaries {
            boundary_force = Confirm::new()
                .with_prompt("Force boundary re-import (even if already populated)?")
                .default(false)
                .interact()?;
        }

        if has_r2_pull {
            r2_pull_shared = Confirm::new()
                .with_prompt("R2 pull: include shared databases?")
                .default(true)
                .interact()?;
        }

        if has_r2_push {
            r2_push_shared = Confirm::new()
                .with_prompt("R2 push: include shared databases?")
                .default(false)
                .interact()?;
        }
    }

    // --- 5. Execute pipeline ---
    println!();
    let total_steps = selected_steps.len();
    let mut current_step = 0usize;

    log::info!("Starting pipeline ({total_steps} steps)...");

    // --- R2 Pull (before sync) ---
    if has_r2_pull {
        current_step += 1;
        log::info!("[{current_step}/{total_steps}] Pulling from R2...");

        match crime_map_r2::R2Client::from_env() {
            Ok(r2) => {
                let mut stats = crime_map_r2::SyncStats::default();
                stats.merge(r2.pull_sources(&source_ids).await?);
                if r2_pull_shared {
                    stats.merge(r2.pull_shared().await?);
                }
                log::info!("[{current_step}/{total_steps}] R2 pull complete: {stats}");
            }
            Err(e) => {
                log::warn!("R2 not configured: {e}");
                if !ask_continue()? {
                    return Ok(());
                }
            }
        }
    }

    // --- Sync ---
    if has_sync {
        current_step += 1;
        let source_bar = IndicatifProgress::steps_bar(
            multi,
            &format!("[{current_step}/{total_steps}] Sources"),
            source_ids.len() as u64,
        );

        let args = SyncArgs {
            source_ids: source_ids.clone(),
            limit: sync_limit,
            force: sync_force,
        };

        let result = crime_map_ingest::run_sync(&args, Some(&source_bar)).await;
        source_bar.finish(format!(
            "[{current_step}/{total_steps}] Synced {} source(s)",
            source_ids.len()
        ));

        if !result.failed.is_empty() {
            log::error!(
                "{} source(s) failed: {}",
                result.failed.len(),
                result.failed.join(", ")
            );
            if !ask_continue()? {
                return Ok(());
            }
        }
    }

    // --- Geocode ---
    if has_geocode {
        current_step += 1;
        let geocode_bar = IndicatifProgress::batch_bar(
            multi,
            &format!("[{current_step}/{total_steps}] Geocoding"),
        );

        let args = GeocodeArgs {
            source_ids: source_ids.clone(),
            batch_size: geocode_batch_size,
            limit: None,
            nominatim_only: geocode_nominatim_only,
        };

        match crime_map_ingest::run_geocode(&args, Some(geocode_bar.clone())).await {
            Ok(result) => {
                geocode_bar.finish(format!(
                    "[{current_step}/{total_steps}] Geocoded {} incidents",
                    result.total()
                ));
            }
            Err(e) => {
                geocode_bar.finish(format!("[{current_step}/{total_steps}] Geocoding failed"));
                log::error!("Geocoding failed: {e}");
                if !ask_continue()? {
                    return Ok(());
                }
            }
        }
    }

    // --- Enrich ---
    if has_enrich {
        current_step += 1;
        let enrich_bar = IndicatifProgress::batch_bar(
            multi,
            &format!("[{current_step}/{total_steps}] Enriching"),
        );

        let args = EnrichArgs {
            source_ids: source_ids.clone(),
            force: false,
        };

        match crime_map_ingest::run_enrich(&args, Some(enrich_bar.clone())) {
            Ok(result) => {
                enrich_bar.finish(format!(
                    "[{current_step}/{total_steps}] Enriched {} incidents",
                    result.enriched
                ));
            }
            Err(e) => {
                enrich_bar.finish(format!("[{current_step}/{total_steps}] Enrichment failed"));
                log::error!("Enrichment failed: {e}");
                if !ask_continue()? {
                    return Ok(());
                }
            }
        }
    }

    // --- Ingest Boundaries ---
    if has_ingest_boundaries {
        current_step += 1;
        log::info!("[{current_step}/{total_steps}] Ingesting boundaries...");

        let args = IngestBoundariesArgs {
            state_fips: boundary_state_fips,
            force: boundary_force,
        };

        match crime_map_ingest::run_ingest_boundaries(&args).await {
            Ok(result) => {
                let total = result.tracts
                    + result.places
                    + result.counties
                    + result.states
                    + result.neighborhoods;
                log::info!(
                    "[{current_step}/{total_steps}] Boundary ingestion complete: {total} total \
                     (tracts={}, places={}, counties={}, states={}, neighborhoods={})",
                    result.tracts,
                    result.places,
                    result.counties,
                    result.states,
                    result.neighborhoods,
                );
            }
            Err(e) => {
                log::error!("Boundary ingestion failed: {e}");
                if !ask_continue()? {
                    return Ok(());
                }
            }
        }
    }

    // --- Pre-generate boundary check ---
    if has_generate && !has_ingest_boundaries {
        // Warn if boundaries.duckdb is empty and the user didn't run boundary ingestion
        match crime_map_ingest::boundary_tract_count() {
            Ok(0) => {
                log::warn!(
                    "WARNING: boundaries.duckdb has 0 census tracts. \
                     Boundary counts and fills will be empty after generation. \
                     Consider running boundary ingestion first."
                );
            }
            Ok(n) => {
                log::info!("Boundary check: {n} census tracts available for spatial lookups");
            }
            Err(e) => {
                log::warn!("Could not check boundary data: {e}");
            }
        }
    }

    // --- Generate ---
    if has_generate {
        current_step += 1;
        log::info!("[{current_step}/{total_steps}] Generating tiles & databases...");

        let args = GenerateArgs {
            limit: None,
            sources: Some(source_ids.join(",")),
            states: None,
            keep_intermediate: false,
            force: generate_force,
        };

        let dir = crime_map_generate::output_dir();
        std::fs::create_dir_all(&dir)?;

        let all_outputs: &[&str] = &[
            OUTPUT_INCIDENTS_PMTILES,
            OUTPUT_INCIDENTS_DB,
            OUTPUT_COUNT_DB,
            OUTPUT_H3_DB,
            OUTPUT_METADATA,
            OUTPUT_BOUNDARIES_PMTILES,
            OUTPUT_BOUNDARIES_DB,
            OUTPUT_ANALYTICS_DB,
        ];

        let resolved = crime_map_generate::resolve_source_ids(&args)?;
        match crime_map_generate::run_with_cache(&args, &resolved, &dir, all_outputs, None).await {
            Ok(()) => {
                log::info!("[{current_step}/{total_steps}] Generation complete");
            }
            Err(e) => {
                log::error!("Generation failed: {e}");
                if !ask_continue()? {
                    return Ok(());
                }
            }
        }
    }

    // --- R2 Push (after pipeline) ---
    if has_r2_push {
        current_step += 1;
        log::info!("[{current_step}/{total_steps}] Pushing to R2...");

        match crime_map_r2::R2Client::from_env() {
            Ok(r2) => {
                let mut stats = crime_map_r2::SyncStats::default();
                stats.merge(r2.push_sources(&source_ids).await?);
                if r2_push_shared {
                    stats.merge(r2.push_shared().await?);
                }
                log::info!("[{current_step}/{total_steps}] R2 push complete: {stats}");
            }
            Err(e) => {
                log::warn!("R2 not configured: {e}");
                if !ask_continue()? {
                    return Ok(());
                }
            }
        }
    }

    let elapsed = pipeline_start.elapsed();
    log::info!("Pipeline complete in {:.1}s", elapsed.as_secs_f64());

    Ok(())
}

/// Asks the user whether to continue after an error.
fn ask_continue() -> Result<bool, Box<dyn std::error::Error>> {
    Ok(Confirm::new()
        .with_prompt("Continue to next step?")
        .default(true)
        .interact()?)
}
