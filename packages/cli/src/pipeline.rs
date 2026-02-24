//! Full pipeline orchestrator for the crime map toolchain.
//!
//! Chains sync -> geocode -> generate in a single interactive flow,
//! prompting for sources, outputs, and optional advanced parameters.
//! Uses `indicatif` progress bars for real-time visual feedback.

use std::time::Instant;

use crime_map_cli_utils::{IndicatifProgress, MultiProgress};
use crime_map_database::{geocode_cache, source_db};
use dialoguer::{Confirm, Input, MultiSelect};

/// Steps available in the pipeline.
enum PipelineStep {
    Sync,
    Geocode,
}

impl PipelineStep {
    const ALL: &[Self] = &[Self::Sync, Self::Geocode];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::Sync => "Sync sources",
            Self::Geocode => "Geocode",
        }
    }
}

/// Advanced configuration for each pipeline step.
struct PipelineConfig {
    // Sync
    sync_force: bool,
    sync_limit: Option<u64>,

    // Geocode
    geocode_batch_size: u64,
    geocode_nominatim_only: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            sync_force: false,
            sync_limit: None,
            geocode_batch_size: 50_000,
            geocode_nominatim_only: false,
        }
    }
}

/// Runs the full pipeline orchestrator.
///
/// Prompts the user for pipeline steps, source selection, and optional
/// advanced configuration, then executes each step sequentially.
///
/// The `multi` parameter is the shared [`MultiProgress`] that is also
/// registered with the log bridge, so all `log::info!` output is
/// automatically suspended while progress bars redraw.
///
/// # Errors
///
/// Returns an error if database connection, user prompts, or any pipeline
/// step fails.
#[allow(clippy::too_many_lines, clippy::future_not_send)]
pub async fn run(multi: &MultiProgress) -> Result<(), Box<dyn std::error::Error>> {
    let pipeline_start = Instant::now();

    // --- 1. Select pipeline steps ---
    let step_labels: Vec<&str> = PipelineStep::ALL.iter().map(PipelineStep::label).collect();
    let defaults = vec![true; PipelineStep::ALL.len()];

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

    // --- 2. Source selection (for sync and geocode filtering) ---
    let all_sources = crime_map_source::registry::all_sources();
    let source_labels: Vec<String> = all_sources
        .iter()
        .map(|s| format!("{} \u{2014} {}", s.id(), s.name()))
        .collect();

    let selected_source_indices = if has_sync {
        let sel = MultiSelect::new()
            .with_prompt("Sources to sync (space=toggle, a=all, enter=confirm)")
            .items(&source_labels)
            .max_length(20)
            .interact()?;

        if sel.is_empty() {
            println!("No sources selected.");
            return Ok(());
        }
        sel
    } else {
        // If not syncing, all sources are implicitly in scope for geocode
        (0..all_sources.len()).collect()
    };

    // --- 3. Sync-specific prompts (always asked if syncing) ---
    let mut config = PipelineConfig::default();

    if has_sync {
        config.sync_force = Confirm::new()
            .with_prompt("Force full sync?")
            .default(false)
            .interact()?;

        config.sync_limit = prompt_optional_u64("Record limit per source (empty for no limit)")?;
    }

    // --- 4. Advanced options gate ---
    let advanced = Confirm::new()
        .with_prompt("Configure advanced options?")
        .default(false)
        .interact()?;

    if advanced && has_geocode {
        let batch_str: String = Input::new()
            .with_prompt("Geocode batch size")
            .default("50000".to_string())
            .interact_text()?;
        config.geocode_batch_size = batch_str.parse().unwrap_or(50_000);

        config.geocode_nominatim_only = Confirm::new()
            .with_prompt("Nominatim only (skip Census geocoder)?")
            .default(false)
            .interact()?;
    }

    // --- 5. Execute pipeline ---
    println!();
    log::info!("Starting pipeline ({} steps)...", selected_steps.len());

    let total_steps = selected_steps.len();
    let mut current_step = 0usize;

    // --- Sync ---
    if has_sync {
        current_step += 1;
        let num_sources = selected_source_indices.len();

        let source_bar = IndicatifProgress::steps_bar(
            multi,
            &format!("[{current_step}/{total_steps}] Sources"),
            num_sources as u64,
        );

        for (i, &idx) in selected_source_indices.iter().enumerate() {
            let src = &all_sources[idx];

            let fetch_bar = IndicatifProgress::records_bar(multi, src.name());

            source_bar.set_message(format!(
                "[{current_step}/{total_steps}] Source {}/{num_sources}: {}",
                i + 1,
                src.name()
            ));

            match source_db::open_by_id(src.id()) {
                Ok(conn) => {
                    let result = crime_map_ingest::sync_source(
                        &conn,
                        src,
                        config.sync_limit,
                        config.sync_force,
                        Some(fetch_bar.clone()),
                    )
                    .await;

                    fetch_bar.finish_and_clear();

                    if let Err(e) = result {
                        log::error!("Failed to sync {}: {e}", src.id());
                        if !ask_continue()? {
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    fetch_bar.finish_and_clear();
                    log::error!("Failed to open DB for {}: {e}", src.id());
                    if !ask_continue()? {
                        return Ok(());
                    }
                }
            }

            source_bar.inc(1);
        }

        source_bar.finish(format!(
            "[{current_step}/{total_steps}] Synced {num_sources} source(s)"
        ));
    }

    // --- Geocode ---
    if has_geocode {
        current_step += 1;
        log::info!("[{current_step}/{total_steps}] Geocoding...");

        let geocode_bar = IndicatifProgress::batch_bar(
            multi,
            &format!("[{current_step}/{total_steps}] Geocoding"),
        );

        let cache_conn = geocode_cache::open_default()?;
        let rt = tokio::runtime::Handle::current();

        let target_sources: Vec<_> = selected_source_indices
            .iter()
            .map(|&i| &all_sources[i])
            .collect();

        // Phase 1: Geocode missing coordinates
        for src in &target_sources {
            let source_conn = source_db::open_by_id(src.id())?;
            if let Err(e) = crime_map_ingest::geocode_missing(
                &source_conn,
                &cache_conn,
                config.geocode_batch_size,
                None,
                config.geocode_nominatim_only,
                Some(geocode_bar.clone()),
                &rt,
            ) {
                log::error!("Geocoding (missing coords) for {} failed: {e}", src.id());
                if !ask_continue()? {
                    return Ok(());
                }
            }
        }

        // Phase 2: Re-geocode imprecise sources
        let re_geocode_sources: Vec<_> = target_sources.iter().filter(|s| s.re_geocode()).collect();

        if !re_geocode_sources.is_empty() {
            log::info!(
                "Re-geocoding {} source(s) with imprecise coordinates...",
                re_geocode_sources.len()
            );
            for src in re_geocode_sources {
                let source_conn = source_db::open_by_id(src.id())?;
                if let Err(e) = crime_map_ingest::re_geocode_source(
                    &source_conn,
                    &cache_conn,
                    config.geocode_batch_size,
                    None,
                    config.geocode_nominatim_only,
                    Some(geocode_bar.clone()),
                    &rt,
                ) {
                    log::error!("Re-geocoding source {} failed: {e}", src.id());
                    if !ask_continue()? {
                        return Ok(());
                    }
                }
            }
        }

        geocode_bar.finish(format!("[{current_step}/{total_steps}] Geocoding complete"));
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

/// Prompts the user for an optional `u64` value.
///
/// Returns `None` if the input is empty.
fn prompt_optional_u64(prompt: &str) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let input: String = Input::new()
        .with_prompt(prompt)
        .allow_empty(true)
        .interact_text()?;

    if input.trim().is_empty() {
        Ok(None)
    } else {
        Ok(Some(input.trim().parse()?))
    }
}
