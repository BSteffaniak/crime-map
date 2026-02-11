//! Full pipeline orchestrator for the crime map toolchain.
//!
//! Chains sync → geocode → attribute → generate in a single interactive
//! flow, prompting for sources, outputs, and optional advanced parameters.

use std::time::Instant;

use dialoguer::{Confirm, Input, MultiSelect, Select};

/// Steps available in the pipeline.
enum PipelineStep {
    Sync,
    Geocode,
    Attribute,
    Generate,
}

impl PipelineStep {
    const ALL: &[Self] = &[Self::Sync, Self::Geocode, Self::Attribute, Self::Generate];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::Sync => "Sync sources",
            Self::Geocode => "Geocode",
            Self::Attribute => "Attribute census data",
            Self::Generate => "Generate tiles & databases",
        }
    }
}

/// Attribution mode for the attribute step.
enum AttributeMode {
    Both,
    PlacesOnly,
    TractsOnly,
}

impl AttributeMode {
    const ALL: &[Self] = &[Self::Both, Self::PlacesOnly, Self::TractsOnly];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::Both => "Both places and tracts",
            Self::PlacesOnly => "Places only",
            Self::TractsOnly => "Tracts only",
        }
    }
}

/// Advanced configuration for each pipeline step.
#[allow(clippy::struct_excessive_bools)]
struct PipelineConfig {
    // Sync
    sync_force: bool,
    sync_limit: Option<u64>,

    // Geocode
    geocode_batch_size: u64,
    geocode_nominatim_only: bool,

    // Attribute
    attribute_buffer: f64,
    attribute_batch_size: u32,
    attribute_mode: usize, // index into AttributeMode::ALL

    // Generate
    generate_limit: Option<u64>,
    generate_keep_intermediate: bool,
    generate_force: bool,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            sync_force: false,
            sync_limit: None,
            geocode_batch_size: 50_000,
            geocode_nominatim_only: false,
            attribute_buffer: 5.0,
            attribute_batch_size: 5000,
            attribute_mode: 0, // Both
            generate_limit: None,
            generate_keep_intermediate: false,
            generate_force: false,
        }
    }
}

/// Runs the full pipeline orchestrator.
///
/// Prompts the user for pipeline steps, source selection, output selection,
/// and optional advanced configuration, then executes each step
/// sequentially.
///
/// # Errors
///
/// Returns an error if database connection, user prompts, or any pipeline
/// step fails.
#[allow(clippy::too_many_lines, clippy::future_not_send)]
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
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
    let has_attribute = selected_steps
        .iter()
        .any(|&i| matches!(PipelineStep::ALL[i], PipelineStep::Attribute));
    let has_generate = selected_steps
        .iter()
        .any(|&i| matches!(PipelineStep::ALL[i], PipelineStep::Generate));

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
        // If not syncing, all sources are implicitly in scope for geocode/attribute
        (0..all_sources.len()).collect()
    };

    // --- 3. Generate output selection ---
    let generate_outputs = if has_generate {
        let output_choices: &[(&str, &str)] = &[
            (
                "PMTiles (heatmap + points)",
                crime_map_generate::OUTPUT_INCIDENTS_PMTILES,
            ),
            ("Cluster tiles", crime_map_generate::OUTPUT_CLUSTERS_PMTILES),
            ("Sidebar SQLite", crime_map_generate::OUTPUT_INCIDENTS_DB),
            ("Count DuckDB", crime_map_generate::OUTPUT_COUNT_DB),
        ];

        let output_labels: Vec<&str> = output_choices.iter().map(|(l, _)| *l).collect();
        let output_defaults = vec![true; output_choices.len()];

        let selected = MultiSelect::new()
            .with_prompt("Outputs to generate (space=toggle, a=all, enter=confirm)")
            .items(&output_labels)
            .defaults(&output_defaults)
            .interact()?;

        if selected.is_empty() {
            println!("No outputs selected.");
            return Ok(());
        }

        selected
            .iter()
            .map(|&i| output_choices[i].1.to_string())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    // --- 4. Sync-specific prompts (always asked if syncing) ---
    let mut config = PipelineConfig::default();

    if has_sync {
        config.sync_force = Confirm::new()
            .with_prompt("Force full sync?")
            .default(false)
            .interact()?;

        config.sync_limit = prompt_optional_u64("Record limit per source (empty for no limit)")?;
    }

    // --- 5. Advanced options gate ---
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
            config.geocode_batch_size = batch_str.parse().unwrap_or(50_000);

            config.geocode_nominatim_only = Confirm::new()
                .with_prompt("Nominatim only (skip Census geocoder)?")
                .default(false)
                .interact()?;
        }

        if has_attribute {
            let buffer_str: String = Input::new()
                .with_prompt("Attribute buffer distance (meters)")
                .default("5".to_string())
                .interact_text()?;
            config.attribute_buffer = buffer_str.parse().unwrap_or(5.0);

            let batch_str: String = Input::new()
                .with_prompt("Attribute batch size")
                .default("5000".to_string())
                .interact_text()?;
            config.attribute_batch_size = batch_str.parse().unwrap_or(5000);

            let mode_labels: Vec<&str> = AttributeMode::ALL
                .iter()
                .map(AttributeMode::label)
                .collect();
            config.attribute_mode = Select::new()
                .with_prompt("What to attribute")
                .items(&mode_labels)
                .default(0)
                .interact()?;
        }

        if has_generate {
            config.generate_limit =
                prompt_optional_u64("Generate record limit (empty for unlimited)")?;

            config.generate_keep_intermediate = Confirm::new()
                .with_prompt("Keep intermediate files?")
                .default(false)
                .interact()?;

            config.generate_force = Confirm::new()
                .with_prompt("Force regeneration?")
                .default(false)
                .interact()?;
        }
    }

    // --- 6. Execute pipeline ---
    println!();
    log::info!("Starting pipeline ({} steps)...", selected_steps.len());

    let db = crime_map_database::db::connect_from_env().await?;
    crime_map_database::run_migrations(db.as_ref()).await?;

    let total_steps = selected_steps.len();
    let mut current_step = 0usize;

    // --- Sync ---
    if has_sync {
        current_step += 1;
        log::info!(
            "[{current_step}/{total_steps}] Syncing {} source(s)...",
            selected_source_indices.len()
        );

        for &idx in &selected_source_indices {
            let src = &all_sources[idx];
            if let Err(e) = crime_map_ingest::sync_source(
                db.as_ref(),
                src,
                config.sync_limit,
                config.sync_force,
            )
            .await
            {
                log::error!("Failed to sync {}: {e}", src.id());
                if !ask_continue()? {
                    return Ok(());
                }
            }
        }
    }

    // --- Geocode ---
    if has_geocode {
        current_step += 1;
        log::info!("[{current_step}/{total_steps}] Geocoding...");

        // Phase 1: Geocode missing coordinates
        if let Err(e) = crime_map_ingest::geocode_missing(
            db.as_ref(),
            config.geocode_batch_size,
            None,
            config.geocode_nominatim_only,
            None,
        )
        .await
        {
            log::error!("Geocoding (missing coords) failed: {e}");
            if !ask_continue()? {
                return Ok(());
            }
        }

        // Phase 2: Re-geocode imprecise sources
        match crime_map_ingest::resolve_re_geocode_source_ids(db.as_ref(), None).await {
            Ok(re_geocode_ids) => {
                if !re_geocode_ids.is_empty() {
                    log::info!(
                        "Re-geocoding {} source(s) with imprecise coordinates...",
                        re_geocode_ids.len()
                    );
                    for sid in &re_geocode_ids {
                        if let Err(e) = crime_map_ingest::re_geocode_source(
                            db.as_ref(),
                            config.geocode_batch_size,
                            None,
                            config.geocode_nominatim_only,
                            Some(*sid),
                        )
                        .await
                        {
                            log::error!("Re-geocoding source {sid} failed: {e}");
                            if !ask_continue()? {
                                return Ok(());
                            }
                        }
                    }
                }
            }
            Err(e) => {
                log::error!("Failed to resolve re-geocode sources: {e}");
                if !ask_continue()? {
                    return Ok(());
                }
            }
        }
    }

    // --- Attribute ---
    if has_attribute {
        current_step += 1;
        log::info!("[{current_step}/{total_steps}] Attributing census data...");

        let (places_only, tracts_only) = match AttributeMode::ALL[config.attribute_mode] {
            AttributeMode::Both => (false, false),
            AttributeMode::PlacesOnly => (true, false),
            AttributeMode::TractsOnly => (false, true),
        };

        if !tracts_only
            && let Err(e) = crime_map_database::queries::attribute_places(
                db.as_ref(),
                config.attribute_buffer,
                config.attribute_batch_size,
            )
            .await
        {
            log::error!("Place attribution failed: {e}");
            if !ask_continue()? {
                return Ok(());
            }
        }

        if !places_only
            && let Err(e) = crime_map_database::queries::attribute_tracts(
                db.as_ref(),
                config.attribute_batch_size,
            )
            .await
        {
            log::error!("Tract attribution failed: {e}");
            if !ask_continue()? {
                return Ok(());
            }
        }
    }

    // --- Generate ---
    if has_generate {
        current_step += 1;
        log::info!("[{current_step}/{total_steps}] Generating outputs...");

        let dir = crime_map_generate::output_dir();
        std::fs::create_dir_all(&dir)?;

        let args = crime_map_generate::GenerateArgs {
            limit: config.generate_limit,
            sources: None,
            keep_intermediate: config.generate_keep_intermediate,
            force: config.generate_force,
        };

        let source_ids = crime_map_generate::resolve_source_ids(db.as_ref(), &args).await?;
        let output_refs: Vec<&str> = generate_outputs.iter().map(String::as_str).collect();

        if let Err(e) =
            crime_map_generate::run_with_cache(db.as_ref(), &args, &source_ids, &dir, &output_refs)
                .await
        {
            log::error!("Generation failed: {e}");
            if !ask_continue()? {
                return Ok(());
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
