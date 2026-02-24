//! Interactive menu for Cloudflare R2 data sync operations.
//!
//! Provides pull (download) and push (upload) of per-source `DuckDB` files
//! and shared databases (`boundaries.duckdb`, `geocode_cache.duckdb`)
//! between the local `data/` directory and the `crime-map-data` R2 bucket.

use std::time::Instant;

use dialoguer::Select;

/// What to include in the R2 sync operation.
enum SyncScope {
    SourcesAndShared,
    SourcesOnly,
    SharedOnly,
}

impl SyncScope {
    const ALL: &[Self] = &[Self::SourcesAndShared, Self::SourcesOnly, Self::SharedOnly];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::SourcesAndShared => "Sources + shared databases",
            Self::SourcesOnly => "Sources only",
            Self::SharedOnly => "Shared databases only",
        }
    }

    #[must_use]
    const fn include_sources(&self) -> bool {
        matches!(self, Self::SourcesAndShared | Self::SourcesOnly)
    }

    #[must_use]
    const fn include_shared(&self) -> bool {
        matches!(self, Self::SourcesAndShared | Self::SharedOnly)
    }
}

/// Runs the interactive R2 sync menu.
///
/// Prompts the user for direction (pull/push), scope (sources/shared/both),
/// and source selection. Handles missing R2 credentials gracefully.
///
/// # Errors
///
/// Returns an error if the R2 client cannot be created or the sync fails.
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // Direction
    let direction_labels = ["Pull from R2 (download)", "Push to R2 (upload)"];
    let direction = Select::new()
        .with_prompt("R2 sync direction")
        .items(direction_labels)
        .default(0)
        .interact()?;
    let is_pull = direction == 0;

    // Scope
    let scope_labels: Vec<&str> = SyncScope::ALL.iter().map(SyncScope::label).collect();
    let scope_idx = Select::new()
        .with_prompt("What to sync?")
        .items(&scope_labels)
        .default(0)
        .interact()?;
    let scope = &SyncScope::ALL[scope_idx];

    // Source selection (if sources are included)
    let source_ids: Vec<String> = if scope.include_sources() {
        let ids = crime_map_cli_utils::prompt_source_multiselect(
            "Sources (space=toggle, a=all, enter=confirm)",
        )?;
        if ids.is_empty() {
            println!("No sources selected.");
            return Ok(());
        }
        ids
    } else {
        Vec::new()
    };

    // Create R2 client
    let r2 = match crime_map_r2::R2Client::from_env() {
        Ok(client) => client,
        Err(e) => {
            log::error!("R2 not configured: {e}");
            log::error!(
                "Required env vars: CLOUDFLARE_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY"
            );
            return Ok(());
        }
    };

    // Execute
    let start = Instant::now();
    let mut total = 0u64;
    let verb = if is_pull { "Pull" } else { "Push" };

    if scope.include_sources() {
        log::info!("{verb}: syncing {} source(s)...", source_ids.len());
        total += if is_pull {
            r2.pull_sources(&source_ids).await?
        } else {
            r2.push_sources(&source_ids).await?
        };
    }

    if scope.include_shared() {
        log::info!("{verb}: syncing shared databases...");
        total += if is_pull {
            r2.pull_shared().await?
        } else {
            r2.push_shared().await?
        };
    }

    let elapsed = start.elapsed();
    log::info!(
        "{verb} complete: {total} file(s) in {:.1}s",
        elapsed.as_secs_f64()
    );

    Ok(())
}
