//! Interactive TUI for the discovery tool.
//!
//! Presents a menu-driven interface using `dialoguer` so operators can
//! browse and manage leads, sources, search history, legal records, and
//! scrape targets without memorising CLI flags.

use dialoguer::{Confirm, Input, Select};
use switchy_database::Database;

use crate::{LeadAction, LegalAction, ScrapeAction, SearchLogAction, SourceAction};

/// Default path for the discovery `SQLite` database.
const DEFAULT_DB_PATH: &str = "data/discovery.db";

/// Runs the interactive discovery menu loop.
///
/// Opens the discovery database at the default path and presents a
/// menu-driven interface.
///
/// # Errors
///
/// Returns an error if a database operation or I/O prompt fails.
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = std::path::PathBuf::from(DEFAULT_DB_PATH);
    let database = crate::db::open_db(&db_path).await?;

    run_with_db(database.as_ref()).await
}

/// Runs the interactive discovery menu loop with an existing database
/// connection.
///
/// # Errors
///
/// Returns an error if a database operation or I/O prompt fails.
pub async fn run_with_db(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        println!();
        let items = &[
            "Show status dashboard",
            "Manage leads",
            "Manage sources",
            "Search history",
            "Legal/licensing info",
            "Scrape targets",
            "Seed database",
            "Integrate lead into pipeline",
            "Health-check sources",
            "Suggest next actions",
            "Exit",
        ];

        let selection = Select::new()
            .with_prompt("Discovery tool")
            .items(items)
            .default(0)
            .interact()?;

        match selection {
            0 => crate::cmd_status(database).await?,
            1 => handle_leads(database).await?,
            2 => handle_sources(database).await?,
            3 => handle_search_log(database).await?,
            4 => handle_legal(database).await?,
            5 => handle_scrape(database).await?,
            6 => crate::cmd_seed(database).await?,
            7 => handle_integrate(database).await?,
            8 => handle_verify(database).await?,
            9 => handle_suggest(database).await?,
            10 => {
                println!("Goodbye.");
                return Ok(());
            }
            _ => unreachable!(),
        }
    }
}

// ---------------------------------------------------------------------------
// Leads
// ---------------------------------------------------------------------------

/// Interactive lead management sub-menu.
async fn handle_leads(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let items = &["List", "Add", "Update", "Investigate", "Back"];
    let selection = Select::new()
        .with_prompt("Leads")
        .items(items)
        .default(0)
        .interact()?;

    match selection {
        0 => {
            let status: String = Input::new()
                .with_prompt("Filter by status (blank for all)")
                .allow_empty(true)
                .interact_text()?;
            let api_type: String = Input::new()
                .with_prompt("Filter by API type (blank for all)")
                .allow_empty(true)
                .interact_text()?;

            let action = LeadAction::List {
                status: if status.is_empty() {
                    None
                } else {
                    Some(status)
                },
                api_type: if api_type.is_empty() {
                    None
                } else {
                    Some(api_type)
                },
            };
            crate::cmd_leads(database, action).await?;
        }
        1 => {
            let jurisdiction: String = Input::new()
                .with_prompt("Jurisdiction (e.g. Washington, DC)")
                .interact_text()?;
            let name: String = Input::new().with_prompt("Source name").interact_text()?;
            let api_type: String = Input::new()
                .with_prompt("API type (blank for unknown)")
                .allow_empty(true)
                .interact_text()?;
            let url: String = Input::new()
                .with_prompt("URL (blank if unknown)")
                .allow_empty(true)
                .interact_text()?;
            let priority: String = Input::new()
                .with_prompt("Priority (high/medium/low)")
                .default("medium".to_string())
                .interact_text()?;
            let notes: String = Input::new()
                .with_prompt("Notes (blank for none)")
                .allow_empty(true)
                .interact_text()?;

            let action = LeadAction::Add {
                jurisdiction,
                name,
                api_type: if api_type.is_empty() {
                    None
                } else {
                    Some(api_type)
                },
                url: if url.is_empty() { None } else { Some(url) },
                priority,
                likelihood: None,
                notes: if notes.is_empty() { None } else { Some(notes) },
            };
            crate::cmd_leads(database, action).await?;
        }
        2 => {
            let id: i64 = Input::new().with_prompt("Lead ID").interact_text()?;
            let status: String = Input::new()
                .with_prompt("New status (blank to skip)")
                .allow_empty(true)
                .interact_text()?;
            let notes: String = Input::new()
                .with_prompt("Notes (blank to skip)")
                .allow_empty(true)
                .interact_text()?;

            let action = LeadAction::Update {
                id,
                status: if status.is_empty() {
                    None
                } else {
                    Some(status)
                },
                record_count: None,
                has_coordinates: None,
                notes: if notes.is_empty() { None } else { Some(notes) },
            };
            crate::cmd_leads(database, action).await?;
        }
        3 => {
            let id: i64 = Input::new().with_prompt("Lead ID").interact_text()?;
            let action = LeadAction::Investigate { id };
            crate::cmd_leads(database, action).await?;
        }
        _ => {}
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

/// Interactive source management sub-menu.
async fn handle_sources(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let items = &["List", "Add", "Update", "Back"];
    let selection = Select::new()
        .with_prompt("Sources")
        .items(items)
        .default(0)
        .interact()?;

    match selection {
        0 => {
            let status: String = Input::new()
                .with_prompt("Filter by status (blank for all)")
                .allow_empty(true)
                .interact_text()?;
            let action = SourceAction::List {
                status: if status.is_empty() {
                    None
                } else {
                    Some(status)
                },
            };
            crate::cmd_sources(database, action).await?;
        }
        1 => {
            let source_id: String = Input::new()
                .with_prompt("Source ID (e.g. philadelphia_pd)")
                .interact_text()?;
            let jurisdiction: String = Input::new().with_prompt("Jurisdiction").interact_text()?;
            let api_type: String = Input::new().with_prompt("API type").interact_text()?;
            let url: String = Input::new().with_prompt("URL").interact_text()?;
            let toml_filename: String = Input::new()
                .with_prompt("TOML filename (blank for none)")
                .allow_empty(true)
                .interact_text()?;
            let notes: String = Input::new()
                .with_prompt("Notes (blank for none)")
                .allow_empty(true)
                .interact_text()?;

            let action = SourceAction::Add {
                source_id,
                jurisdiction,
                api_type,
                url,
                toml_filename: if toml_filename.is_empty() {
                    None
                } else {
                    Some(toml_filename)
                },
                notes: if notes.is_empty() { None } else { Some(notes) },
            };
            crate::cmd_sources(database, action).await?;
        }
        2 => {
            println!("sources update: Not yet implemented");
        }
        _ => {}
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Search log
// ---------------------------------------------------------------------------

/// Interactive search-history sub-menu.
async fn handle_search_log(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let items = &["List recent", "Add entry", "Back"];
    let selection = Select::new()
        .with_prompt("Search history")
        .items(items)
        .default(0)
        .interact()?;

    match selection {
        0 => {
            let limit: u32 = Input::new()
                .with_prompt("Max entries to show")
                .default(20)
                .interact_text()?;
            let action = SearchLogAction::List { limit };
            crate::cmd_search_log(database, action).await?;
        }
        1 => {
            let search_type: String = Input::new()
                .with_prompt("Search type (e.g. web, socrata_catalog)")
                .interact_text()?;
            let query: String = Input::new().with_prompt("Query / URL").interact_text()?;
            let geographic_scope: String = Input::new()
                .with_prompt("Geographic scope (blank for none)")
                .allow_empty(true)
                .interact_text()?;
            let results_summary: String = Input::new()
                .with_prompt("Results summary (blank for none)")
                .allow_empty(true)
                .interact_text()?;

            let action = SearchLogAction::Add {
                search_type,
                query,
                geographic_scope: if geographic_scope.is_empty() {
                    None
                } else {
                    Some(geographic_scope)
                },
                results_summary: if results_summary.is_empty() {
                    None
                } else {
                    Some(results_summary)
                },
                session_id: None,
            };
            crate::cmd_search_log(database, action).await?;
        }
        _ => {}
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Legal
// ---------------------------------------------------------------------------

/// Interactive legal-info sub-menu.
async fn handle_legal(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let items = &["List", "Add", "Show details", "Back"];
    let selection = Select::new()
        .with_prompt("Legal/licensing")
        .items(items)
        .default(0)
        .interact()?;

    match selection {
        0 => {
            crate::cmd_legal(database, LegalAction::List).await?;
        }
        1 => {
            let lead_id: String = Input::new()
                .with_prompt("Lead ID (blank for none)")
                .allow_empty(true)
                .interact_text()?;
            let source_id: String = Input::new()
                .with_prompt("Source ID (blank for none)")
                .allow_empty(true)
                .interact_text()?;
            let license_type: String = Input::new()
                .with_prompt("License type (blank for unknown)")
                .allow_empty(true)
                .interact_text()?;
            let tos_url: String = Input::new()
                .with_prompt("TOS URL (blank for none)")
                .allow_empty(true)
                .interact_text()?;
            let allows_api = Confirm::new()
                .with_prompt("Allows API access?")
                .default(true)
                .interact_opt()?;
            let allows_bulk = Confirm::new()
                .with_prompt("Allows bulk download?")
                .default(true)
                .interact_opt()?;
            let allows_redistribution = Confirm::new()
                .with_prompt("Allows redistribution?")
                .default(true)
                .interact_opt()?;
            let notes: String = Input::new()
                .with_prompt("Notes (blank for none)")
                .allow_empty(true)
                .interact_text()?;

            let action = LegalAction::Add {
                lead_id: lead_id.parse().ok(),
                source_id: source_id.parse().ok(),
                license_type: if license_type.is_empty() {
                    None
                } else {
                    Some(license_type)
                },
                tos_url: if tos_url.is_empty() {
                    None
                } else {
                    Some(tos_url)
                },
                allows_bulk_download: allows_bulk,
                allows_api_access: allows_api,
                allows_redistribution,
                allows_scraping: None,
                attribution_required: None,
                attribution_text: None,
                notes: if notes.is_empty() { None } else { Some(notes) },
            };
            crate::cmd_legal(database, action).await?;
        }
        2 => {
            let id: i64 = Input::new()
                .with_prompt("Legal record ID")
                .interact_text()?;
            crate::cmd_legal(database, LegalAction::Show { id }).await?;
        }
        _ => {}
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Scrape
// ---------------------------------------------------------------------------

/// Interactive scrape-targets sub-menu.
async fn handle_scrape(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let items = &["List", "Add", "Back"];
    let selection = Select::new()
        .with_prompt("Scrape targets")
        .items(items)
        .default(0)
        .interact()?;

    match selection {
        0 => {
            crate::cmd_scrape(database, ScrapeAction::List).await?;
        }
        1 => {
            let lead_id: i64 = Input::new()
                .with_prompt("Associated lead ID")
                .interact_text()?;
            let url: String = Input::new().with_prompt("URL to scrape").interact_text()?;
            let strategy: String = Input::new()
                .with_prompt("Strategy (blank for unknown)")
                .allow_empty(true)
                .interact_text()?;
            let notes: String = Input::new()
                .with_prompt("Notes (blank for none)")
                .allow_empty(true)
                .interact_text()?;

            let action = ScrapeAction::Add {
                lead_id,
                url,
                strategy: if strategy.is_empty() {
                    None
                } else {
                    Some(strategy)
                },
                auth_required: None,
                anti_bot: None,
                estimated_effort: None,
                notes: if notes.is_empty() { None } else { Some(notes) },
            };
            crate::cmd_scrape(database, action).await?;
        }
        _ => {}
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Integrate
// ---------------------------------------------------------------------------

/// Prompts for integration parameters and delegates to [`crate::cmd_integrate`].
async fn handle_integrate(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let id: i64 = Input::new()
        .with_prompt("Lead ID to integrate")
        .interact_text()?;
    let source_id: String = Input::new()
        .with_prompt("Source ID override (blank for auto)")
        .allow_empty(true)
        .interact_text()?;
    let dry_run = Confirm::new()
        .with_prompt("Dry run (preview only)?")
        .default(true)
        .interact()?;

    crate::cmd_integrate(
        database,
        id,
        if source_id.is_empty() {
            None
        } else {
            Some(source_id)
        },
        dry_run,
    )
    .await
}

// ---------------------------------------------------------------------------
// Verify
// ---------------------------------------------------------------------------

/// Prompts for an optional source ID and delegates to [`crate::cmd_verify`].
async fn handle_verify(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let source_id: String = Input::new()
        .with_prompt("Source ID to verify (blank for all)")
        .allow_empty(true)
        .interact_text()?;

    crate::cmd_verify(
        database,
        if source_id.is_empty() {
            None
        } else {
            Some(source_id)
        },
    )
    .await
}

// ---------------------------------------------------------------------------
// Suggest
// ---------------------------------------------------------------------------

/// Prompts for an optional region and delegates to [`crate::cmd_suggest`].
async fn handle_suggest(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let region: String = Input::new()
        .with_prompt("Region to focus on (blank for all)")
        .allow_empty(true)
        .interact_text()?;

    crate::cmd_suggest(
        database,
        if region.is_empty() {
            None
        } else {
            Some(region)
        },
    )
    .await
}
