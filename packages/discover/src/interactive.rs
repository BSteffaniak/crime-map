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

// ---------------------------------------------------------------------------
// Top-level menu
// ---------------------------------------------------------------------------

/// Top-level actions in the discovery interactive menu.
enum DiscoverAction {
    Status,
    Leads,
    Sources,
    SearchLog,
    Legal,
    Scrape,
    Seed,
    Integrate,
    Verify,
    Suggest,
    Exit,
}

impl DiscoverAction {
    const ALL: &[Self] = &[
        Self::Status,
        Self::Leads,
        Self::Sources,
        Self::SearchLog,
        Self::Legal,
        Self::Scrape,
        Self::Seed,
        Self::Integrate,
        Self::Verify,
        Self::Suggest,
        Self::Exit,
    ];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::Status => "Show status dashboard",
            Self::Leads => "Manage leads",
            Self::Sources => "Manage sources",
            Self::SearchLog => "Search history",
            Self::Legal => "Legal/licensing info",
            Self::Scrape => "Scrape targets",
            Self::Seed => "Seed database",
            Self::Integrate => "Integrate lead into pipeline",
            Self::Verify => "Health-check sources",
            Self::Suggest => "Suggest next actions",
            Self::Exit => "Exit",
        }
    }
}

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
    let labels: Vec<&str> = DiscoverAction::ALL
        .iter()
        .map(DiscoverAction::label)
        .collect();

    loop {
        println!();

        let idx = Select::new()
            .with_prompt("Discovery tool")
            .items(&labels)
            .default(0)
            .interact()?;

        match DiscoverAction::ALL[idx] {
            DiscoverAction::Status => crate::cmd_status(database).await?,
            DiscoverAction::Leads => handle_leads(database).await?,
            DiscoverAction::Sources => handle_sources(database).await?,
            DiscoverAction::SearchLog => handle_search_log(database).await?,
            DiscoverAction::Legal => handle_legal(database).await?,
            DiscoverAction::Scrape => handle_scrape(database).await?,
            DiscoverAction::Seed => crate::cmd_seed(database).await?,
            DiscoverAction::Integrate => handle_integrate(database).await?,
            DiscoverAction::Verify => handle_verify(database).await?,
            DiscoverAction::Suggest => handle_suggest(database).await?,
            DiscoverAction::Exit => {
                println!("Goodbye.");
                return Ok(());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Sub-menu enums
// ---------------------------------------------------------------------------

/// Lead management sub-actions.
enum LeadMenuAction {
    List,
    Add,
    Update,
    Investigate,
    Back,
}

impl LeadMenuAction {
    const ALL: &[Self] = &[
        Self::List,
        Self::Add,
        Self::Update,
        Self::Investigate,
        Self::Back,
    ];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::List => "List",
            Self::Add => "Add",
            Self::Update => "Update",
            Self::Investigate => "Investigate",
            Self::Back => "Back",
        }
    }
}

/// Source management sub-actions.
enum SourceMenuAction {
    List,
    Add,
    Update,
    Back,
}

impl SourceMenuAction {
    const ALL: &[Self] = &[Self::List, Self::Add, Self::Update, Self::Back];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::List => "List",
            Self::Add => "Add",
            Self::Update => "Update",
            Self::Back => "Back",
        }
    }
}

/// Search log sub-actions.
enum SearchLogMenuAction {
    List,
    Add,
    Back,
}

impl SearchLogMenuAction {
    const ALL: &[Self] = &[Self::List, Self::Add, Self::Back];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::List => "List recent",
            Self::Add => "Add entry",
            Self::Back => "Back",
        }
    }
}

/// Legal info sub-actions.
enum LegalMenuAction {
    List,
    Add,
    Show,
    Back,
}

impl LegalMenuAction {
    const ALL: &[Self] = &[Self::List, Self::Add, Self::Show, Self::Back];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::List => "List",
            Self::Add => "Add",
            Self::Show => "Show details",
            Self::Back => "Back",
        }
    }
}

/// Scrape target sub-actions.
enum ScrapeMenuAction {
    List,
    Add,
    Back,
}

impl ScrapeMenuAction {
    const ALL: &[Self] = &[Self::List, Self::Add, Self::Back];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::List => "List",
            Self::Add => "Add",
            Self::Back => "Back",
        }
    }
}

// ---------------------------------------------------------------------------
// Leads
// ---------------------------------------------------------------------------

/// Interactive lead management sub-menu.
#[allow(clippy::too_many_lines)]
async fn handle_leads(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let labels: Vec<&str> = LeadMenuAction::ALL
        .iter()
        .map(LeadMenuAction::label)
        .collect();
    let idx = Select::new()
        .with_prompt("Leads")
        .items(&labels)
        .default(0)
        .interact()?;

    match LeadMenuAction::ALL[idx] {
        LeadMenuAction::List => {
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
        LeadMenuAction::Add => {
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
        LeadMenuAction::Update => {
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
        LeadMenuAction::Investigate => {
            let id: i64 = Input::new().with_prompt("Lead ID").interact_text()?;
            let action = LeadAction::Investigate { id };
            crate::cmd_leads(database, action).await?;
        }
        LeadMenuAction::Back => {}
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

/// Interactive source management sub-menu.
async fn handle_sources(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let labels: Vec<&str> = SourceMenuAction::ALL
        .iter()
        .map(SourceMenuAction::label)
        .collect();
    let idx = Select::new()
        .with_prompt("Sources")
        .items(&labels)
        .default(0)
        .interact()?;

    match SourceMenuAction::ALL[idx] {
        SourceMenuAction::List => {
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
        SourceMenuAction::Add => {
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
        SourceMenuAction::Update => {
            println!("sources update: Not yet implemented");
        }
        SourceMenuAction::Back => {}
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Search log
// ---------------------------------------------------------------------------

/// Interactive search-history sub-menu.
async fn handle_search_log(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let labels: Vec<&str> = SearchLogMenuAction::ALL
        .iter()
        .map(SearchLogMenuAction::label)
        .collect();
    let idx = Select::new()
        .with_prompt("Search history")
        .items(&labels)
        .default(0)
        .interact()?;

    match SearchLogMenuAction::ALL[idx] {
        SearchLogMenuAction::List => {
            let limit: u32 = Input::new()
                .with_prompt("Max entries to show")
                .default(20)
                .interact_text()?;
            let action = SearchLogAction::List { limit };
            crate::cmd_search_log(database, action).await?;
        }
        SearchLogMenuAction::Add => {
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
        SearchLogMenuAction::Back => {}
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Legal
// ---------------------------------------------------------------------------

/// Interactive legal-info sub-menu.
async fn handle_legal(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let labels: Vec<&str> = LegalMenuAction::ALL
        .iter()
        .map(LegalMenuAction::label)
        .collect();
    let idx = Select::new()
        .with_prompt("Legal/licensing")
        .items(&labels)
        .default(0)
        .interact()?;

    match LegalMenuAction::ALL[idx] {
        LegalMenuAction::List => {
            crate::cmd_legal(database, LegalAction::List).await?;
        }
        LegalMenuAction::Add => {
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
        LegalMenuAction::Show => {
            let id: i64 = Input::new()
                .with_prompt("Legal record ID")
                .interact_text()?;
            crate::cmd_legal(database, LegalAction::Show { id }).await?;
        }
        LegalMenuAction::Back => {}
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Scrape
// ---------------------------------------------------------------------------

/// Interactive scrape-targets sub-menu.
async fn handle_scrape(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let labels: Vec<&str> = ScrapeMenuAction::ALL
        .iter()
        .map(ScrapeMenuAction::label)
        .collect();
    let idx = Select::new()
        .with_prompt("Scrape targets")
        .items(&labels)
        .default(0)
        .interact()?;

    match ScrapeMenuAction::ALL[idx] {
        ScrapeMenuAction::List => {
            crate::cmd_scrape(database, ScrapeAction::List).await?;
        }
        ScrapeMenuAction::Add => {
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
        ScrapeMenuAction::Back => {}
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
