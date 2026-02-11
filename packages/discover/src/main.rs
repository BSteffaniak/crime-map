#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! CLI entry point for the crime data source discovery tool.
//!
//! Provides subcommands for tracking leads, managing confirmed sources,
//! recording search history, reviewing legal/licensing info, managing
//! scrape targets, seeding the database with existing knowledge,
//! health-checking sources, and suggesting next discovery actions.

use std::fmt::Write as _;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use switchy_database::{Database, DatabaseValue};

mod commands;
mod db;

/// A known lead to seed into the discovery database.
struct SeedLead {
    jurisdiction: &'static str,
    name: &'static str,
    api_type: &'static str,
    url: &'static str,
    status: &'static str,
    record_count: Option<i64>,
    notes: &'static str,
}

// ---------------------------------------------------------------------------
// CLI definitions
// ---------------------------------------------------------------------------

/// Discover and track crime data sources.
#[derive(Parser)]
#[command(name = "crime_map_discover")]
#[command(about = "Discover and track crime data sources")]
struct Cli {
    /// Path to the discovery `SQLite` database.
    #[arg(long, default_value = "data/discovery.db")]
    db_path: PathBuf,

    /// Subcommand to execute.
    #[command(subcommand)]
    command: Commands,
}

/// Top-level subcommands.
#[derive(Subcommand)]
enum Commands {
    /// Show summary dashboard of leads, sources, and searches.
    Status,

    /// List and manage discovery leads.
    Leads {
        #[command(subcommand)]
        action: LeadAction,
    },

    /// List and manage confirmed sources.
    Sources {
        #[command(subcommand)]
        action: SourceAction,
    },

    /// View and add search history entries.
    SearchLog {
        #[command(subcommand)]
        action: SearchLogAction,
    },

    /// View and manage legal/licensing info.
    Legal {
        #[command(subcommand)]
        action: LegalAction,
    },

    /// Manage scrape targets.
    Scrape {
        #[command(subcommand)]
        action: ScrapeAction,
    },

    /// Populate DB with existing knowledge.
    Seed,

    /// Generate a source TOML config from a verified lead and register it.
    Integrate {
        /// Lead ID to integrate.
        id: i64,

        /// Override the auto-generated source ID.
        #[arg(long)]
        source_id: Option<String>,

        /// Preview the generated TOML without writing any files.
        #[arg(long)]
        dry_run: bool,
    },

    /// Health-check existing sources.
    Verify {
        /// Specific source ID to verify (default: all).
        source_id: Option<String>,
    },

    /// Suggest next discovery actions.
    Suggest {
        /// Geographic region to focus on.
        #[arg(long)]
        region: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Lead subcommands
// ---------------------------------------------------------------------------

/// Actions for the `leads` subcommand.
#[derive(Subcommand)]
enum LeadAction {
    /// List leads, optionally filtered by status or API type.
    List {
        /// Filter by lead status (e.g., "new", "investigating", "`verified_good`").
        #[arg(long)]
        status: Option<String>,

        /// Filter by API type (e.g., "socrata", "arcgis").
        #[arg(long)]
        api_type: Option<String>,
    },

    /// Add a new discovery lead.
    Add {
        /// Jurisdiction name (e.g., "Washington, DC").
        #[arg(long)]
        jurisdiction: String,

        /// Human-readable name for the data source.
        #[arg(long)]
        name: String,

        /// API type (e.g., "socrata", "arcgis", "csv").
        #[arg(long)]
        api_type: Option<String>,

        /// URL for the data source endpoint or landing page.
        #[arg(long)]
        url: Option<String>,

        /// Investigation priority ("high", "medium", "low").
        #[arg(long, default_value = "medium")]
        priority: String,

        /// Estimated likelihood (0.0–1.0) that this lead contains usable data.
        #[arg(long)]
        likelihood: Option<f64>,

        /// Free-form notes about this lead.
        #[arg(long)]
        notes: Option<String>,
    },

    /// Update a lead's status and metadata.
    Update {
        /// Lead ID.
        id: i64,

        /// New status value.
        #[arg(long)]
        status: Option<String>,

        /// Approximate number of records available.
        #[arg(long)]
        record_count: Option<i64>,

        /// Whether the source includes geographic coordinates.
        #[arg(long)]
        has_coordinates: Option<bool>,

        /// Free-form notes about this lead.
        #[arg(long)]
        notes: Option<String>,
    },

    /// Show detailed information about a specific lead.
    Investigate {
        /// Lead ID.
        id: i64,
    },
}

// ---------------------------------------------------------------------------
// Source subcommands
// ---------------------------------------------------------------------------

/// Actions for the `sources` subcommand.
#[derive(Subcommand)]
enum SourceAction {
    /// List all confirmed sources.
    List {
        /// Filter by source status (e.g., "active", "stale", "broken").
        #[arg(long)]
        status: Option<String>,
    },

    /// Add a new confirmed source.
    Add {
        /// Unique source identifier matching the TOML `id` field.
        #[arg(long)]
        source_id: String,

        /// Jurisdiction name.
        #[arg(long)]
        jurisdiction: String,

        /// API type used to access this source.
        #[arg(long)]
        api_type: String,

        /// Endpoint URL for data access.
        #[arg(long)]
        url: String,

        /// Path to the TOML configuration file.
        #[arg(long)]
        toml_filename: Option<String>,

        /// Free-form notes about this source.
        #[arg(long)]
        notes: Option<String>,
    },

    /// Update an existing source's metadata.
    Update {
        /// Database row ID.
        id: i64,

        /// New operational status.
        #[arg(long)]
        status: Option<String>,

        /// Total number of records available.
        #[arg(long)]
        record_count: Option<i64>,

        /// Free-form notes about this source.
        #[arg(long)]
        notes: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Search log subcommands
// ---------------------------------------------------------------------------

/// Actions for the `search-log` subcommand.
#[derive(Subcommand)]
enum SearchLogAction {
    /// List recent search history entries.
    List {
        /// Maximum number of entries to show.
        #[arg(long, default_value = "20")]
        limit: u32,
    },

    /// Add a new search history entry.
    Add {
        /// Type of search performed (e.g., "web", "`socrata_catalog`").
        #[arg(long)]
        search_type: String,

        /// The search query or URL that was executed.
        #[arg(long)]
        query: String,

        /// Geographic scope (e.g., "national", "Virginia").
        #[arg(long)]
        geographic_scope: Option<String>,

        /// Summary of what was found.
        #[arg(long)]
        results_summary: Option<String>,

        /// Discovery session identifier.
        #[arg(long)]
        session_id: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Legal subcommands
// ---------------------------------------------------------------------------

/// Actions for the `legal` subcommand.
#[derive(Subcommand)]
enum LegalAction {
    /// List legal/licensing records.
    List,

    /// Add legal/licensing info for a lead or source.
    Add {
        /// Associated lead ID.
        #[arg(long)]
        lead_id: Option<i64>,

        /// Associated source ID.
        #[arg(long)]
        source_id: Option<i64>,

        /// License type (e.g., "`open_data`", "`cc_by`", "proprietary").
        #[arg(long)]
        license_type: Option<String>,

        /// URL to the terms of service.
        #[arg(long)]
        tos_url: Option<String>,

        /// Whether bulk download is permitted.
        #[arg(long)]
        allows_bulk_download: Option<bool>,

        /// Whether API access is permitted.
        #[arg(long)]
        allows_api_access: Option<bool>,

        /// Whether redistribution is permitted.
        #[arg(long)]
        allows_redistribution: Option<bool>,

        /// Whether scraping is permitted.
        #[arg(long)]
        allows_scraping: Option<bool>,

        /// Whether attribution is required.
        #[arg(long)]
        attribution_required: Option<bool>,

        /// Required attribution text.
        #[arg(long)]
        attribution_text: Option<String>,

        /// Free-form notes.
        #[arg(long)]
        notes: Option<String>,
    },

    /// Show legal details for a specific record.
    Show {
        /// Legal record ID.
        id: i64,
    },
}

// ---------------------------------------------------------------------------
// Scrape subcommands
// ---------------------------------------------------------------------------

/// Actions for the `scrape` subcommand.
#[derive(Subcommand)]
enum ScrapeAction {
    /// List scrape targets.
    List,

    /// Add a new scrape target.
    Add {
        /// Associated lead ID.
        #[arg(long)]
        lead_id: i64,

        /// URL to scrape.
        #[arg(long)]
        url: String,

        /// Scrape strategy (e.g., "`html_table`", "`json_paginated`", "`csv_download`").
        #[arg(long)]
        strategy: Option<String>,

        /// Whether authentication is required.
        #[arg(long)]
        auth_required: Option<bool>,

        /// Anti-bot protection type.
        #[arg(long)]
        anti_bot: Option<String>,

        /// Estimated development effort.
        #[arg(long)]
        estimated_effort: Option<String>,

        /// Free-form notes.
        #[arg(long)]
        notes: Option<String>,
    },

    /// Update a scrape target.
    Update {
        /// Scrape target ID.
        id: i64,

        /// Updated scrape strategy.
        #[arg(long)]
        strategy: Option<String>,

        /// Updated anti-bot protection type.
        #[arg(long)]
        anti_bot: Option<String>,

        /// Free-form notes.
        #[arg(long)]
        notes: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let cli = Cli::parse();

    // Open (or create) the discovery `SQLite` database. This also ensures the
    // parent directory exists and runs schema migrations.
    let database = db::open_db(&cli.db_path).await?;

    match cli.command {
        Commands::Status => cmd_status(database.as_ref()).await,
        Commands::Leads { action } => cmd_leads(database.as_ref(), action).await,
        Commands::Sources { action } => cmd_sources(database.as_ref(), action).await,
        Commands::SearchLog { action } => cmd_search_log(database.as_ref(), action).await,
        Commands::Legal { action } => cmd_legal(database.as_ref(), action).await,
        Commands::Scrape { action } => cmd_scrape(database.as_ref(), action).await,
        Commands::Seed => cmd_seed(database.as_ref()).await,
        Commands::Integrate {
            id,
            source_id,
            dry_run,
        } => cmd_integrate(database.as_ref(), id, source_id, dry_run).await,
        Commands::Verify { source_id } => cmd_verify(database.as_ref(), source_id).await,
        Commands::Suggest { region } => cmd_suggest(database.as_ref(), region).await,
    }
}

// ---------------------------------------------------------------------------
// Status command (fully implemented)
// ---------------------------------------------------------------------------

/// Prints a summary dashboard of leads, sources, and search history.
async fn cmd_status(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let summary = db::get_status_summary(database).await?;

    println!("=== Discovery Status ===");
    println!();
    println!("Leads:    {} total", summary.total_leads);
    println!("  new:               {}", summary.new_leads);
    println!("  investigating:     {}", summary.investigating_leads);
    println!("  verified_good:     {}", summary.verified_good_leads);
    println!("  integrated:        {}", summary.integrated_leads);
    println!("  rejected:          {}", summary.rejected_leads);
    println!();
    println!(
        "Sources:  {} total ({} active)",
        summary.total_sources, summary.active_sources
    );
    println!("Searches: {}", summary.total_searches);

    Ok(())
}

// ---------------------------------------------------------------------------
// Leads command (list is implemented, others are stubs)
// ---------------------------------------------------------------------------

/// Dispatches `leads` subcommand actions.
#[allow(clippy::too_many_lines)]
async fn cmd_leads(
    database: &dyn Database,
    action: LeadAction,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        LeadAction::List { status, api_type } => {
            cmd_leads_list(database, status, api_type).await?;
        }
        LeadAction::Add {
            jurisdiction,
            name,
            api_type,
            url,
            priority,
            likelihood,
            notes,
        } => {
            let id = db::insert_lead(
                database,
                &jurisdiction,
                &name,
                api_type.as_deref(),
                url.as_deref(),
                &priority,
                likelihood,
                notes.as_deref(),
            )
            .await?;
            println!("Lead added (id={id}): {name}");
        }
        LeadAction::Update {
            id,
            status,
            record_count,
            has_coordinates,
            notes,
        } => {
            let lead = db::get_lead(database, id).await?;
            if lead.is_none() {
                println!("No lead found with id={id}");
                return Ok(());
            }

            db::update_lead(
                database,
                id,
                status.as_deref(),
                record_count,
                has_coordinates,
                notes.as_deref(),
            )
            .await?;
            println!("Lead {id} updated.");

            if let Some(lead) = db::get_lead(database, id).await? {
                println!(
                    "  Status: {}  Records: {}  Coords: {}",
                    lead.status.as_str(),
                    opt_display(lead.record_count),
                    opt_display(lead.has_coordinates),
                );
            }
        }
        LeadAction::Investigate { id } => {
            cmd_leads_investigate(database, id).await?;
        }
    }

    Ok(())
}

/// Lists leads, with optional status and API type filters.
async fn cmd_leads_list(
    database: &dyn Database,
    status: Option<String>,
    api_type: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let leads = db::get_leads(database, status.as_deref()).await?;

    let leads: Vec<_> = if let Some(ref at) = api_type {
        leads
            .into_iter()
            .filter(|l| {
                l.api_type
                    .as_ref()
                    .is_some_and(|t| t.as_str() == at.as_str())
            })
            .collect()
    } else {
        leads
    };

    if leads.is_empty() {
        println!("No leads found.");
        return Ok(());
    }

    println!(
        "{:<5} {:<20} {:<30} {:<12} {:<10} {:<8}",
        "ID", "JURISDICTION", "SOURCE NAME", "STATUS", "PRIORITY", "API TYPE"
    );
    println!("{}", "-".repeat(90));

    for lead in &leads {
        let api_type_str = lead.api_type.as_ref().map_or("-", |t| t.as_str());
        println!(
            "{:<5} {:<20} {:<30} {:<12} {:<10} {:<8}",
            lead.id,
            truncate(&lead.jurisdiction, 19),
            truncate(&lead.source_name, 29),
            lead.status.as_str(),
            lead.priority.as_str(),
            api_type_str,
        );
    }

    println!();
    println!("{} lead(s)", leads.len());
    Ok(())
}

/// Shows detailed information about a specific lead.
async fn cmd_leads_investigate(
    database: &dyn Database,
    id: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(lead) = db::get_lead(database, id).await? else {
        println!("No lead found with id={id}");
        return Ok(());
    };

    println!("=== Lead #{} ===", lead.id);
    println!("Jurisdiction: {}", lead.jurisdiction);
    println!("Source Name:  {}", lead.source_name);
    println!(
        "API Type:     {}",
        lead.api_type.as_ref().map_or("-", |t| t.as_str())
    );
    println!("URL:          {}", lead.url.as_deref().unwrap_or("-"));
    println!("Status:       {}", lead.status.as_str());
    println!("Priority:     {}", lead.priority.as_str());
    println!("Likelihood:   {}", opt_display_f64(lead.likelihood));
    println!("Record Count: {}", opt_display(lead.record_count));
    println!("Has Coords:   {}", opt_display(lead.has_coordinates));
    println!("Has Dates:    {}", opt_display(lead.has_dates));
    println!(
        "Coord Type:   {}",
        lead.coordinate_type.as_ref().map_or("-", |t| t.as_str())
    );
    println!("Discovered:   {}", lead.discovered_at);
    println!("Updated:      {}", lead.updated_at);
    println!(
        "Investigated: {}",
        lead.investigated_at.as_deref().unwrap_or("-")
    );
    if let Some(ref notes) = lead.notes {
        println!();
        println!("Notes:");
        println!("  {notes}");
    }

    let legal = db::get_legal_for_lead(database, id).await?;
    if !legal.is_empty() {
        println!();
        println!("Legal Info:");
        for l in &legal {
            println!(
                "  License: {}  API: {}  Bulk: {}  Redistribute: {}",
                l.license_type.as_ref().map_or("-", |t| t.as_str()),
                bool_char(l.allows_api_access),
                bool_char(l.allows_bulk_download),
                bool_char(l.allows_redistribution),
            );
            if let Some(ref notes) = l.notes {
                println!("  Notes: {notes}");
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Sources command (stub)
// ---------------------------------------------------------------------------

/// Dispatches `sources` subcommand actions.
async fn cmd_sources(
    database: &dyn Database,
    action: SourceAction,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        SourceAction::List { status } => {
            let sources = db::get_sources(database, status.as_deref()).await?;

            if sources.is_empty() {
                println!("No sources found.");
                return Ok(());
            }

            println!(
                "{:<4} {:<22} {:<22} {:<8} {:<8} {:<8}",
                "ID", "SOURCE ID", "JURISDICTION", "TYPE", "STATUS", "RECORDS"
            );
            println!("{}", "-".repeat(90));

            for source in &sources {
                let records = opt_display(source.record_count);
                println!(
                    "{:<4} {:<22} {:<22} {:<8} {:<8} {:<8}",
                    source.id,
                    truncate(&source.source_id, 21),
                    truncate(&source.jurisdiction, 21),
                    source.api_type.as_str(),
                    source.status.as_str(),
                    records,
                );
            }

            println!();
            println!("{} source(s)", sources.len());
        }
        SourceAction::Add {
            source_id,
            jurisdiction,
            api_type,
            url,
            toml_filename,
            notes,
        } => {
            let id = db::insert_source(
                database,
                &source_id,
                &jurisdiction,
                &api_type,
                &url,
                None, // record_count
                None, // date_range_start
                None, // date_range_end
                toml_filename.as_deref(),
                notes.as_deref(),
            )
            .await?;
            println!("Source added (id={id}): {source_id}");
        }
        SourceAction::Update { .. } => {
            println!("sources update: Not yet implemented");
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Search log command (stub)
// ---------------------------------------------------------------------------

/// Dispatches `search-log` subcommand actions.
async fn cmd_search_log(
    database: &dyn Database,
    action: SearchLogAction,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        SearchLogAction::List { limit } => {
            let searches = db::get_searches(database, None).await?;
            let searches: Vec<_> = searches.into_iter().take(limit as usize).collect();

            if searches.is_empty() {
                println!("No search history entries found.");
                return Ok(());
            }

            println!(
                "{:<4} {:<18} {:<40} {:<20} {:<10}",
                "ID", "TYPE", "QUERY", "SCOPE", "DATE"
            );
            println!("{}", "-".repeat(100));

            for entry in &searches {
                let scope = entry.geographic_scope.as_deref().unwrap_or("-");
                let date = entry.searched_at.get(..10).unwrap_or(&entry.searched_at);
                println!(
                    "{:<4} {:<18} {:<40} {:<20} {}",
                    entry.id,
                    truncate(&entry.search_type, 17),
                    truncate(&entry.query, 39),
                    truncate(scope, 19),
                    date,
                );
            }

            println!();
            println!("{} entries shown", searches.len());

            // Show summaries for the most recent entries
            for entry in searches.iter().take(5) {
                if let Some(ref summary) = entry.results_summary {
                    println!();
                    println!("  [{}] {}", entry.id, truncate(&entry.query, 60));
                    println!("    {summary}");
                }
            }
        }
        SearchLogAction::Add {
            search_type,
            query,
            geographic_scope,
            results_summary,
            session_id,
        } => {
            let id = db::insert_search(
                database,
                &search_type,
                &query,
                geographic_scope.as_deref(),
                results_summary.as_deref(),
                session_id.as_deref(),
            )
            .await?;
            println!("Search recorded (id={id})");
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Legal command (stub)
// ---------------------------------------------------------------------------

/// Dispatches `legal` subcommand actions.
async fn cmd_legal(
    database: &dyn Database,
    action: LegalAction,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        LegalAction::List => {
            cmd_legal_list(database).await?;
        }
        LegalAction::Add {
            lead_id,
            source_id,
            license_type,
            tos_url,
            allows_bulk_download,
            allows_api_access,
            allows_redistribution,
            allows_scraping,
            attribution_required,
            attribution_text,
            notes,
        } => {
            let id = db::insert_legal(
                database,
                lead_id,
                source_id,
                license_type.as_deref(),
                tos_url.as_deref(),
                allows_bulk_download,
                allows_api_access,
                allows_redistribution,
                allows_scraping,
                attribution_required,
                attribution_text.as_deref(),
                None, // rate_limits
                notes.as_deref(),
            )
            .await?;
            println!("Legal record added (id={id})");
        }
        LegalAction::Show { id } => {
            cmd_legal_show(database, id).await?;
        }
    }

    Ok(())
}

/// Lists all legal/licensing records.
async fn cmd_legal_list(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let records = db::get_all_legal(database).await?;

    if records.is_empty() {
        println!("No legal records found.");
        return Ok(());
    }

    println!(
        "{:<4} {:<8} {:<8} {:<14} {:<5} {:<5} {:<5} {:<30}",
        "ID", "LEAD", "SOURCE", "LICENSE", "API", "BULK", "REDIS", "NOTES"
    );
    println!("{}", "-".repeat(80));

    for l in &records {
        let lead = opt_display(l.lead_id);
        let source = opt_display(l.source_id);
        let license = l.license_type.as_ref().map_or("-", |t| t.as_str());
        let api = bool_char(l.allows_api_access);
        let bulk = bool_char(l.allows_bulk_download);
        let redis = bool_char(l.allows_redistribution);
        let notes = l.notes.as_deref().unwrap_or("-");
        println!(
            "{:<4} {:<8} {:<8} {:<14} {:<5} {:<5} {:<5} {:<30}",
            l.id,
            lead,
            source,
            truncate(license, 13),
            api,
            bulk,
            redis,
            truncate(notes, 30),
        );
    }

    println!();
    println!("{} record(s)", records.len());
    Ok(())
}

/// Shows detailed legal information for a specific record.
async fn cmd_legal_show(
    database: &dyn Database,
    id: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(l) = db::get_legal(database, id).await? else {
        println!("No legal record found with id={id}");
        return Ok(());
    };

    println!("=== Legal Record #{} ===", l.id);
    println!("Lead ID:           {}", opt_display(l.lead_id));
    println!("Source ID:         {}", opt_display(l.source_id));
    println!(
        "License Type:      {}",
        l.license_type.as_ref().map_or("-", |t| t.as_str())
    );
    println!("TOS URL:           {}", l.tos_url.as_deref().unwrap_or("-"));
    println!("Allows API:        {}", bool_str(l.allows_api_access));
    println!("Allows Bulk DL:    {}", bool_str(l.allows_bulk_download));
    println!("Allows Redistrib:  {}", bool_str(l.allows_redistribution));
    println!("Allows Scraping:   {}", bool_str(l.allows_scraping));
    println!("Attribution Req:   {}", bool_str(l.attribution_required));
    println!(
        "Attribution Text:  {}",
        l.attribution_text.as_deref().unwrap_or("-")
    );
    println!(
        "Rate Limits:       {}",
        l.rate_limits.as_deref().unwrap_or("-")
    );
    println!(
        "Reviewed At:       {}",
        l.reviewed_at.as_deref().unwrap_or("-")
    );
    if let Some(ref notes) = l.notes {
        println!();
        println!("Notes:");
        println!("  {notes}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Scrape command
// ---------------------------------------------------------------------------

/// Dispatches `scrape` subcommand actions.
async fn cmd_scrape(
    database: &dyn Database,
    action: ScrapeAction,
) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        ScrapeAction::List => {
            let targets = db::get_scrape_targets(database).await?;

            if targets.is_empty() {
                println!("No scrape targets found.");
                return Ok(());
            }

            println!(
                "{:<4} {:<6} {:<40} {:<16} {:<10} {:<10}",
                "ID", "LEAD", "URL", "STRATEGY", "EFFORT", "ANTI-BOT"
            );
            println!("{}", "-".repeat(90));

            for t in &targets {
                let strategy = t.scrape_strategy.as_ref().map_or("-", |s| s.as_str());
                let effort = t.estimated_effort.as_deref().unwrap_or("-");
                let anti_bot = t.anti_bot.as_ref().map_or("-", |a| a.as_str());
                println!(
                    "{:<4} {:<6} {:<40} {:<16} {:<10} {}",
                    t.id,
                    t.lead_id,
                    truncate(&t.url, 39),
                    strategy,
                    effort,
                    anti_bot,
                );
            }

            println!();
            println!("{} target(s)", targets.len());
        }
        ScrapeAction::Add {
            lead_id,
            url,
            strategy,
            auth_required,
            anti_bot,
            estimated_effort,
            notes,
        } => {
            let id = db::insert_scrape_target(
                database,
                lead_id,
                &url,
                strategy.as_deref(),
                None, // pagination_method
                auth_required.unwrap_or(false),
                anti_bot.as_deref(),
                estimated_effort.as_deref(),
                notes.as_deref(),
            )
            .await?;
            println!("Scrape target added (id={id})");
        }
        ScrapeAction::Update { .. } => {
            println!("scrape update: Not yet implemented");
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Seed command
// ---------------------------------------------------------------------------

/// Seeds the discovery database with existing knowledge about data sources.
async fn cmd_seed(database: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    println!("Seeding discovery database...\n");

    let source_count = seed_sources(database).await?;
    println!("  Sources:  {source_count} imported from TOML configs");

    let lead_count = seed_leads(database).await?;
    println!("  Leads:    {lead_count} known leads added");

    let search_count = seed_searches(database).await?;
    println!("  Searches: {search_count} historical searches recorded");

    let pattern_count = seed_patterns(database).await?;
    println!("  Patterns: {pattern_count} API patterns recorded");

    println!("\nSeed complete.");
    Ok(())
}

/// Imports existing TOML source definitions into the discovery database.
async fn seed_sources(database: &dyn Database) -> Result<u32, Box<dyn std::error::Error>> {
    let sources = crime_map_source::registry::all_sources();
    let mut source_count = 0u32;

    for source in &sources {
        let (api_type, url) = extract_source_info(&source.fetcher);
        let jurisdiction = format!("{}, {}", source.city, source.state);

        // Skip duplicates (INSERT OR IGNORE style — check first)
        let existing = db::get_sources(database, None).await?;
        if existing.iter().any(|s| s.source_id == source.id) {
            continue;
        }

        db::insert_source(
            database,
            &source.id,
            &jurisdiction,
            api_type,
            &url,
            None, // record_count — unknown without querying
            None, // date_range_start
            None, // date_range_end
            Some(&format!("{}.toml", source.id.replace("_pd", ""))),
            None, // notes
        )
        .await?;
        source_count += 1;
    }

    Ok(source_count)
}

/// Seeds the database with known leads that were investigated but not integrated.
#[allow(clippy::too_many_lines)]
async fn seed_leads(database: &dyn Database) -> Result<u32, Box<dyn std::error::Error>> {
    let known_leads: &[SeedLead] = &[
        SeedLead {
            jurisdiction: "Norfolk, VA",
            name: "Norfolk Police Incident Reports",
            api_type: "socrata",
            url: "https://data.norfolk.gov/resource/r7bn-2egr.json",
            status: "needs_geocoding",
            record_count: Some(107_480),
            notes: "Has street address + neighborhood but no lat/lng coordinates. Would need geocoding.",
        },
        SeedLead {
            jurisdiction: "Virginia Beach, VA",
            name: "Virginia Beach Police Offense Reports",
            api_type: "arcgis",
            url: "https://services2.arcgis.com/CyVvlIiUfRBmMQuu/arcgis/rest/services/Police_Incident_Reports_view/FeatureServer/0/query",
            status: "needs_geocoding",
            record_count: Some(175_856),
            notes: "ArcGIS FeatureServer but geometryType is NONE. Has Block + Street fields for geocoding.",
        },
        SeedLead {
            jurisdiction: "Howard County, MD",
            name: "Howard County Calls for Service",
            api_type: "socrata",
            url: "https://opendata.howardcountymd.gov/resource/qccx-65fg.json",
            status: "verified_no_coords",
            record_count: Some(114_125),
            notes: "Address text only, no lat/lng. Calls for service, not crime incidents.",
        },
        SeedLead {
            jurisdiction: "Anne Arundel County, MD",
            name: "Anne Arundel End of Year Crime Data",
            api_type: "arcgis",
            url: "https://services2.arcgis.com/nUoGCkM6W8Wqdvvh/arcgis/rest/services/Police_End_of_Year_Crime_Data/FeatureServer",
            status: "verified_aggregate_only",
            record_count: Some(197),
            notes: "Aggregate yearly totals by district. Not incident-level data.",
        },
        SeedLead {
            jurisdiction: "Loudoun County, VA",
            name: "Loudoun County Crime Reports",
            api_type: "unknown",
            url: "https://www.crimereports.com",
            status: "verified_proprietary",
            record_count: None,
            notes: "Uses proprietary CrimeReports.com (Motorola). No public API.",
        },
        SeedLead {
            jurisdiction: "Alexandria, VA",
            name: "Alexandria Police Crime Database",
            api_type: "unknown",
            url: "https://www3.alexandriava.gov/police/crime_reports/reporter.php",
            status: "verified_proprietary",
            record_count: None,
            notes: "PHP form-based search only. Uses LexisNexis CommunityCrimeMap. No bulk API.",
        },
        SeedLead {
            jurisdiction: "Charlottesville, VA",
            name: "Charlottesville Arrests",
            api_type: "arcgis",
            url: "https://gisweb.charlottesville.org/arcgis/rest/services/OpenData_2/MapServer/22/query",
            status: "rejected",
            record_count: Some(11_374),
            notes: "Arrests only, no geometry (geometryType: NONE), contains PII (names).",
        },
        SeedLead {
            jurisdiction: "Newport News, VA",
            name: "Newport News PD Incidents",
            api_type: "arcgis",
            url: "https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/NNPD_OpenData/FeatureServer/1/query",
            status: "verified_no_coords",
            record_count: Some(41_116),
            notes: "No geometry, no pagination support (returns error 400). Text-only location.",
        },
    ];

    let mut lead_count = 0u32;
    let existing_leads = db::get_leads(database, None).await?;
    for lead in known_leads {
        // Skip if we already have a lead with this name
        if existing_leads.iter().any(|l| l.source_name == lead.name) {
            continue;
        }

        let lead_id = db::insert_lead(
            database,
            lead.jurisdiction,
            lead.name,
            Some(lead.api_type),
            Some(lead.url),
            "medium",
            None,
            Some(lead.notes),
        )
        .await?;

        // Update status and record count
        db::update_lead_status(database, lead_id, lead.status).await?;
        if let Some(count) = lead.record_count {
            database
                .exec_raw_params(
                    "UPDATE leads SET record_count = ?, has_coordinates = ?, investigated_at = ? WHERE id = ?",
                    &[
                        DatabaseValue::Int64(count),
                        DatabaseValue::Int32(i32::from(lead.status == "needs_geocoding" || lead.status == "verified_good")),
                        DatabaseValue::String(chrono::Utc::now().to_rfc3339()),
                        DatabaseValue::Int64(lead_id),
                    ],
                )
                .await
                .map_err(|e| db::DbError::Database(e.to_string()))?;
        }
        lead_count += 1;
    }

    Ok(lead_count)
}

/// Records historical searches that have been performed.
#[allow(clippy::too_many_lines)]
async fn seed_searches(database: &dyn Database) -> Result<u32, Box<dyn std::error::Error>> {
    let searches: &[(&str, &str, &str, &str)] = &[
        // (search_type, query, scope, results_summary)
        (
            "socrata_portal",
            "data.montgomerycountymd.gov crime datasets",
            "county:Montgomery,MD",
            "Found icn6-v9z3 (479K records). All other crime datasets are derived views.",
        ),
        (
            "socrata_portal",
            "data.princegeorgescountymd.gov crime datasets",
            "county:Prince George's,MD",
            "Found xjru-idbe (67K, 2023+) and wb4e-w4nf (151K, 2017-2023). Both integrated.",
        ),
        (
            "socrata_portal",
            "opendata.maryland.gov crime datasets",
            "state:MD",
            "Only aggregate county-level stats. No incident-level geocoded data.",
        ),
        (
            "socrata_portal",
            "data.virginia.gov crime datasets",
            "state:VA",
            "CKAN-based portal. Only indexes other datasets. No state-level geocoded crime.",
        ),
        (
            "socrata_portal",
            "data.norfolk.gov crime datasets",
            "city:Norfolk,VA",
            "Found r7bn-2egr (107K records) but no coordinates. Address text only.",
        ),
        (
            "socrata_portal",
            "data.richmondgov.com crime datasets",
            "city:Richmond,VA",
            "Only 911 call timing metadata. No crime incidents with coordinates.",
        ),
        (
            "arcgis_hub",
            "crime incidents maryland bounding box search",
            "state:MD",
            "Found Baltimore County PublicCrime (257K). Anne Arundel is aggregate only.",
        ),
        (
            "arcgis_hub",
            "crime incidents virginia bounding box search",
            "state:VA",
            "Found Fairfax County (188K, 3 layers). Loudoun/Alexandria use proprietary platforms.",
        ),
        (
            "arcgis_server",
            "DC MPD MapServer layers",
            "city:Washington,DC",
            "19 per-year layers (2008-2026), 588K records total. All integrated.",
        ),
        (
            "arcgis_server",
            "Baltimore City FeatureServer datasets",
            "city:Baltimore,MD",
            "Found NIBRS (237K), Part1 (538K), Arrests. All integrated.",
        ),
        (
            "arcgis_server",
            "bcgisdata.baltimorecountymd.gov",
            "county:Baltimore County,MD",
            "Found PublicCrime MapServer table (257K). String coords, no geometry layer.",
        ),
        (
            "arcgis_server",
            "Fairfax County ArcGIS crime layers",
            "county:Fairfax,VA",
            "3 NIBRS layers: Person (51K), Property (136K), Society (949). All integrated.",
        ),
        (
            "arcgis_hub",
            "crime incidents virginia cities (Lynchburg, Chesterfield)",
            "state:VA",
            "Lynchburg (66K, 10yr), Chesterfield County (26K, 2yr). Both integrated.",
        ),
        (
            "manual",
            "Arlington County datahub API",
            "county:Arlington,VA",
            "OData-style REST API. 82K records (2015-2022, frozen). Integrated with new OData fetcher.",
        ),
        (
            "manual",
            "Norfolk, Virginia Beach, Newport News, Charlottesville",
            "state:VA",
            "Norfolk/VB have large datasets but no coordinates. Newport News no pagination. Charlottesville arrests only + PII.",
        ),
        (
            "manual",
            "Howard, Frederick, Charles, Harford, Carroll counties MD",
            "state:MD",
            "Howard has CFS (no coords). Others have no open data portals at all.",
        ),
        (
            "manual",
            "Loudoun, Alexandria, Fairfax City, Falls Church, Manassas VA",
            "state:VA",
            "All use proprietary platforms (CrimeReports, LexisNexis) or have no data portal.",
        ),
        (
            "manual",
            "FBI NIBRS bulk data",
            "national",
            "No coordinates in NIBRS. Only agency-level (ORI) geography. Not usable for mapping.",
        ),
        (
            "manual",
            "Commercial aggregators (CrimeMapping, SpotCrime, LexisNexis)",
            "national",
            "All proprietary. SpotCrime has semi-public /crimes.json but session-gated token.",
        ),
    ];

    let mut search_count = 0u32;
    let existing_searches = db::get_searches(database, None).await?;
    for &(search_type, query, scope, summary) in searches {
        if existing_searches.iter().any(|s| s.query == query) {
            continue;
        }
        db::insert_search(
            database,
            search_type,
            query,
            Some(scope),
            Some(summary),
            Some("seed"),
        )
        .await?;
        search_count += 1;
    }

    Ok(search_count)
}

/// Seeds known API pattern knowledge.
async fn seed_patterns(database: &dyn Database) -> Result<u32, Box<dyn std::error::Error>> {
    let patterns: &[(&str, &str, &str, &str)] = &[
        // (pattern_name, discovery_strategy, quality_rating, notes)
        (
            "socrata_crime",
            "Check {domain}/api/views.json, filter by name containing crime/police/incident. Fetch 1 record to verify lat/lng columns.",
            "excellent",
            "Best API quality. Standard pagination, counts, filtering. Most major cities use Socrata.",
        ),
        (
            "arcgis_featureserver",
            "Search hub.arcgis.com or arcgis.com/sharing/rest/search. Check for esriGeometryPoint features with crime/police keywords.",
            "excellent",
            "Good pagination via resultOffset. Watch for per-year layers. Some strip geometry intentionally.",
        ),
        (
            "arcgis_mapserver_table",
            "Same as FeatureServer but MapServer tables have no geometry. Coords are string attributes.",
            "good",
            "Works fine but coords must be extracted from attribute fields, not geometry. Baltimore County pattern.",
        ),
        (
            "ckan_crime",
            "Check {domain}/api/3/action/package_search?q=crime. Verify datastore_search works.",
            "good",
            "Less common than Socrata/ArcGIS. Boston and Pittsburgh use CKAN.",
        ),
        (
            "carto_sql",
            "Carto SQL API at {domain}/api/v2/sql. Table names found via metadata.",
            "fair",
            "Only Philadelphia uses Carto in our dataset. Reasonable pagination via LIMIT/OFFSET.",
        ),
        (
            "odata_rest",
            "Custom JSON REST APIs with $top/$skip/$orderby. Count via /$count endpoint.",
            "fair",
            "Uncommon. Arlington VA is the only example. Each implementation is slightly different.",
        ),
    ];

    let mut pattern_count = 0u32;
    for &(name, strategy, quality, notes) in patterns {
        // Use INSERT OR IGNORE pattern — check for existing
        let existing = db::get_api_patterns(database).await?;
        if existing.iter().any(|p| p.pattern_name == name) {
            continue;
        }
        database
            .exec_raw_params(
                "INSERT INTO api_patterns (pattern_name, discovery_strategy, quality_rating, notes) VALUES (?, ?, ?, ?)",
                &[
                    DatabaseValue::String(name.to_string()),
                    DatabaseValue::String(strategy.to_string()),
                    DatabaseValue::String(quality.to_string()),
                    DatabaseValue::String(notes.to_string()),
                ],
            )
            .await
            .map_err(|e| db::DbError::Database(e.to_string()))?;
        pattern_count += 1;
    }

    Ok(pattern_count)
}

/// Extracts the API type string and primary URL from a [`FetcherConfig`].
fn extract_source_info(fetcher: &crime_map_source::source_def::FetcherConfig) -> (&str, String) {
    use crime_map_source::source_def::FetcherConfig;
    match fetcher {
        FetcherConfig::Socrata { api_url, .. } => ("socrata", api_url.clone()),
        FetcherConfig::Arcgis { query_urls, .. } => {
            ("arcgis", query_urls.first().cloned().unwrap_or_default())
        }
        FetcherConfig::Ckan {
            api_url,
            resource_ids,
            ..
        } => (
            "ckan",
            format!(
                "{}/api/3/action/datastore_search?resource_id={}",
                api_url,
                resource_ids.first().map_or("", String::as_str)
            ),
        ),
        FetcherConfig::Carto { api_url, .. } => ("carto", api_url.clone()),
        FetcherConfig::Odata { api_url, .. } => ("odata", api_url.clone()),
        FetcherConfig::HtmlTable { url, .. } => ("scrape", url.clone()),
        FetcherConfig::CsvDownload { urls, .. } => {
            ("csv", urls.first().cloned().unwrap_or_default())
        }
        FetcherConfig::JsonPaginated { api_url, .. } => ("json_paginated", api_url.clone()),
        FetcherConfig::PdfExtract { urls, .. } => {
            ("pdf_extract", urls.first().cloned().unwrap_or_default())
        }
    }
}

// ---------------------------------------------------------------------------
// Integrate command
// ---------------------------------------------------------------------------

/// Generates a source TOML config from a verified lead and registers it in
/// the ingest pipeline.
async fn cmd_integrate(
    database: &dyn Database,
    id: i64,
    source_id_override: Option<String>,
    dry_run: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(lead) = db::get_lead(database, id).await? else {
        println!("No lead found with id={id}");
        return Ok(());
    };

    // Validate the lead is in a suitable state
    let status = lead.status.as_str();
    if status != "verified_good" && status != "new" && status != "investigating" {
        println!("Lead {id} has status '{status}' -- expected 'verified_good' for integration.",);
        println!("Use `cargo discover leads update {id} --status verified_good` first.");
        return Ok(());
    }

    let api_type = lead.api_type.as_ref().map_or("unknown", |t| t.as_str());

    // Generate a source ID from the jurisdiction if not overridden
    let source_id = source_id_override.unwrap_or_else(|| derive_source_id(&lead.jurisdiction));

    // Derive the TOML filename (without _pd suffix for the filename)
    let toml_filename = format!(
        "{}.toml",
        source_id.strip_suffix("_pd").unwrap_or(&source_id)
    );
    let toml_path = format!("packages/source/sources/{toml_filename}");

    // Generate the skeleton TOML content
    let toml_content = generate_toml_skeleton(&lead, &source_id, api_type);

    if dry_run {
        println!("=== Dry Run: Integration Preview ===");
        println!();
        println!("Lead:      #{} - {}", lead.id, lead.source_name);
        println!("Source ID: {source_id}");
        println!("TOML file: {toml_path}");
        println!();
        println!("--- Generated TOML ---");
        println!("{toml_content}");
        println!("--- End TOML ---");
        println!();
        println!("Registry: Would add include_str! entry to packages/source/src/registry.rs");
        println!("Lead:     Would update status to 'integrated'");
        return Ok(());
    }

    // Check if the TOML file already exists
    let full_toml_path = PathBuf::from(&toml_path);
    if full_toml_path.exists() {
        println!("TOML file already exists: {toml_path}");
        println!("Aborting to avoid overwriting. Use --source-id to pick a different name.");
        return Ok(());
    }

    // Write the TOML file
    std::fs::write(&full_toml_path, &toml_content)?;
    println!("Created {toml_path}");

    // Update registry.rs
    let registry_path = PathBuf::from("packages/source/src/registry.rs");
    let registry_content = std::fs::read_to_string(&registry_path)?;

    let registry_name = source_id.strip_suffix("_pd").unwrap_or(&source_id);

    // Determine which section to insert into based on API type
    let section_marker = match api_type {
        "socrata" => "Socrata sources",
        "arcgis" => "ArcGIS sources",
        "carto" => "Carto sources",
        "ckan" => "CKAN sources",
        // Default to OData section (last before closing bracket)
        _ => "OData sources",
    };

    // Find the right place to insert. We insert just before the next section
    // marker or at the end of the SOURCE_TOMLS array.
    let include_line =
        format!("    (\"{registry_name}\", include_str!(\"../sources/{toml_filename}\")),",);

    // Simple approach: find the section marker line, then find the next
    // section marker after it, and insert before that next section.
    let updated_registry = insert_registry_entry(&registry_content, &include_line, section_marker);

    if let Some(updated) = updated_registry {
        // Also increment EXPECTED_SOURCE_COUNT
        let updated = increment_source_count(&updated);
        std::fs::write(&registry_path, updated)?;
        println!("Updated packages/source/src/registry.rs");
    } else {
        println!("Warning: Could not find section '{section_marker}' in registry.rs.");
        println!("Please add manually:");
        println!("  {include_line}");
    }

    // Mark the lead as integrated
    db::update_lead_status(database, id, "integrated").await?;
    println!("Lead {id} status updated to 'integrated'");

    println!();
    println!("Next steps:");
    println!("  1. Edit {toml_path} -- fill in TODO field mappings from sample API records");
    println!("  2. Run: cargo test -p crime_map_source");
    println!("  3. Run: cargo ingest sync --source {source_id} --limit 100");

    Ok(())
}

/// Derives a source ID from a jurisdiction string like "Philadelphia, PA".
///
/// Examples:
/// - "Philadelphia, PA" -> `"philadelphia_pd"`
/// - "San Francisco, CA" -> `"san_francisco_pd"`
/// - "Denver, CO" -> `"denver_pd"`
/// - "Montgomery County, MD" -> `"montgomery_county_pd"`
fn derive_source_id(jurisdiction: &str) -> String {
    let city_part = jurisdiction
        .split(',')
        .next()
        .unwrap_or(jurisdiction)
        .trim()
        .to_lowercase();

    let sanitized: String = city_part
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();

    // Collapse multiple underscores
    let mut result = String::new();
    let mut prev_underscore = false;
    for c in sanitized.chars() {
        if c == '_' {
            if !prev_underscore {
                result.push('_');
            }
            prev_underscore = true;
        } else {
            result.push(c);
            prev_underscore = false;
        }
    }

    // Trim trailing underscores
    let trimmed = result.trim_end_matches('_');
    format!("{trimmed}_pd")
}

/// Generates a skeleton TOML config for a source.
#[allow(clippy::too_many_lines)]
fn generate_toml_skeleton(
    lead: &crime_map_discover_models::Lead,
    source_id: &str,
    api_type: &str,
) -> String {
    let jurisdiction = &lead.jurisdiction;
    let city = jurisdiction
        .split(',')
        .next()
        .unwrap_or(jurisdiction)
        .trim();
    let state = jurisdiction.split(',').nth(1).map_or("XX", str::trim);
    let url = lead.url.as_deref().unwrap_or("https://TODO");

    let city_lower = city.to_lowercase().replace(' ', "_");

    let mut toml = String::new();

    // Header
    writeln!(toml, "id = \"{source_id}\"").unwrap();
    writeln!(toml, "name = \"{city} Police Department\"").unwrap();
    writeln!(toml, "city = \"{city}\"").unwrap();
    writeln!(toml, "state = \"{state}\"").unwrap();
    writeln!(toml, "output_filename = \"{city_lower}_crimes.json\"").unwrap();
    writeln!(toml).unwrap();

    // Fetcher section based on API type
    match api_type {
        "socrata" => {
            writeln!(toml, "[fetcher]").unwrap();
            writeln!(toml, "type = \"socrata\"").unwrap();
            writeln!(toml, "api_url = \"{url}\"").unwrap();
            writeln!(toml, "date_column = \"TODO\"  # date field name").unwrap();
            writeln!(toml, "page_size = 50000").unwrap();
        }
        "arcgis" => {
            writeln!(toml, "[fetcher]").unwrap();
            writeln!(toml, "type = \"arcgis\"").unwrap();
            writeln!(toml, "query_urls = [\"{url}\"]").unwrap();
            writeln!(toml, "page_size = 2000").unwrap();
            writeln!(
                toml,
                "# date_column = \"TODO\"  # optional, for incremental sync"
            )
            .unwrap();
        }
        "carto" => {
            // Parse table name from URL if possible
            let table_name = extract_carto_table(url);
            let api_base = extract_carto_base(url);
            writeln!(toml, "[fetcher]").unwrap();
            writeln!(toml, "type = \"carto\"").unwrap();
            writeln!(toml, "api_url = \"{api_base}\"").unwrap();
            writeln!(toml, "table_name = \"{table_name}\"").unwrap();
            writeln!(toml, "date_column = \"TODO\"  # date field name").unwrap();
            writeln!(toml, "page_size = 10000").unwrap();
        }
        "ckan" => {
            writeln!(toml, "[fetcher]").unwrap();
            writeln!(toml, "type = \"ckan\"").unwrap();
            writeln!(toml, "api_url = \"{url}\"").unwrap();
            writeln!(toml, "resource_ids = [\"TODO\"]").unwrap();
            writeln!(toml, "page_size = 32000").unwrap();
            writeln!(toml, "# date_column = \"TODO\"  # optional").unwrap();
        }
        "odata" => {
            writeln!(toml, "[fetcher]").unwrap();
            writeln!(toml, "type = \"odata\"").unwrap();
            writeln!(toml, "api_url = \"{url}\"").unwrap();
            writeln!(toml, "date_column = \"TODO\"  # date field name").unwrap();
            writeln!(toml, "page_size = 5000").unwrap();
        }
        _ => {
            writeln!(toml, "[fetcher]").unwrap();
            writeln!(
                toml,
                "# TODO: Choose fetcher type (socrata, arcgis, carto, ckan, odata)"
            )
            .unwrap();
            writeln!(toml, "type = \"socrata\"").unwrap();
            writeln!(toml, "api_url = \"{url}\"").unwrap();
            writeln!(toml, "date_column = \"TODO\"").unwrap();
            writeln!(toml, "page_size = 50000").unwrap();
        }
    }

    // Fields section (always TODO placeholders)
    writeln!(toml).unwrap();
    writeln!(toml, "[fields]").unwrap();
    writeln!(
        toml,
        "incident_id = [\"TODO\"]  # field(s) for unique incident/case ID"
    )
    .unwrap();
    writeln!(
        toml,
        "crime_type = [\"TODO\"]   # field(s) for crime type/category"
    )
    .unwrap();
    writeln!(
        toml,
        "# reported_at = \"TODO\"  # optional: report date field"
    )
    .unwrap();
    writeln!(
        toml,
        "# block_address = \"TODO\"  # optional: street/block address"
    )
    .unwrap();
    writeln!(
        toml,
        "# location_type = \"TODO\"  # optional: location description"
    )
    .unwrap();
    writeln!(toml).unwrap();
    writeln!(toml, "[fields.occurred_at]").unwrap();
    writeln!(
        toml,
        "type = \"simple\"  # simple | date_plus_hhmm | epoch_ms | mdy_date"
    )
    .unwrap();
    writeln!(toml, "field = \"TODO\"").unwrap();
    writeln!(toml).unwrap();
    writeln!(toml, "[fields.lat]").unwrap();
    writeln!(toml, "field = \"TODO\"").unwrap();
    writeln!(toml, "type = \"string\"  # string | f64 | point_lat").unwrap();
    writeln!(toml).unwrap();
    writeln!(toml, "[fields.lng]").unwrap();
    writeln!(toml, "field = \"TODO\"").unwrap();
    writeln!(toml, "type = \"string\"  # string | f64 | point_lng").unwrap();
    writeln!(toml).unwrap();
    writeln!(toml, "[fields.description]").unwrap();
    writeln!(
        toml,
        "type = \"single\"  # single | combine | fallback_chain"
    )
    .unwrap();
    writeln!(toml, "field = \"TODO\"").unwrap();
    writeln!(toml).unwrap();
    writeln!(toml, "[fields.arrest]").unwrap();
    writeln!(
        toml,
        "type = \"none\"  # none | direct_bool | string_contains"
    )
    .unwrap();

    toml
}

/// Extracts a Carto table name from a URL, if present.
fn extract_carto_table(url: &str) -> String {
    // Normalize URL-encoded spaces for matching
    let normalized = url.replace('+', " ");
    let lower = normalized.to_lowercase();
    // Try to extract from ?q=SELECT * FROM {table}
    if let Some(idx) = lower.find("from ") {
        let rest = &normalized[idx + 5..];
        let end = rest
            .find(|c: char| c.is_whitespace() || c == '&')
            .unwrap_or(rest.len());
        return rest[..end].to_string();
    }
    "TODO".to_string()
}

/// Extracts the base Carto API URL (without query parameters).
fn extract_carto_base(url: &str) -> String {
    url.find('?')
        .map_or_else(|| url.to_string(), |idx| url[..idx].to_string())
}

/// Inserts a new `include_str!` entry into `registry.rs` content.
///
/// Finds the given section marker text and inserts the entry at the end of
/// that section (before the next section comment or the closing `];`).
fn insert_registry_entry(
    content: &str,
    include_line: &str,
    section_marker: &str,
) -> Option<String> {
    let lines: Vec<&str> = content.lines().collect();

    // Find the section marker line (matches substring)
    let section_idx = lines
        .iter()
        .position(|line| line.contains(section_marker))?;

    // Find the end of this section: either the next section header comment
    // (lines starting with whitespace + //) or the closing `];`
    let mut insert_idx = None;
    for (i, line) in lines.iter().enumerate().skip(section_idx + 1) {
        let trimmed = line.trim();
        // Section headers are comment lines containing a unicode box char or `--`
        let is_section_header =
            trimmed.starts_with("//") && (trimmed.contains('─') || trimmed.contains("--"));
        if is_section_header || trimmed == "];" {
            insert_idx = Some(i);
            break;
        }
    }

    let insert_idx = insert_idx?;

    let mut result = String::new();
    for (i, line) in lines.iter().enumerate() {
        if i == insert_idx {
            writeln!(result, "{include_line}").unwrap();
        }
        writeln!(result, "{line}").unwrap();
    }

    Some(result)
}

/// Increments the `EXPECTED_SOURCE_COUNT` constant in registry.rs content.
fn increment_source_count(content: &str) -> String {
    let re_pattern = "const EXPECTED_SOURCE_COUNT: usize = ";
    if let Some(idx) = content.find(re_pattern) {
        let after = &content[idx + re_pattern.len()..];
        if let Some(end) = after.find(';') {
            let num_str = &after[..end];
            if let Ok(count) = num_str.trim().parse::<usize>() {
                let new_count = count + 1;
                return format!(
                    "{}{}{}{}",
                    &content[..idx],
                    re_pattern,
                    new_count,
                    &content[idx + re_pattern.len() + end..]
                );
            }
        }
    }
    content.to_string()
}

// ---------------------------------------------------------------------------
// Verify command (stub)
// ---------------------------------------------------------------------------

/// Health-checks existing sources by probing their endpoints.
#[allow(clippy::unused_async)]
async fn cmd_verify(
    _database: &dyn Database,
    _source_id: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("verify: Not yet implemented");
    Ok(())
}

// ---------------------------------------------------------------------------
// Suggest command (stub)
// ---------------------------------------------------------------------------

/// Suggests next discovery actions based on current database state.
#[allow(clippy::unused_async)]
async fn cmd_suggest(
    _database: &dyn Database,
    _region: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("suggest: Not yet implemented");
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Truncates a string to `max_len` characters, appending "…" if it was
/// longer than the limit.
#[must_use]
fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_owned()
    } else {
        let mut result = s[..max_len.saturating_sub(1)].to_owned();
        result.push('…');
        result
    }
}

/// Formats an `Option<bool>` as a short character for table display.
#[must_use]
const fn bool_char(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "Y",
        Some(false) => "N",
        None => "-",
    }
}

/// Formats an `Option<bool>` as a human-readable string for detail display.
#[must_use]
const fn bool_str(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "Yes",
        Some(false) => "No",
        None => "-",
    }
}

/// Formats an `Option<T: Display>` as a string, using "-" for `None`.
fn opt_display<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map_or_else(|| "-".to_string(), |v| v.to_string())
}

/// Formats an `Option<f64>` with 2 decimal places, using "-" for `None`.
fn opt_display_f64(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |v| format!("{v:.2}"))
}
