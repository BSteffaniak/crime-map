#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Actix-Web API server for the crime map application.
//!
//! Serves the REST API for querying crime data and static tile files
//! (`PMTiles`) for the `MapLibre` frontend. Sidebar queries are served
//! from a pre-generated `SQLite` database with R-tree spatial indexing.
//! AI-powered queries are served via SSE streaming from the `/api/ai/ask`
//! endpoint.

mod handlers;

use actix_cors::Cors;
use actix_files::Files;
use actix_web::{App, HttpServer, middleware, web};
use crime_map_ai::providers::Message;
use crime_map_database::{db, run_migrations};
use crime_map_geography::queries as geo_queries;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use switchy_database::Database;
use switchy_database_connection::init_sqlite_rusqlite;

/// A stored conversation session.
pub struct ConversationSession {
    /// The full LLM message history (user, assistant, tool calls, tool results).
    pub messages: Vec<Message>,
    /// Unix timestamp (seconds) of last access.
    pub last_accessed: u64,
}

/// Maximum session idle time before expiry (30 minutes).
const SESSION_EXPIRY_SECS: u64 = 30 * 60;

/// Shared application state.
pub struct AppState {
    /// `PostGIS` database connection for primary queries.
    pub db: Arc<dyn Database>,
    /// `SQLite` database for sidebar queries (pre-generated, read-only).
    pub sidebar_db: Arc<dyn Database>,
    /// `DuckDB` connection for fast pre-aggregated count queries.
    /// `duckdb::Connection` is `Send` but not `Sync`, so a `Mutex` is needed.
    pub count_db: Arc<Mutex<duckdb::Connection>>,
    /// AI agent context (available cities, date range).
    pub ai_context: Arc<crime_map_ai::agent::AgentContext>,
    /// Active conversation sessions keyed by conversation ID.
    pub sessions: Arc<RwLock<BTreeMap<String, ConversationSession>>>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    pretty_env_logger::init_custom_env("RUST_LOG");

    log::info!("Connecting to database...");
    let db_conn = db::connect_from_env()
        .await
        .expect("Failed to connect to database");

    log::info!("Running migrations...");
    run_migrations(db_conn.as_ref())
        .await
        .expect("Failed to run migrations");

    log::info!("Opening sidebar SQLite database...");
    let sidebar_path = Path::new("data/generated/incidents.db");
    let sidebar_db =
        init_sqlite_rusqlite(Some(sidebar_path)).expect("Failed to open sidebar SQLite database");

    log::info!("Opening DuckDB count database...");
    let count_path = Path::new("data/generated/counts.duckdb");
    let count_db = duckdb::Connection::open_with_flags(
        count_path,
        duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .expect("Failed to set DuckDB access mode"),
    )
    .expect("Failed to open DuckDB count database");

    // Build AI context: discover available cities and date range
    log::info!("Building AI context...");
    let available_cities = geo_queries::get_available_cities(db_conn.as_ref())
        .await
        .unwrap_or_default();
    let (min_date, max_date) = geo_queries::get_data_date_range(db_conn.as_ref())
        .await
        .unwrap_or((None, None));
    let ai_context = crime_map_ai::agent::AgentContext {
        available_cities,
        min_date,
        max_date,
    };

    let state = web::Data::new(AppState {
        db: Arc::from(db_conn),
        sidebar_db: Arc::from(sidebar_db),
        count_db: Arc::new(Mutex::new(count_db)),
        ai_context: Arc::new(ai_context),
        sessions: Arc::new(RwLock::new(BTreeMap::new())),
    });

    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    log::info!("Starting server on {bind_addr}:{port}");

    HttpServer::new(move || {
        let cors = Cors::permissive();

        App::new()
            .wrap(cors)
            .wrap(middleware::Logger::default())
            .app_data(state.clone())
            .service(
                web::scope("/api")
                    .route("/health", web::get().to(handlers::health))
                    .route("/categories", web::get().to(handlers::categories))
                    .route("/incidents", web::get().to(handlers::incidents))
                    .route("/sources", web::get().to(handlers::sources))
                    .route("/sidebar", web::get().to(handlers::sidebar))
                    .route("/ai/ask", web::post().to(handlers::ai_ask)),
            )
            // Serve generated tile data
            .service(Files::new("/tiles", "data/generated").show_files_listing())
            // Serve frontend static files (production)
            .service(Files::new("/", "app/dist").index_file("index.html"))
    })
    .bind((bind_addr, port))?
    .run()
    .await
}
