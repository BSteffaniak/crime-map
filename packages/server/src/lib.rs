#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Actix-Web API server for the crime map application.
//!
//! Serves the REST API for querying crime data and static tile files
//! (`PMTiles`) for the `MapLibre` frontend. Sidebar queries are served
//! from a pre-generated `SQLite` database with R-tree spatial indexing.
//! AI-powered queries are served via SSE streaming from the `/api/ai/ask`
//! endpoint. Conversation history is persisted in a dedicated `SQLite`
//! database at `data/conversations.db`.

mod handlers;
pub mod interactive;

use actix_cors::Cors;
use actix_files::Files;
use actix_web::{App, HttpServer, middleware, web};
use crime_map_database::{db, run_migrations};
use crime_map_geography::queries as geo_queries;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use switchy_database::Database;
use switchy_database_connection::init_sqlite_rusqlite;

/// Simple round-robin pool of read-only `DuckDB` connections.
///
/// `duckdb::Connection` is `Send` but not `Sync`, so each connection is
/// wrapped in a `Mutex`. The pool hands out connections round-robin via
/// an atomic counter, allowing concurrent queries on different
/// connections.
pub struct DuckDbPool {
    connections: Vec<Mutex<duckdb::Connection>>,
    next: AtomicUsize,
}

impl DuckDbPool {
    /// Opens `size` read-only connections to the `DuckDB` file at `path`.
    ///
    /// # Panics
    ///
    /// Panics if any connection fails to open.
    #[must_use]
    pub fn new(path: &Path, size: usize) -> Self {
        let connections = (0..size)
            .map(|_| {
                let conn = duckdb::Connection::open_with_flags(
                    path,
                    duckdb::Config::default()
                        .access_mode(duckdb::AccessMode::ReadOnly)
                        .expect("Failed to set DuckDB access mode"),
                )
                .expect("Failed to open DuckDB connection for pool");
                Mutex::new(conn)
            })
            .collect();
        Self {
            connections,
            next: AtomicUsize::new(0),
        }
    }

    /// Acquires the next connection from the pool (round-robin).
    ///
    /// # Panics
    ///
    /// Panics if the `Mutex` is poisoned.
    pub fn acquire(&self) -> std::sync::MutexGuard<'_, duckdb::Connection> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        self.connections[idx]
            .lock()
            .expect("DuckDB pool mutex poisoned")
    }
}

/// Shared application state.
pub struct AppState {
    /// `PostGIS` database connection for primary queries.
    pub db: Arc<dyn Database>,
    /// `SQLite` database for sidebar queries (pre-generated, read-only).
    pub sidebar_db: Arc<dyn Database>,
    /// `DuckDB` connection for fast pre-aggregated count queries.
    /// `duckdb::Connection` is `Send` but not `Sync`, so a `Mutex` is needed.
    pub count_db: Arc<Mutex<duckdb::Connection>>,
    /// Pool of read-only `DuckDB` connections for H3 hexbin queries.
    pub h3_pool: Arc<DuckDbPool>,
    /// AI agent context (available cities, date range).
    pub ai_context: Arc<crime_map_ai::agent::AgentContext>,
    /// `SQLite` database for persistent AI conversation storage.
    pub conversations_db: Arc<dyn Database>,
}

/// Starts the crime map API server.
///
/// Connects to the `PostGIS` database, runs migrations, opens the sidebar
/// `SQLite` and `DuckDB` databases, builds the AI agent context, and
/// starts the Actix-Web HTTP server. This is a regular async function —
/// the caller is responsible for providing the async runtime (e.g. via
/// `#[actix_web::main]`).
///
/// # Errors
///
/// Returns an `std::io::Result` error if the HTTP server fails to bind or
/// encounters a runtime error.
///
/// # Panics
///
/// Panics if the database connection fails, migrations fail, the sidebar
/// `SQLite` database cannot be opened, or the `DuckDB` count database
/// cannot be opened.
#[allow(clippy::future_not_send)]
pub async fn run_server() -> std::io::Result<()> {
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

    log::info!("Opening H3 hexbin DuckDB connection pool...");
    let h3_path = Path::new("data/generated/h3.duckdb");
    let h3_pool = DuckDbPool::new(h3_path, 4);

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

    log::info!("Opening conversations database...");
    let conversations_db =
        crime_map_conversations::open_db(Path::new(crime_map_conversations::DEFAULT_DB_PATH))
            .await
            .expect("Failed to open conversations database");

    let state = web::Data::new(AppState {
        db: Arc::from(db_conn),
        sidebar_db: Arc::from(sidebar_db),
        count_db: Arc::new(Mutex::new(count_db)),
        h3_pool: Arc::new(h3_pool),
        ai_context: Arc::new(ai_context),
        conversations_db: Arc::from(conversations_db),
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
                    .route("/clusters", web::get().to(handlers::clusters))
                    .route("/hexbins", web::get().to(handlers::hexbins))
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
