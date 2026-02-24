#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Actix-Web API server for the crime map application.
//!
//! Serves the REST API for querying crime data and static tile files
//! (`PMTiles`) for the `MapLibre` frontend. All data is served from
//! pre-generated files: sidebar queries from `SQLite` (`incidents.db`),
//! fast counts from `DuckDB` (`counts.duckdb`), hexbin analytics from
//! `DuckDB` (`h3.duckdb`), AI analytics from `DuckDB`
//! (`analytics.duckdb`), and source metadata from `metadata.json`.
//!
//! AI-powered queries are served via SSE streaming from the `/api/ai/ask`
//! endpoint. Conversation history is persisted in a dedicated `SQLite`
//! database at `data/conversations.db`.
//!
//! ## Graceful Startup
//!
//! The server starts immediately and serves the health endpoint even if
//! the pre-generated data files (`incidents.db`, `counts.duckdb`,
//! `h3.duckdb`, `analytics.duckdb`) are not yet present. A background
//! task polls for the files and initializes the data connections once they
//! appear. Endpoints that depend on the data return `503 Service
//! Unavailable` until the data is ready.
//!
//! ## No Runtime `PostgreSQL`
//!
//! The server runs entirely from pre-generated files. No `PostGIS` or
//! `PostgreSQL` connection is needed at runtime. All data is materialized
//! during the `cargo generate all` step.

mod handlers;
pub mod interactive;

use actix_cors::Cors;
use actix_files::Files;
use actix_web::{App, HttpServer, middleware, web};
use crime_map_server_models::ApiSource;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
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
    /// # Errors
    ///
    /// Returns a `duckdb::Error` if any connection fails to open.
    pub fn new(path: &Path, size: usize) -> Result<Self, duckdb::Error> {
        let mut connections = Vec::with_capacity(size);
        for _ in 0..size {
            let conn = duckdb::Connection::open_with_flags(
                path,
                duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly)?,
            )?;
            connections.push(Mutex::new(conn));
        }
        Ok(Self {
            connections,
            next: AtomicUsize::new(0),
        })
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

/// Pre-generated data connections that are initialized lazily once data
/// files become available on disk.
pub struct DataState {
    /// `SQLite` database for sidebar queries (pre-generated, read-only).
    pub sidebar_db: Arc<dyn Database>,
    /// `DuckDB` connection for fast pre-aggregated count queries.
    /// `duckdb::Connection` is `Send` but not `Sync`, so a `Mutex` is needed.
    pub count_db: Arc<Mutex<duckdb::Connection>>,
    /// Pool of read-only `DuckDB` connections for H3 hexbin queries.
    pub h3_pool: Arc<DuckDbPool>,
    /// `DuckDB` connection for AI analytics tool queries.
    /// Contains denormalized incident data and reference tables.
    pub analytics_db: Arc<Mutex<duckdb::Connection>>,
}

/// Shared application state.
pub struct AppState {
    /// Pre-generated data connections. Starts empty and gets populated
    /// by the background file watcher once all data files are present.
    pub data: Arc<OnceLock<DataState>>,
    /// AI agent context (available cities, date range).
    pub ai_context: Arc<crime_map_ai::agent::AgentContext>,
    /// `SQLite` database for persistent AI conversation storage.
    pub conversations_db: Arc<dyn Database>,
    /// Pre-generated `SQLite` database for boundary name searches.
    /// `None` when `boundaries.db` is not present.
    pub boundaries_db: Option<Arc<dyn Database>>,
    /// Pre-generated source metadata loaded from `metadata.json`.
    pub sources: Vec<ApiSource>,
}

/// Required data files that must all be present before the server can
/// serve map data.
const REQUIRED_DATA_FILES: &[&str] = &[
    "incidents.db",
    "counts.duckdb",
    "h3.duckdb",
    "analytics.duckdb",
];

/// Interval between file existence checks when data files are missing.
const DATA_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Initializes the data connections from the pre-generated files in the
/// given directory.
///
/// # Errors
///
/// Returns an error if any database file cannot be opened.
fn init_data_state(dir: &Path) -> Result<DataState, Box<dyn std::error::Error>> {
    log::info!("Opening sidebar SQLite database...");
    let sidebar_path = dir.join("incidents.db");
    let sidebar_db = init_sqlite_rusqlite(Some(&sidebar_path))
        .map_err(|e| format!("Failed to open sidebar SQLite: {e}"))?;

    log::info!("Opening DuckDB count database...");
    let count_path = dir.join("counts.duckdb");
    let count_db = duckdb::Connection::open_with_flags(
        &count_path,
        duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .map_err(|e| format!("Failed to set DuckDB access mode: {e}"))?,
    )
    .map_err(|e| format!("Failed to open DuckDB count database: {e}"))?;

    log::info!("Opening H3 hexbin DuckDB connection pool...");
    let h3_path = dir.join("h3.duckdb");
    let h3_pool =
        DuckDbPool::new(&h3_path, 4).map_err(|e| format!("Failed to open H3 DuckDB pool: {e}"))?;

    log::info!("Opening analytics DuckDB database...");
    let analytics_path = dir.join("analytics.duckdb");
    let analytics_db = duckdb::Connection::open_with_flags(
        &analytics_path,
        duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .map_err(|e| format!("Failed to set DuckDB access mode: {e}"))?,
    )
    .map_err(|e| format!("Failed to open analytics DuckDB: {e}"))?;

    Ok(DataState {
        sidebar_db: Arc::from(sidebar_db),
        count_db: Arc::new(Mutex::new(count_db)),
        h3_pool: Arc::new(h3_pool),
        analytics_db: Arc::new(Mutex::new(analytics_db)),
    })
}

/// Spawns a background task that waits for data files to appear and
/// initializes the [`DataState`] once they are all present.
///
/// If the files already exist at startup, the `OnceLock` is set
/// immediately. Otherwise, the task polls every
/// [`DATA_POLL_INTERVAL`] seconds until the files appear.
fn spawn_data_watcher(data_lock: Arc<OnceLock<DataState>>, data_dir: PathBuf) {
    tokio::spawn(async move {
        loop {
            let missing: Vec<&&str> = REQUIRED_DATA_FILES
                .iter()
                .filter(|f| !data_dir.join(f).exists())
                .collect();

            if missing.is_empty() {
                match init_data_state(&data_dir) {
                    Ok(state) => {
                        if data_lock.set(state).is_ok() {
                            log::info!("All data files loaded successfully");
                        }
                        return;
                    }
                    Err(e) => {
                        log::error!("Failed to open data files (will retry): {e}");
                    }
                }
            } else {
                log::info!(
                    "Waiting for data files: {}",
                    missing
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }

            tokio::time::sleep(DATA_POLL_INTERVAL).await;
        }
    });
}

/// Pre-generated metadata loaded from `metadata.json`.
///
/// Contains the AI agent context and source metadata for the
/// `/api/sources` endpoint.
struct LoadedMetadata {
    context: crime_map_ai::agent::AgentContext,
    sources: Vec<ApiSource>,
}

/// Loads metadata from a pre-generated `metadata.json` file.
///
/// The file is produced by `cargo generate all` and contains:
/// - `cities`: array of `[city, state]` pairs
/// - `minDate` / `maxDate`: dataset date range
/// - `sources`: array of source metadata objects
///
/// Falls back to empty defaults if the file is missing or malformed.
fn load_metadata(dir: &Path) -> LoadedMetadata {
    let path = dir.join("metadata.json");
    let Ok(contents) = std::fs::read_to_string(&path) else {
        log::warn!(
            "No metadata.json found at {}; AI context will be empty",
            path.display()
        );
        return LoadedMetadata {
            context: crime_map_ai::agent::AgentContext {
                available_cities: Vec::new(),
                min_date: None,
                max_date: None,
            },
            sources: Vec::new(),
        };
    };

    let Ok(value) = serde_json::from_str::<serde_json::Value>(&contents) else {
        log::warn!("Failed to parse metadata.json; AI context will be empty");
        return LoadedMetadata {
            context: crime_map_ai::agent::AgentContext {
                available_cities: Vec::new(),
                min_date: None,
                max_date: None,
            },
            sources: Vec::new(),
        };
    };

    let cities: Vec<(String, String)> = value["cities"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let pair = v.as_array()?;
                    let city = pair.first()?.as_str()?.to_string();
                    let state = pair.get(1)?.as_str()?.to_string();
                    Some((city, state))
                })
                .collect()
        })
        .unwrap_or_default();

    let min_date = value["minDate"].as_str().map(String::from);
    let max_date = value["maxDate"].as_str().map(String::from);

    let sources: Vec<ApiSource> = value["sources"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| serde_json::from_value(v.clone()).ok())
                .collect()
        })
        .unwrap_or_default();

    log::info!(
        "Loaded metadata: {} cities, {} sources, date range {:?} to {:?}",
        cities.len(),
        sources.len(),
        min_date,
        max_date
    );

    LoadedMetadata {
        context: crime_map_ai::agent::AgentContext {
            available_cities: cities,
            min_date,
            max_date,
        },
        sources,
    }
}

/// Starts the crime map API server.
///
/// The server starts immediately and begins serving the health endpoint
/// and frontend static files. Data-dependent endpoints (`/api/sidebar`,
/// `/api/hexbins`, `/api/clusters`) become available once the
/// pre-generated data files appear on the volume.
///
/// All data is served from pre-generated files (`SQLite`, `DuckDB`,
/// `PMTiles`, `metadata.json`). No `PostGIS` connection is needed at
/// runtime.
///
/// # Errors
///
/// Returns an `std::io::Result` error if the HTTP server fails to bind or
/// encounters a runtime error.
///
/// # Panics
///
/// Panics if the conversations database cannot be opened.
#[allow(clippy::future_not_send, clippy::too_many_lines)]
pub async fn run_server() -> std::io::Result<()> {
    pretty_env_logger::init_custom_env("RUST_LOG");

    let data_dir = PathBuf::from("data/generated");

    // Initialize data state lazily via OnceLock
    let data = Arc::new(OnceLock::new());

    // Try to load data immediately if files exist, otherwise spawn watcher
    let all_files_exist = REQUIRED_DATA_FILES
        .iter()
        .all(|f| data_dir.join(f).exists());

    if all_files_exist {
        match init_data_state(&data_dir) {
            Ok(state) => {
                if data.set(state).is_err() {
                    log::warn!("Data state already initialized (race condition)");
                }
                log::info!("Data files loaded at startup");
            }
            Err(e) => {
                log::error!("Failed to open data files at startup: {e}");
                log::info!("Will retry in background...");
                spawn_data_watcher(Arc::clone(&data), data_dir.clone());
            }
        }
    } else {
        let missing: Vec<&&str> = REQUIRED_DATA_FILES
            .iter()
            .filter(|f| !data_dir.join(f).exists())
            .collect();
        log::info!(
            "Data files not yet available (missing: {}); will poll until ready",
            missing
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
        spawn_data_watcher(Arc::clone(&data), data_dir.clone());
    }

    // Load metadata from pre-generated file
    log::info!("Loading metadata...");
    let metadata = load_metadata(&data_dir);

    log::info!("Opening conversations database...");
    let conversations_db =
        crime_map_conversations::open_db(Path::new(crime_map_conversations::DEFAULT_DB_PATH))
            .await
            .expect("Failed to open conversations database");

    let boundaries_db_path = data_dir.join("boundaries.db");
    let boundaries_db: Option<Arc<dyn Database>> = if boundaries_db_path.exists() {
        match init_sqlite_rusqlite(Some(&boundaries_db_path)) {
            Ok(db) => {
                log::info!("Opened boundaries search database");
                Some(Arc::from(db))
            }
            Err(e) => {
                log::warn!("Failed to open boundaries.db: {e}");
                None
            }
        }
    } else {
        log::info!("boundaries.db not found; boundary search will be unavailable");
        None
    };

    let state = web::Data::new(AppState {
        data,
        ai_context: Arc::new(metadata.context),
        conversations_db: Arc::from(conversations_db),
        boundaries_db,
        sources: metadata.sources,
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
                    .route("/source-counts", web::get().to(handlers::source_counts))
                    .route("/sidebar", web::get().to(handlers::sidebar))
                    .route("/clusters", web::get().to(handlers::clusters))
                    .route("/hexbins", web::get().to(handlers::hexbins))
                    .route("/boundary-counts", web::get().to(handlers::boundary_counts))
                    .route(
                        "/boundaries/search",
                        web::get().to(handlers::boundary_search),
                    )
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
