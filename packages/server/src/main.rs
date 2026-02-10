#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Actix-Web API server for the crime map application.
//!
//! Serves the REST API for querying crime data and static tile files
//! (`PMTiles`) for the `MapLibre` frontend. Sidebar queries are served
//! from a pre-generated `SQLite` database with R-tree spatial indexing.

mod handlers;

use actix_cors::Cors;
use actix_files::Files;
use actix_web::{App, HttpServer, middleware, web};
use crime_map_database::{db, run_migrations};
use std::path::Path;
use std::sync::Arc;
use switchy_database::Database;
use switchy_database_connection::init_sqlite_rusqlite;

/// Shared application state.
pub struct AppState {
    /// `PostGIS` database connection for primary queries.
    pub db: Arc<dyn Database>,
    /// `SQLite` database for sidebar queries (pre-generated, read-only).
    pub sidebar_db: Arc<dyn Database>,
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

    let state = web::Data::new(AppState {
        db: Arc::from(db_conn),
        sidebar_db: Arc::from(sidebar_db),
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
                    .route("/sidebar", web::get().to(handlers::sidebar)),
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
