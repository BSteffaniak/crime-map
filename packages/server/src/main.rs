#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! CLI entry point for the crime map API server.

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    crime_map_server::run_server().await
}
