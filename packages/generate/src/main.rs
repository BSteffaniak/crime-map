#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions)]

//! CLI tool for generating `PMTiles` and `FlatGeobuf` files from `PostGIS` data.
//!
//! Exports crime incident data as `GeoJSONSeq`, then runs tippecanoe (`PMTiles`)
//! and ogr2ogr (`FlatGeobuf`) to produce optimized spatial data files for the
//! frontend.

use std::path::Path;
use std::process::Command;

use clap::{Parser, Subcommand};
use crime_map_database::db;
use moosicbox_json_utils::database::ToValue as _;
use switchy_database::Database;

#[derive(Parser)]
#[command(name = "crime_map_generate", about = "Tile generation tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate `PMTiles` from `PostGIS` data
    Pmtiles,
    /// Generate `FlatGeobuf` files from `PostGIS` data
    Flatgeobuf,
    /// Generate both `PMTiles` and `FlatGeobuf`
    All,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let cli = Cli::parse();

    let db = db::connect_from_env().await?;
    std::fs::create_dir_all("data/generated")?;

    match cli.command {
        Commands::Pmtiles => generate_pmtiles(db.as_ref()).await?,
        Commands::Flatgeobuf => generate_flatgeobuf(db.as_ref()).await?,
        Commands::All => {
            generate_pmtiles(db.as_ref()).await?;
            generate_flatgeobuf(db.as_ref()).await?;
        }
    }

    Ok(())
}

/// Exports incidents as `GeoJSONSeq` and generates `PMTiles` via tippecanoe.
async fn generate_pmtiles(db: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let geojsonseq_path = "data/generated/incidents.geojsonseq";

    log::info!("Exporting incidents to GeoJSONSeq...");
    export_geojsonseq(db, geojsonseq_path).await?;

    log::info!("Running tippecanoe to generate PMTiles...");

    let output_path = "data/generated/incidents.pmtiles";

    let status = Command::new("tippecanoe")
        .args([
            "-o",
            output_path,
            "--force",
            "--no-feature-limit",
            "--no-tile-size-limit",
            "--minimum-zoom=0",
            "--maximum-zoom=14",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            "--layer=incidents",
            geojsonseq_path,
        ])
        .status()?;

    if !status.success() {
        return Err("tippecanoe failed".into());
    }

    log::info!("PMTiles generated: {output_path}");
    Ok(())
}

/// Exports incidents as a `FlatGeobuf` file via ogr2ogr.
async fn generate_flatgeobuf(db: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let geojsonseq_path = "data/generated/incidents.geojsonseq";

    // Ensure GeoJSONSeq exists
    if !Path::new(geojsonseq_path).exists() {
        log::info!("GeoJSONSeq not found, exporting...");
        export_geojsonseq(db, geojsonseq_path).await?;
    }

    log::info!("Running ogr2ogr to generate FlatGeobuf...");

    let output_path = "data/generated/incidents.fgb";

    let status = Command::new("ogr2ogr")
        .args([
            "-f",
            "FlatGeobuf",
            "-if",
            "GeoJSONSeq",
            output_path,
            geojsonseq_path,
            "-nln",
            "incidents",
            "-lco",
            "SPATIAL_INDEX=YES",
        ])
        .status()?;

    if !status.success() {
        return Err("ogr2ogr failed".into());
    }

    log::info!("FlatGeobuf generated: {output_path}");
    Ok(())
}

/// Exports all incidents from `PostGIS` as newline-delimited `GeoJSON`.
async fn export_geojsonseq(
    db: &dyn Database,
    output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let rows = db
        .query_raw_params(
            "SELECT i.id, i.source_incident_id,
                    c.name as subcategory, c.severity,
                    pc.name as category,
                    i.occurred_at, i.description, i.block_address,
                    i.city, i.state, i.arrest_made, i.domestic,
                    i.location_type,
                    ST_X(i.location::geometry) as longitude,
                    ST_Y(i.location::geometry) as latitude
             FROM crime_incidents i
             JOIN crime_categories c ON i.category_id = c.id
             LEFT JOIN crime_categories pc ON c.parent_id = pc.id
             ORDER BY i.occurred_at DESC",
            &[],
        )
        .await?;

    let mut output = String::new();
    for row in &rows {
        let lng: f64 = row.to_value("longitude").unwrap_or(0.0);
        let lat: f64 = row.to_value("latitude").unwrap_or(0.0);
        let id: i64 = row.to_value("id").unwrap_or(0);
        let subcategory: String = row.to_value("subcategory").unwrap_or_default();
        let category: String = row.to_value("category").unwrap_or_default();
        let severity: i32 = row.to_value("severity").unwrap_or(1);
        let city: String = row.to_value("city").unwrap_or_default();
        let state: String = row.to_value("state").unwrap_or_default();
        let arrest_made: Option<bool> = row.to_value("arrest_made").unwrap_or(None);
        let occurred_at: String = row.to_value("occurred_at").unwrap_or_default();

        let feature = serde_json::json!({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lng, lat]
            },
            "properties": {
                "id": id,
                "subcategory": subcategory,
                "category": category,
                "severity": severity,
                "city": city,
                "state": state,
                "arrest": arrest_made,
                "date": occurred_at,
            }
        });

        output.push_str(&serde_json::to_string(&feature)?);
        output.push('\n');
    }

    std::fs::write(output_path, &output)?;
    log::info!("Exported {} features to {output_path}", rows.len());
    Ok(())
}
