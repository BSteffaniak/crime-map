#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! CLI tool for generating `PMTiles`, cluster tiles, and `FlatGeobuf` files
//! from `PostGIS` data.
//!
//! Exports crime incident data as `GeoJSONSeq`, then runs tippecanoe
//! (`PMTiles` for heatmap/points and clustered tiles) and ogr2ogr
//! (`FlatGeobuf` for sidebar spatial queries) to produce optimized spatial
//! data files for the frontend.
//!
//! Uses keyset pagination and streaming writes to keep memory usage constant
//! regardless of dataset size.

use std::fmt::Write as _;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::process::Command;

use clap::{Args, Parser, Subcommand};
use crime_map_database::db;
use crime_map_source::registry::all_sources;
use moosicbox_json_utils::database::ToValue as _;
use switchy_database::{Database, DatabaseValue};

/// Number of rows to fetch per database query batch.
const BATCH_SIZE: i64 = 10_000;

/// Returns the workspace root directory, resolved at compile time from
/// `CARGO_MANIFEST_DIR`. This ensures output paths are always relative to
/// the project root regardless of the caller's working directory.
#[must_use]
fn output_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("Failed to find project root from CARGO_MANIFEST_DIR")
        .join("data/generated")
}

#[derive(Parser)]
#[command(name = "crime_map_generate", about = "Tile generation tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

/// Shared arguments for all generate subcommands.
#[derive(Args)]
struct GenerateArgs {
    /// Maximum number of records to export (useful for testing).
    #[arg(long)]
    limit: Option<u64>,

    /// Comma-separated list of source IDs to include (e.g., "chicago,la,sf").
    /// Only incidents from these sources will be exported.
    #[arg(long)]
    sources: Option<String>,

    /// Keep the intermediate `.geojsonseq` file after generation instead of
    /// deleting it.
    #[arg(long)]
    keep_intermediate: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate `PMTiles` from `PostGIS` data (heatmap + individual points)
    Pmtiles {
        #[command(flatten)]
        args: GenerateArgs,
    },
    /// Generate clustered `PMTiles` for mid-zoom (zoom 8-11) via tippecanoe
    Clusters {
        #[command(flatten)]
        args: GenerateArgs,
    },
    /// Generate `FlatGeobuf` files from `PostGIS` data (sidebar spatial queries)
    Flatgeobuf {
        #[command(flatten)]
        args: GenerateArgs,
    },
    /// Generate all output files (`PMTiles`, clusters, and `FlatGeobuf`)
    All {
        #[command(flatten)]
        args: GenerateArgs,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let cli = Cli::parse();

    let db = db::connect_from_env().await?;
    let dir = output_dir();
    std::fs::create_dir_all(&dir)?;

    match cli.command {
        Commands::Pmtiles { args } => {
            let source_ids = resolve_source_ids(db.as_ref(), &args).await?;
            generate_pmtiles(db.as_ref(), &args, &source_ids, &dir).await?;
            cleanup_intermediate(&args, &dir);
        }
        Commands::Clusters { args } => {
            let source_ids = resolve_source_ids(db.as_ref(), &args).await?;
            generate_cluster_tiles(db.as_ref(), &args, &source_ids, &dir).await?;
            cleanup_intermediate(&args, &dir);
        }
        Commands::Flatgeobuf { args } => {
            let source_ids = resolve_source_ids(db.as_ref(), &args).await?;
            generate_flatgeobuf(db.as_ref(), &args, &source_ids, &dir).await?;
            cleanup_intermediate(&args, &dir);
        }
        Commands::All { args } => {
            let source_ids = resolve_source_ids(db.as_ref(), &args).await?;
            generate_pmtiles(db.as_ref(), &args, &source_ids, &dir).await?;
            generate_cluster_tiles(db.as_ref(), &args, &source_ids, &dir).await?;
            generate_flatgeobuf(db.as_ref(), &args, &source_ids, &dir).await?;
            cleanup_intermediate(&args, &dir);
        }
    }

    Ok(())
}

/// Resolves `--sources` short IDs (e.g., "chicago") to database integer
/// `source_id` values by looking up each source's human-readable name in
/// `crime_sources`.
///
/// Returns an empty `Vec` if `--sources` was not provided (meaning: export
/// all sources).
///
/// # Errors
///
/// Returns an error if a provided source ID does not match any configured
/// source, or if the database lookup fails.
async fn resolve_source_ids(
    db: &dyn Database,
    args: &GenerateArgs,
) -> Result<Vec<i32>, Box<dyn std::error::Error>> {
    let Some(ref sources_str) = args.sources else {
        return Ok(Vec::new());
    };

    let requested: Vec<&str> = sources_str.split(',').map(str::trim).collect();
    let registry = all_sources();

    let mut source_ids = Vec::with_capacity(requested.len());
    for short_id in &requested {
        let Some(def) = registry.iter().find(|s| s.id() == *short_id) else {
            return Err(format!("Unknown source ID: {short_id}").into());
        };

        let rows = db
            .query_raw_params(
                "SELECT id FROM crime_sources WHERE name = $1",
                &[DatabaseValue::String(def.name().to_string())],
            )
            .await?;

        let Some(row) = rows.first() else {
            log::warn!(
                "Source '{}' ({}) not found in database — skipping",
                short_id,
                def.name()
            );
            continue;
        };

        let id: i32 = row
            .to_value("id")
            .map_err(|e| format!("Failed to parse source_id for {short_id}: {e}"))?;
        source_ids.push(id);
        log::info!("Resolved source '{short_id}' -> source_id {id}");
    }

    if source_ids.is_empty() {
        return Err("None of the requested sources were found in the database".into());
    }

    Ok(source_ids)
}

/// Deletes the intermediate `.geojsonseq` file unless `--keep-intermediate`
/// was specified.
fn cleanup_intermediate(args: &GenerateArgs, dir: &Path) {
    let path = dir.join("incidents.geojsonseq");
    if args.keep_intermediate {
        log::info!("Keeping intermediate file: {}", path.display());
        return;
    }
    if path.exists() {
        match std::fs::remove_file(&path) {
            Ok(()) => log::info!("Cleaned up intermediate file: {}", path.display()),
            Err(e) => log::warn!("Failed to remove intermediate file {}: {e}", path.display()),
        }
    }
}

/// Exports incidents as `GeoJSONSeq` and generates `PMTiles` via tippecanoe.
async fn generate_pmtiles(
    db: &dyn Database,
    args: &GenerateArgs,
    source_ids: &[i32],
    dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let geojsonseq_path = dir.join("incidents.geojsonseq");

    log::info!("Exporting incidents to GeoJSONSeq...");
    export_geojsonseq(db, &geojsonseq_path, args.limit, source_ids).await?;

    log::info!("Running tippecanoe to generate PMTiles...");

    let output_path = dir.join("incidents.pmtiles");

    let status = Command::new("tippecanoe")
        .args([
            "-o",
            &output_path.to_string_lossy(),
            "--force",
            "--no-feature-limit",
            "--no-tile-size-limit",
            "--minimum-zoom=0",
            "--maximum-zoom=14",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            "--layer=incidents",
            &geojsonseq_path.to_string_lossy(),
        ])
        .status()?;

    if !status.success() {
        return Err("tippecanoe failed".into());
    }

    log::info!("PMTiles generated: {}", output_path.display());
    Ok(())
}

/// Generates clustered `PMTiles` for mid-zoom levels (8-11) via tippecanoe.
///
/// Uses tippecanoe's built-in point clustering (`--cluster-distance=60`) to
/// aggregate nearby points. Each cluster feature receives automatic
/// `point_count` and `clustered` properties. The `severity` attribute is
/// summed across merged points via `--accumulate-attribute`.
///
/// The output is a static file suitable for CDN caching.
///
/// # Errors
///
/// Returns an error if the `GeoJSONSeq` export or tippecanoe fails.
async fn generate_cluster_tiles(
    db: &dyn Database,
    args: &GenerateArgs,
    source_ids: &[i32],
    dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let geojsonseq_path = dir.join("incidents.geojsonseq");

    // Ensure GeoJSONSeq exists
    if !geojsonseq_path.exists() {
        log::info!("GeoJSONSeq not found, exporting...");
        export_geojsonseq(db, &geojsonseq_path, args.limit, source_ids).await?;
    }

    log::info!("Running tippecanoe to generate cluster tiles...");

    let output_path = dir.join("clusters.pmtiles");

    let status = Command::new("tippecanoe")
        .args([
            "-o",
            &output_path.to_string_lossy(),
            "--force",
            "--no-feature-limit",
            "--no-tile-size-limit",
            "--minimum-zoom=8",
            "--maximum-zoom=11",
            "-r1",
            "--cluster-distance=60",
            "--accumulate-attribute=severity:sum",
            "--layer=clusters",
            &geojsonseq_path.to_string_lossy(),
        ])
        .status()?;

    if !status.success() {
        return Err("tippecanoe cluster generation failed".into());
    }

    log::info!("Cluster PMTiles generated: {}", output_path.display());
    Ok(())
}

/// Exports incidents as a `FlatGeobuf` file via ogr2ogr.
async fn generate_flatgeobuf(
    db: &dyn Database,
    args: &GenerateArgs,
    source_ids: &[i32],
    dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let geojsonseq_path = dir.join("incidents.geojsonseq");

    // Ensure GeoJSONSeq exists
    if !geojsonseq_path.exists() {
        log::info!("GeoJSONSeq not found, exporting...");
        export_geojsonseq(db, &geojsonseq_path, args.limit, source_ids).await?;
    }

    log::info!("Running ogr2ogr to generate FlatGeobuf...");

    let output_path = dir.join("incidents.fgb");

    let status = Command::new("ogr2ogr")
        .args([
            "-f",
            "FlatGeobuf",
            "-if",
            "GeoJSONSeq",
            &output_path.to_string_lossy(),
            &geojsonseq_path.to_string_lossy(),
            "-nln",
            "incidents",
            "-lco",
            "SPATIAL_INDEX=YES",
        ])
        .status()?;

    if !status.success() {
        return Err("ogr2ogr failed".into());
    }

    log::info!("FlatGeobuf generated: {}", output_path.display());
    Ok(())
}

/// Builds the SQL query and parameters for a single batch of incidents.
///
/// Uses keyset pagination (`WHERE i.id > $1 ORDER BY i.id ASC LIMIT $N`)
/// for efficient, index-backed iteration over the entire table.
fn build_batch_query(
    last_id: i64,
    batch_limit: i64,
    source_ids: &[i32],
) -> (String, Vec<DatabaseValue>) {
    let mut params: Vec<DatabaseValue> = Vec::new();
    let mut param_idx: usize = 1;

    // Always filter by id > last_id
    let mut where_clause = format!("i.id > ${param_idx}");
    params.push(DatabaseValue::Int64(last_id));
    param_idx += 1;

    // Optional source filter
    if !source_ids.is_empty() {
        let placeholders: Vec<String> = source_ids
            .iter()
            .map(|_| {
                let p = format!("${param_idx}");
                param_idx += 1;
                p
            })
            .collect();
        write!(
            where_clause,
            " AND i.source_id IN ({})",
            placeholders.join(", ")
        )
        .unwrap();
        for &sid in source_ids {
            params.push(DatabaseValue::Int32(sid));
        }
    }

    let limit_placeholder = format!("${param_idx}");
    params.push(DatabaseValue::Int64(batch_limit));

    let query = format!(
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
         WHERE {where_clause}
         ORDER BY i.id ASC
         LIMIT {limit_placeholder}"
    );

    (query, params)
}

/// Exports all incidents from `PostGIS` as newline-delimited `GeoJSON`,
/// using keyset pagination and streaming writes to keep memory constant.
async fn export_geojsonseq(
    db: &dyn Database,
    output_path: &Path,
    limit: Option<u64>,
    source_ids: &[i32],
) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::create(output_path)?;
    let mut writer = BufWriter::new(file);

    let mut last_id: i64 = 0;
    let mut total_count: u64 = 0;
    let mut remaining = limit;

    loop {
        #[allow(clippy::cast_sign_loss)]
        let batch_limit = match remaining {
            Some(0) => break,
            Some(r) => i64::try_from(r.min(BATCH_SIZE as u64))?,
            None => BATCH_SIZE,
        };

        let (query, params) = build_batch_query(last_id, batch_limit, source_ids);

        let rows = db.query_raw_params(&query, &params).await?;

        if rows.is_empty() {
            break;
        }

        #[allow(clippy::cast_possible_truncation)]
        let batch_len = rows.len() as u64;

        for row in &rows {
            let id: i64 = row.to_value("id").unwrap_or(0);
            let lng: f64 = row.to_value("longitude").unwrap_or(0.0);
            let lat: f64 = row.to_value("latitude").unwrap_or(0.0);
            let source_incident_id: String = row.to_value("source_incident_id").unwrap_or_default();
            let subcategory: String = row.to_value("subcategory").unwrap_or_default();
            let category: String = row.to_value("category").unwrap_or_default();
            let severity: i32 = row.to_value("severity").unwrap_or(1);
            let city: String = row.to_value("city").unwrap_or_default();
            let state: String = row.to_value("state").unwrap_or_default();
            let arrest_made: Option<bool> = row.to_value("arrest_made").unwrap_or(None);
            let description: Option<String> = row.to_value("description").unwrap_or(None);
            let block_address: Option<String> = row.to_value("block_address").unwrap_or(None);

            let occurred_at_naive: chrono::NaiveDateTime =
                row.to_value("occurred_at").unwrap_or_default();
            let occurred_at = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                occurred_at_naive,
                chrono::Utc,
            )
            .to_rfc3339();

            let feature = serde_json::json!({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lng, lat]
                },
                "properties": {
                    "id": id,
                    "sid": source_incident_id,
                    "subcategory": subcategory,
                    "category": category,
                    "severity": severity,
                    "city": city,
                    "state": state,
                    "arrest": arrest_made,
                    "date": occurred_at,
                    "desc": description,
                    "addr": block_address,
                }
            });

            serde_json::to_writer(&mut writer, &feature)?;
            writer.write_all(b"\n")?;

            last_id = id;
        }

        total_count += batch_len;

        if let Some(ref mut r) = remaining {
            *r = r.saturating_sub(batch_len);
        }

        log::info!("Exported {total_count} features so far...");

        #[allow(clippy::cast_sign_loss)]
        let batch_limit_u64 = batch_limit as u64;
        if batch_len < batch_limit_u64 {
            break;
        }
    }

    writer.flush()?;
    log::info!(
        "Exported {total_count} features to {}",
        output_path.display()
    );
    Ok(())
}
