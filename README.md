# Crime Map

Interactive map visualizing public crime data from US cities. Rust backend with PostGIS, React + MapLibre GL JS frontend.

## Architecture

```
Browser (MapLibre GL JS)
  |
  |-- Zoom 0-7:   Heatmap layer (PMTiles vector tiles, server-side filter)
  |-- Zoom 8-11:  Supercluster (FlatGeobuf spatial query in Web Worker)
  |-- Zoom 12+:   Individual points (PMTiles vector tiles)
  |
Actix-Web API server (port 8080)
  |-- /api/*         REST endpoints (incidents, categories, sources)
  |-- /tiles/*       Static PMTiles + FlatGeobuf files
  |
PostGIS (port 5440)
  |-- Spatial indexes on incident locations
  |-- Crime category taxonomy (seeded via migrations)
```

## Prerequisites

**Recommended:** Use [Nix](https://nixos.org/) with the included `flake.nix` -- it provides all dependencies automatically via `direnv` or `nix develop`.

**Manual setup** requires:

- Rust (stable, 2024 edition)
- [Bun](https://bun.sh/) (frontend package manager and bundler)
- Docker or Podman (for PostGIS)
- [tippecanoe](https://github.com/felt/tippecanoe) (PMTiles generation)
- [GDAL](https://gdal.org/) (ogr2ogr for FlatGeobuf generation)
- PostgreSQL client (`psql`, for debugging)

## Quick Start

### 1. Start the database

```sh
docker compose up -d
```

This runs PostGIS 17 on port **5440** with database `crime_map`, user `postgres`, password `postgres`.

### 2. Run migrations

```sh
cargo ingest migrate
```

Creates tables (`crime_sources`, `crime_categories`, `crime_incidents`, `county_stats`) and seeds the category taxonomy.

### 3. Ingest crime data

```sh
# Test with a small sample from each source
cargo ingest sync-all --limit 100

# Full sync (all records from all 9 sources)
cargo ingest sync-all

# Sync a single source
cargo ingest sync chicago_pd --limit 1000
```

### 4. Generate map tiles

```sh
cargo generate all
```

Exports incidents from PostGIS as GeoJSONSeq, then generates:

- `data/generated/incidents.pmtiles` -- vector tiles for heatmap and point layers
- `data/generated/incidents.fgb` -- FlatGeobuf for client-side spatial queries

### 5. Build the frontend

```sh
cd app && bun install && bun run build
```

### 6. Start the server

```sh
cargo server
```

Open http://localhost:8080 in your browser.

## Data Sources

| ID           | City              | API Type       | Dataset                                                                                                         |
| ------------ | ----------------- | -------------- | --------------------------------------------------------------------------------------------------------------- |
| `chicago_pd` | Chicago, IL       | Socrata        | [ijzp-q8t2](https://data.cityofchicago.org/resource/ijzp-q8t2)                                                  |
| `la_pd`      | Los Angeles, CA   | Socrata        | [2nrs-mtv8](https://data.lacity.org/resource/2nrs-mtv8)                                                         |
| `sf_pd`      | San Francisco, CA | Socrata        | [wg3w-h783](https://data.sfgov.org/resource/wg3w-h783)                                                          |
| `seattle_pd` | Seattle, WA       | Socrata        | [tazs-3rd5](https://data.seattle.gov/resource/tazs-3rd5)                                                        |
| `nyc_pd`     | New York, NY      | Socrata        | [5uac-w243](https://data.cityofnewyork.us/resource/5uac-w243)                                                   |
| `denver_pd`  | Denver, CO        | Socrata        | [j6g8-fkyh](https://data.denvergov.org/resource/j6g8-fkyh)                                                      |
| `dc_mpd`     | Washington, DC    | ArcGIS REST    | [MPD MapServer](https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/8)                           |
| `philly_pd`  | Philadelphia, PA  | Carto SQL      | [phl.carto.com](https://phl.carto.com/api/v2/sql)                                                               |
| `boston_pd`  | Boston, MA        | CKAN Datastore | [data.boston.gov](https://data.boston.gov/dataset/crime-incident-reports-august-2015-to-date-source-new-system) |

## CLI Reference

Cargo aliases are defined in `.cargo/config.toml`. All commands run in release mode by default; append `:debug` for debug builds (e.g., `cargo ingest:debug`).

### `cargo ingest`

```
cargo ingest migrate              Run database migrations
cargo ingest sources              List all configured data sources
cargo ingest sync <SOURCE_ID>     Sync a single source
cargo ingest sync-all             Sync all sources
  --limit <N>                     Max records per source (for testing)
```

### `cargo generate`

```
cargo generate all                Generate PMTiles and FlatGeobuf
cargo generate pmtiles            Generate PMTiles only
cargo generate flatgeobuf         Generate FlatGeobuf only
```

### `cargo server`

Starts the Actix-Web server. API endpoints:

```
GET /api/health                   Health check
GET /api/incidents                Query incidents (supports bbox, date range, severity filters)
GET /api/categories               List crime categories
GET /api/sources                  List data sources and sync status
```

## Project Structure

```
packages/
  crime/models/     Crime taxonomy types (CrimeCategory, CrimeSubcategory, CrimeSeverity)
  source/models/    Source types (NormalizedIncident, SourceConfig, SourceType)
  source/           CrimeSource trait, shared fetchers (Socrata, ArcGIS), all source implementations
  database/models/  Query types (BoundingBox, IncidentQuery, IncidentRow)
  database/         PostGIS migrations and spatial queries
  ingest/models/    Ingestion types (FetchConfig, ImportResult)
  ingest/           CLI binary for data ingestion (migrate, sync, sync-all)
  generate/         PMTiles/FlatGeobuf generation via tippecanoe and ogr2ogr
  server/models/    API request/response types
  server/           Actix-Web HTTP server
app/                Vite + React + TypeScript + TailwindCSS + MapLibre GL JS frontend
```

## Environment Variables

| Variable       | Default                                                 | Description                                                 |
| -------------- | ------------------------------------------------------- | ----------------------------------------------------------- |
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5440/crime_map` | PostgreSQL connection string                                |
| `BIND_ADDR`    | `127.0.0.1`                                             | Server bind address                                         |
| `PORT`         | `8080`                                                  | Server port                                                 |
| `RUST_LOG`     | (none)                                                  | Log level (`info`, `debug`, `crime_map_ingest=debug`, etc.) |

## Development

```sh
# Frontend dev server with HMR (proxies API to port 8080)
cd app && bun dev

# Debug build of the server
cargo server:debug

# Run all tests
cargo test

# Lint
cargo clippy --all-targets

# Format
cargo fmt
```

## License

[MPL-2.0](https://www.mozilla.org/en-US/MPL/2.0/)
