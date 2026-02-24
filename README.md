# Crime Map

Interactive map visualizing public crime data from US cities. Rust backend with PostGIS, React + MapLibre GL JS frontend.

## Architecture

```
Browser (MapLibre GL JS)
  |
  |-- Zoom 0-7:   Heatmap layer (PMTiles vector tiles)
  |-- Zoom 8-11:  Hexbin layer (H3 hexbin analytics via DuckDB)
  |-- Zoom 12+:   Individual points (PMTiles vector tiles)
  |
Actix-Web API server (port 8080)
  |-- /api/*         REST endpoints (incidents, categories, sources, sidebar, AI chat)
  |-- /tiles/*       Static PMTiles files
  |
PostGIS (port 5440)           -- Spatial indexes, crime category taxonomy, census tracts
SQLite (incidents.db)         -- R-tree indexed sidebar queries
DuckDB (counts.duckdb)        -- Pre-aggregated counts for sub-10ms filtering
```

## Prerequisites

**Recommended:** Use [Nix](https://nixos.org/) with the included `flake.nix` -- it provides all dependencies automatically via `direnv` or `nix develop`.

**Manual setup** requires:

- Rust (stable, 2024 edition)
- [Bun](https://bun.sh/) (frontend package manager and bundler)
- Docker or Podman (for PostGIS)
- [tippecanoe](https://github.com/felt/tippecanoe) (PMTiles generation)
- [DuckDB](https://duckdb.org/) shared library (for the `duckdb` Rust crate; or enable the `duckdb-bundled` Cargo feature to compile from source)
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

Creates tables (`crime_sources`, `crime_categories`, `crime_incidents`, `county_stats`, `census_tracts`) and seeds the category taxonomy.

### 3. Ingest crime data

```sh
# Test with a small sample from each source
cargo ingest sync-all --limit 100

# Full sync (all records from all sources)
cargo ingest sync-all

# Sync only specific sources
cargo ingest sync-all --sources chicago_pd,dc_mpd,boston_pd

# Sync a single source
cargo ingest sync chicago_pd --limit 1000
```

You can also set the `CRIME_MAP_SOURCES` environment variable in your `.env` file to persistently select which sources to sync:

```sh
# Only sync these sources by default
CRIME_MAP_SOURCES=chicago_pd,la_pd,sf_pd,dc_mpd
```

The `--sources` CLI flag takes precedence over the env var. If neither is set, all sources are synced.

### 4. Generate map tiles

```sh
cargo generate all
```

Exports incidents from PostGIS as GeoJSONSeq, then generates:

- `data/generated/incidents.pmtiles` -- vector tiles for heatmap and point layers
- `data/generated/incidents.db` -- SQLite with R-tree spatial index for sidebar queries
- `data/generated/counts.duckdb` -- pre-aggregated counts for fast filtering

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

The project ingests crime data from **42 municipal open data sources** across 5 API types. Run `cargo ingest sources` to see the full list.

| API Type       | Sources | Cities                                                                                                                   |
| -------------- | ------- | ------------------------------------------------------------------------------------------------------------------------ |
| Socrata        | 21      | Chicago, Los Angeles, San Francisco, Seattle, New York, Denver, Dallas, Oakland, Kansas City, Cambridge, Mesa, Gainesville, Everett, Baton Rouge, Cincinnati, Montgomery County MD, Prince George's County MD |
| ArcGIS REST    | 16      | Washington DC, Baltimore, Atlanta, Detroit, Charlotte, Minneapolis, Tampa, Las Vegas, Raleigh, Fairfax VA, Prince William VA, Chesterfield VA, Lynchburg VA                                                 |
| CKAN Datastore | 3       | Boston, Pittsburgh                                                                                                       |
| Carto SQL      | 1       | Philadelphia                                                                                                             |
| OData          | 1       | Arlington VA                                                                                                             |

> **Note on data licensing:** Most sources above are municipal open data with permissive reuse terms. Philadelphia (`philly_pd`) has more restrictive terms that prohibit commercial use and redistribution without written permission from the City. DC (`dc_mpd`) is the most permissive (CC0 public domain). Chicago (`chicago_pd`) requires a disclaimer citing cityofchicago.org as the original source. Use `--sources` or `CRIME_MAP_SOURCES` to control which sources you ingest.

## AI Chat

The frontend includes a natural language chat interface for querying crime data. It streams responses via SSE (`/api/ai/ask`) using an LLM agent that has access to 6 analytical tools:

| Tool               | Description                                                                  |
| ------------------ | ---------------------------------------------------------------------------- |
| `count_incidents`  | Count incidents in an area with optional category, severity, and date filters |
| `rank_areas`       | Rank census tracts by crime count (total or per-capita)                      |
| `compare_periods`  | Compare crime between two time periods (year-over-year, before/after)        |
| `get_trend`        | Time-series trends at daily, weekly, monthly, or yearly granularity          |
| `top_crime_types`  | Most common crime categories and subcategories in an area                    |
| `list_cities`      | List all cities available in the dataset                                     |

### Provider setup

Set one of the following API keys in your `.env` file or environment to enable AI chat:

```sh
# Anthropic Claude (default: claude-sonnet-4-20250514)
ANTHROPIC_API_KEY=sk-ant-...

# OpenAI GPT (default: gpt-4o)
OPENAI_API_KEY=sk-...

# AWS Bedrock (default: us.anthropic.claude-sonnet-4-20250514-v1:0)
# Uses standard AWS credential chain (AWS_ACCESS_KEY_ID, AWS_PROFILE, etc.)
# Or set AWS_BEARER_TOKEN_BEDROCK for temporary console tokens
```

The provider is auto-detected from whichever key is set. To override, set `AI_PROVIDER` (`anthropic`, `openai`, or `bedrock`) and optionally `AI_MODEL`.

## CLI Reference

Cargo aliases are defined in `.cargo/config.toml`. All commands run in release mode by default; append `:debug` for debug builds (e.g., `cargo ingest:debug`).

### `cargo ingest`

```
cargo ingest migrate              Run database migrations
cargo ingest sources              List all configured data sources
cargo ingest sync <SOURCE_ID>     Sync a single source
  --limit <N>                     Max records to fetch (for testing)
  --force                         Full sync, ignoring previously synced data
cargo ingest sync-all             Sync all sources
  --limit <N>                     Max records per source (for testing)
  --sources <IDS>                 Comma-separated source IDs to sync (overrides CRIME_MAP_SOURCES)
  --force                         Full sync for all sources, ignoring previously synced data
```

### `cargo generate`

```
cargo generate all                Generate all output files
cargo generate pmtiles            Generate PMTiles (heatmap + point layers)
cargo generate sidebar            Generate sidebar SQLite database (R-tree spatial index)
cargo generate count-db           Generate DuckDB count database (pre-aggregated summary)
cargo generate h3-db              Generate DuckDB H3 hexbin database
cargo generate boundaries         Generate boundary PMTiles + SQLite search database
cargo generate merge              Merge partitioned artifacts into unified outputs
  --limit <N>                     Max records to export (for testing)
  --sources <IDS>                 Comma-separated source IDs to include
  --force                         Regenerate even if source data hasn't changed
  --keep-intermediate             Keep intermediate .geojsonseq file after generation
```

### `cargo server`

Starts the Actix-Web server. API endpoints:

```
GET /api/health                   Health check
GET /api/incidents                Query incidents (supports bbox, date range, severity filters)
GET /api/categories               List crime categories
GET /api/sources                  List data sources and sync status
GET /api/sidebar                  Paginated sidebar incidents (bbox, filters, counts via DuckDB)
GET /api/ai/ask?q=...             AI chat (SSE streaming, natural language crime queries)
```

## Geocoding

Addresses without coordinates are geocoded through a priority-based provider chain defined in TOML config files (`packages/geocoder/services/*.toml`). Each provider is tried in order; addresses resolved by an earlier provider are skipped by later ones.

| Priority | Provider | Strategy | Speed |
| -------- | -------- | -------- | ----- |
| 1 | **Census Bureau** | Batch API (up to 10K addresses/request), exact match | ~10K/batch |
| 2 | **Pelias** (self-hosted) | Concurrent fuzzy search against OpenAddresses + OSM | ~100 req/s |
| 3 | **Nominatim** | Public OSM API, strict rate limit | ~0.9 req/s |

Pelias is optional -- if unreachable, the pipeline skips it and falls through to Nominatim. See [`infra/pelias/README.md`](infra/pelias/README.md) for running Pelias locally with a Cloudflare Tunnel for CI access.

### Adding a new geocoding provider

1. Create `packages/geocoder/services/<provider>.toml` with `id`, `name`, `enabled`, `priority`, and a `[provider]` section
2. Add a new variant to `ProviderConfig` in `packages/geocoder/src/service_registry.rs`
3. Implement the client in `packages/geocoder/src/<provider>.rs`
4. Add the provider match arm in `resolve_addresses()` in `packages/ingest/src/lib.rs`

## Project Structure

```
packages/
  crime/models/       Crime taxonomy types (CrimeCategory, CrimeSubcategory, CrimeSeverity)
  source/models/      Source types (NormalizedIncident, SourceConfig, SourceType)
  source/             CrimeSource trait, shared fetchers (Socrata, ArcGIS, Carto, CKAN, OData), 42 TOML source configs
  database/models/    Query types (BoundingBox, IncidentQuery, IncidentRow)
  database/           PostGIS migrations and spatial queries
  geocoder/           Geocoding clients (Census Bureau, Pelias, Nominatim) and TOML service registry
  geography/models/   Census tract and area statistics types
  geography/          Census tract boundary ingestion (TIGERweb API), spatial queries
  analytics/models/   AI tool parameter/result types, JSON Schema tool definitions
  analytics/          Analytical query engine (count, rank, compare, trend, top types, list cities)
  ai/                 LLM agent loop with provider abstraction (Anthropic, OpenAI, Bedrock)
  ingest/models/      Ingestion types (FetchConfig, ImportResult)
  ingest/             CLI binary for data ingestion (migrate, sync, sync-all, geocode)
  generate/           Tile and database generation via tippecanoe (PMTiles, SQLite, DuckDB)
  server/models/      API request/response types
  server/             Actix-Web HTTP server
  scraper/            HTML table scraping and CSV download utilities
app/                  Vite + React + TypeScript + TailwindCSS + MapLibre GL JS frontend
infra/                OpenTofu configs for self-hosted Pelias on Oracle Cloud Always Free
```

## Environment Variables

| Variable                 | Default                                                 | Description                                                       |
| ------------------------ | ------------------------------------------------------- | ----------------------------------------------------------------- |
| `DATABASE_URL`           | `postgres://postgres:postgres@localhost:5440/crime_map` | PostgreSQL connection string                                      |
| `CRIME_MAP_SOURCES`      | (all sources)                                           | Comma-separated source IDs to sync (e.g., `chicago_pd,dc_mpd`)   |
| `BIND_ADDR`              | `127.0.0.1`                                             | Server bind address                                               |
| `PORT`                   | `8080`                                                  | Server port                                                       |
| `RUST_LOG`               | (none)                                                  | Log level (`info`, `debug`, `crime_map_ingest=debug`, etc.)       |
| `AI_PROVIDER`            | (auto-detect)                                           | AI provider: `anthropic`, `openai`, or `bedrock`                  |
| `AI_MODEL`               | (per-provider default)                                  | Override the default model for the selected AI provider            |
| `ANTHROPIC_API_KEY`      | (none)                                                  | Anthropic API key (enables AI chat with Claude)                    |
| `OPENAI_API_KEY`         | (none)                                                  | OpenAI API key (enables AI chat with GPT)                         |
| `AWS_BEARER_TOKEN_BEDROCK` | (none)                                                | Bedrock temporary bearer token (highest auto-detection priority)   |
| `AWS_REGION`             | (none)                                                  | AWS region for Bedrock                                             |

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
