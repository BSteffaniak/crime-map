# Crime Map Agent Guidelines

## Build/Test Commands

- **Rust build**: `cargo build`
- **Rust test**: `cargo test` (all packages), `cargo test -p <package>` (single package)
- **Rust lint**: `cargo clippy --all-targets`
- **Rust lint enforce no warnings**: `cargo clippy --all-targets -- -D warnings`
- **Format**: `cargo fmt` (Rust) for ALL packages in the workspace
- **Frontend dev**: `cd app && bun dev`
- **Frontend build**: `cd app && bun run build`
- **Database**: `docker compose up -d` (PostGIS on port 5440)
- **Ingest data**: `cargo ingest sync-all`
- **Generate tiles**: `cargo generate all`
- **Run server**: `cargo server`

## Architecture

### Runtime vs. Ingestion

**PostgreSQL (PostGIS) is used ONLY during ingestion and generation.**
The production server runs exclusively on pre-generated SQLite, DuckDB, and
PMTiles files. No runtime API endpoint may depend on a PostgreSQL connection.

| Database | Role | When Used |
|----------|------|-----------|
| PostGIS (port 5440) | Source of truth; spatial attribution, geocoding, boundary ingestion | `cargo ingest`, `cargo generate` |
| SQLite | Sidebar incidents (`incidents.db`), conversations (`conversations.db`), boundary search (`boundaries.db`) | Runtime server |
| DuckDB | Aggregated counts (`counts.duckdb`), hexbin analytics (`h3.duckdb`) | Runtime server |
| PMTiles | Vector tiles for map layers (`incidents.pmtiles`, `boundaries.pmtiles`) | Runtime server (served as static files) |

When adding new server endpoints or features:
- **Never** query PostgreSQL from the server at runtime
- Pre-generate any data the server needs into SQLite, DuckDB, or static files
  during `cargo generate`
- Boundary GEOIDs (`state_fips`, `county_geoid`, `place_geoid`, `tract_geoid`,
  `neighborhood_id`) are derived from PostGIS at generation time and stored in
  DuckDB/SQLite so boundary filtering works without spatial joins at runtime

### Migration Roadmap: Remaining PostgreSQL Dependencies

Three server endpoints still require PostgreSQL when `DATABASE_URL` is set
(they return `503` when it is not). These should be migrated:

| Endpoint | Current State | Migration Approach |
|----------|--------------|-------------------|
| `GET /api/incidents` | Queries PostGIS with `ST_MakeEnvelope` | Port to sidebar SQLite (`incidents.db` already has coordinates + R-tree index) |
| `GET /api/sources` | `SELECT * FROM crime_sources` | Pre-generate source metadata into `metadata.json` or a SQLite table during generation |
| `POST /api/ai/ask` | 7 analytics tools query PostGIS | **Temporary exception** -- port all tools to DuckDB (none use spatial functions, only standard SQL aggregations) |

## Code Style Guidelines

### Rust Patterns

- **Collections**: Always use `BTreeMap`/`BTreeSet`, never `HashMap`/`HashSet`
- **Dependencies**: Use `workspace = true`, never path dependencies or inline versions
- **New Dependencies**: When adding a new dependency:
    - Add to workspace `Cargo.toml` with `default-features = false`
    - Specify full version including patch (e.g., `"0.4.28"` not `"0.4"`)
    - Verify you're using the LATEST stable version from crates.io
    - In package `Cargo.toml`, use `workspace = true` and opt-in to specific features only
- **Clippy**: Required in every crate:
    ```rust
    #![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
    #![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
    #![allow(clippy::multiple_crate_versions)]
    ```
- **Must use**: Add `#[must_use]` to constructors and getters that return types
  OTHER THAN Result or Option. **CRITICAL**: Do NOT add `#[must_use]` to
  functions returning Result or Option types -- these types are already marked
  `#[must_use]` and adding the attribute to the function is redundant and will
  trigger clippy warnings.
- **Serde**: Use `SCREAMING_SNAKE_CASE` for enum rename attributes,
  `camelCase` for struct fields in API responses
- **Discarded results**: Never use `let _ = ...` to discard results. For
  infallible operations like `write!` on a `String`, call `.unwrap()` instead
  (e.g., `write!(buf, "...").unwrap()`). `write!` to a `String` cannot fail,
  so the unwrap is safe and makes intent explicit.

### Package Organization

- **Naming**: All packages use underscore naming with `crime_map_` prefix
  (e.g., `crime_map_database`, `crime_map_ingest`)
- **Features**: Always include `fail-on-warnings = []` feature
- **`_models` Pattern**: When a package defines types that other packages need
  to depend on without pulling in the full package's functionality:
    - Extract shared types into a sibling `models/` subdirectory as a separate
      crate (e.g., `crime_map_source` has `crime_map_source_models`)
    - `_models` crates contain ONLY: structs, enums, type aliases, `From`/`Into`
      impls, serialization derives, and simple utility/parsing functions on those
      types
    - `_models` crates must NOT contain: business logic, database queries, HTTP
      handlers, service orchestration, or heavy dependencies
    - This prevents circular dependencies: two crates that need each other's types
      can both depend on a shared `_models` crate instead of depending on each other
    - `_models` crates should be leaves in the dependency graph (they may depend on
      other `_models` crates but never on their parent implementation crate)
    - Never create generic "shared" or "common" crates -- types belong in the
      domain-specific `_models` crate for the package that owns them

### Database

- **Database abstraction**: Always use `switchy_database` and
  `switchy_database_connection` for ALL database access — both PostgreSQL
  and SQLite. Never use `rusqlite`, `tokio-postgres`, or other database
  drivers directly. Use `init_sqlite_rusqlite` from
  `switchy_database_connection` for SQLite connections and
  `query_raw_params`/`exec_raw_params`/`exec_raw` for all queries.
- Uses `switchy_schema` from MoosicBox for PostGIS migrations
- PostGIS spatial queries use `query_raw_params()` with raw SQL
- Non-spatial queries use the typed query builder where possible
- PostGIS migrations are raw SQL files in `migrations/` using
  switchy_schema's embedded migration runner
- SQLite databases (sidebar, conversations, discovery) create their
  schemas at runtime via `exec_raw("CREATE TABLE IF NOT EXISTS ...")`
- PostGIS container runs on port 5440 to avoid conflicts

### Frontend

- **Package manager**: Bun (never pnpm or npm)
- Vite + React + TypeScript + TailwindCSS
- MapLibre GL JS for map rendering
- PMTiles for vector tile serving (generated offline via tippecanoe)
- Server-side sidebar via pre-generated SQLite with R-tree spatial index
- URL search params as single source of truth for filter state

### Documentation

- Document all public APIs with comprehensive error information
- Include examples for complex functions
