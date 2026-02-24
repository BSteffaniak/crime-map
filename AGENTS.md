# Crime Map Agent Guidelines

## Build/Test Commands

- **Rust build**: `cargo build`
- **Rust test**: `cargo test` (all packages), `cargo test -p <package>` (single package)
- **Rust lint**: `cargo clippy --all-targets`
- **Rust lint enforce no warnings**: `cargo clippy --all-targets -- -D warnings`
- **Format**: `cargo fmt` (Rust) for ALL packages in the workspace
- **Frontend dev**: `cd app && bun dev`
- **Frontend build**: `cd app && bun run build`
- **Ingest data**: `cargo ingest sync-all`
- **Generate tiles**: `cargo generate all`
- **Run server**: `cargo server`

## Architecture

### Runtime vs. Ingestion

The system uses **DuckDB files** for ingestion/generation and pre-generated
**SQLite/DuckDB/PMTiles** files at runtime. There is no PostgreSQL or any
external database service.

| Database | Role | When Used |
|----------|------|-----------|
| Per-source DuckDB (`data/sources/{id}.duckdb`) | Incident storage per data source | `cargo ingest` |
| Boundaries DuckDB (`data/shared/boundaries.duckdb`) | Census tract, place, county, state, neighborhood boundaries | `cargo ingest`, `cargo generate` |
| Geocode cache DuckDB (`data/shared/geocode_cache.duckdb`) | Cached geocoding results (shared across sources) | `cargo ingest` |
| SQLite | Sidebar incidents (`incidents.db`), conversations (`conversations.db`), boundary search (`boundaries.db`) | Runtime server, `cargo generate` (output) |
| DuckDB | Aggregated counts (`counts.duckdb`), hexbin analytics (`h3.duckdb`), analytics (`analytics.duckdb`) | Runtime server, `cargo generate` (output) |
| PMTiles | Vector tiles for map layers (`incidents.pmtiles`, `boundaries.pmtiles`) | Runtime server (served as static files) |

### Data Directory Layout

```
data/
├── sources/                    # Per-source DuckDB files (one per crime data source)
│   ├── chicago_pd.duckdb
│   ├── dc_mpd.duckdb
│   └── ...
├── shared/                     # Shared databases
│   ├── boundaries.duckdb       # Census boundary polygons (GeoJSON as TEXT)
│   └── geocode_cache.duckdb    # Address geocoding cache
├── generated/                  # Output artifacts (from cargo generate)
│   ├── incidents.pmtiles
│   ├── incidents.db
│   ├── counts.duckdb
│   ├── h3.duckdb
│   ├── analytics.duckdb
│   ├── boundaries.pmtiles
│   ├── boundaries.db
│   ├── metadata.json
│   └── manifest.json
└── partitions/                 # CI partition outputs (before merge)
```

### R2 Cloud Storage

Per-source DuckDB files and shared databases are persisted on Cloudflare R2
(`crime-map-data` bucket) for CI pipeline use. The local `data/` directory
mirrors the R2 layout. Use `cargo ingest pull` / `cargo ingest push` to
sync between local and R2.

### Spatial Operations

All spatial operations (point-in-polygon, containment tests, boundary
attribution) happen in **Rust** using the `geo`, `rstar`, and `geojson`
crates. Boundary polygons are stored as GeoJSON TEXT strings in DuckDB and
loaded into an in-memory R-tree (`SpatialIndex`) at generation time. There
are no PostGIS or DuckDB Spatial extension dependencies.

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

- **SQLite abstraction (CRITICAL)**: **ALWAYS** use `switchy_database` and
  `switchy_database_connection` for ALL SQLite database access. **NEVER**
  use `rusqlite` directly -- not as a dependency, not as an import, not in
  any form. This applies everywhere: the server, the CLI, generation output
  files, merge operations -- **no exceptions**. Use `init_sqlite_rusqlite`
  from `switchy_database_connection` for SQLite connections and
  `query_raw_params`/`exec_raw_params`/`exec_raw` for all SQLite queries.
  Placeholders use `$1, $2, $3` style (PostgreSQL-compatible). Parameters
  use `DatabaseValue` variants. Do **NOT** add `rusqlite` to any
  `Cargo.toml` as a direct dependency.
- **DuckDB**: Use the `duckdb` crate directly for all DuckDB access. There
  is no `switchy_database` backend for DuckDB. DuckDB connections use `?`
  placeholders and native Rust types for parameters.
- **Ingestion storage**: Per-source DuckDB files in `data/sources/`. One
  file per data source containing an `incidents` table and a `_meta` table.
  Opened via `crime_map_database::source_db::open_by_id()`.
- **Boundary storage**: Shared DuckDB file at `data/shared/boundaries.duckdb`.
  Contains census tracts, places, counties, states, neighborhoods, and the
  tract-neighborhoods crosswalk. Boundary geometry stored as GeoJSON TEXT
  (no PostGIS geometry types). Opened via
  `crime_map_database::boundaries_db::open_default()`.
- **Geocode cache**: Shared DuckDB file at `data/shared/geocode_cache.duckdb`.
  Opened via `crime_map_database::geocode_cache::open_default()`.
- **Output SQLite databases** (sidebar `incidents.db`, `boundaries.db`):
  Created during `cargo generate` using `switchy_database_connection` (not
  `rusqlite` directly).
- **Output DuckDB databases** (`counts.duckdb`, `h3.duckdb`,
  `analytics.duckdb`): Created during `cargo generate` using the `duckdb`
  crate directly.

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
