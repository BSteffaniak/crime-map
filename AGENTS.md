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

- Uses `switchy_database` and `switchy_schema` from MoosicBox for database
  abstraction and migrations
- PostGIS spatial queries use `query_raw_params()` with raw SQL
- Non-spatial queries use the typed query builder where possible
- Migrations are raw SQL files in `migrations/` using switchy_schema's embedded
  migration runner
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
