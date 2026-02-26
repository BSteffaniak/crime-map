# Geocoder Index

Tantivy-based local geocoder for the crime map pipeline.
Resolves US street addresses to coordinates using an in-process full-text search
index built from freely-available OpenStreetMap data.

**Key properties:**

- No Docker containers, no external services -- runs entirely in-process
- ~10K lookups/second
- Index is pre-built in CI and stored on R2 (~2-3 GB compressed)
- Zero-config for CI -- partition jobs pull and unpack the index automatically

## Quick Start (OSM-only, zero-config)

This builds an index with the same freely-available OpenStreetMap address data.

1. Go to **GitHub Actions > Build Geocoder Index**
2. Click **Run workflow** with the defaults (no inputs needed)
3. Wait ~2-4 hours (downloads 9 GB OSM PBF from Geofabrik, builds index, uploads to R2)
4. Next data pipeline run automatically picks up the new index

That's it. The Tantivy provider at priority 2 resolves addresses between Census (priority 1)
and Nominatim (priority 3).

## Enhancing with OpenAddresses

OpenAddresses has more comprehensive US address coverage than OSM alone (~100M+ addresses
vs ~30M from OSM). Adding it is optional and requires a free account.

### One-time setup

1. **Create an account** at [batch.openaddresses.io](https://batch.openaddresses.io)

2. **Download the US region zips** (4 files, ~26 GB total):

   | File | Size |
   |------|------|
   | US South | ~11 GB |
   | US West | ~9 GB |
   | US Midwest | ~4.4 GB |
   | US Northeast | ~1.9 GB |

3. **Upload them to R2** under the `oa-data/` prefix:

   ```bash
   # Using the ingest CLI (requires R2 env vars set)
   # There's no push-r2-file command yet, so use the AWS CLI or rclone:
   export AWS_ACCESS_KEY_ID=$R2_ACCESS_KEY_ID
   export AWS_SECRET_ACCESS_KEY=$R2_SECRET_ACCESS_KEY
   export AWS_ENDPOINT_URL=https://$CLOUDFLARE_ACCOUNT_ID.r2.cloudflarestorage.com

   aws s3 cp us_south.zip s3://crime-map-data/oa-data/us_south.zip
   aws s3 cp us_west.zip s3://crime-map-data/oa-data/us_west.zip
   aws s3 cp us_midwest.zip s3://crime-map-data/oa-data/us_midwest.zip
   aws s3 cp us_northeast.zip s3://crime-map-data/oa-data/us_northeast.zip
   ```

4. **Trigger the workflow** with the OA files:

   - Go to **GitHub Actions > Build Geocoder Index**
   - Set `oa_r2_files` to: `us_south.zip,us_west.zip,us_midwest.zip,us_northeast.zip`
   - Click **Run workflow**

The workflow pulls each zip from R2, indexes all four into the same Tantivy index
alongside OSM data, then uploads the combined index back to R2.

### Refreshing OpenAddresses data

OpenAddresses updates their collections periodically (roughly monthly). To refresh:

1. Log in to batch.openaddresses.io and download new zips
2. Re-upload to R2 (same commands as above -- overwrites the old files)
3. Re-trigger the Build Geocoder Index workflow with `oa_r2_files`

## CI Workflows

### Build Geocoder Index (`build-geocoder-index.yml`)

Manually triggered. Builds the Tantivy index and uploads it to R2.

| Input | Default | Description |
|-------|---------|-------------|
| `skip_osm` | `false` | Skip OSM PBF download (OA-only build) |
| `oa_r2_files` | (empty) | Comma-separated OA zip filenames on R2 under `oa-data/` |

**What it does:**

1. Builds the `crime_map_ingest` binary (release mode)
2. Deletes `target/` to reclaim disk space
3. Downloads the US OSM PBF from Geofabrik (~9 GB, free, no auth)
4. (If `oa_r2_files` is set) Pulls each OA zip from R2
5. Runs `geocoder-build` with all archives + OSM
6. Runs `geocoder-verify` smoke tests (fails the build if any test fails)
7. Packs the index to `geocoder_index.tar.zst`
8. Uploads to R2 via `push --shared-only`

**Disk budget:** ~60 GB available after cleanup. Peak usage ~26 GB (OSM PBF + OA zips + index).

### Data Pipeline integration (`data-pipeline.yml`)

Each partition job automatically:

1. `pull --shared-only` -- downloads `geocoder_index.tar.zst` from R2
2. `geocoder-unpack` -- extracts the index to `data/shared/geocoder_index/`
3. `geocode --sources ...` -- Tantivy provider resolves addresses at priority 2

If the index archive doesn't exist on R2 yet, the unpack step is silently
skipped and geocoding falls through to Nominatim.

### Verify Geocoder Index (`verify-geocoder-index.yml`)

Manually triggered. Pulls the index from R2 and runs verification tests.
Optionally searches for user-provided addresses.

| Input | Default | Description |
|-------|---------|-------------|
| `addresses` | (empty) | Semicolon-separated addresses to search (e.g. `100 N State St, Chicago, IL; 350 5th Ave, New York, NY`) |

**What it does:**

1. Builds the `crime_map_ingest` binary
2. Pulls `geocoder_index.tar.zst` from R2
3. Unpacks the index
4. Runs `geocoder-verify` (hardcoded smoke tests -- always runs)
5. (If `addresses` is set) Runs `geocoder-search` for each address and prints results
6. Writes results to the GitHub Actions step summary

## CLI Commands

All commands are subcommands of `cargo ingest` (or the `crime_map_ingest` binary):

### `geocoder-download`

Downloads OSM PBF and OpenAddresses data for building the index locally.

```bash
cargo ingest geocoder-download
```

Downloads to `data/shared/osm/us-latest.osm.pbf` and `data/shared/openaddresses/`.
Primarily useful for local development. CI uses the workflow instead.

### `geocoder-build`

Builds the Tantivy index from available address data.

```bash
# OSM-only (if you ran geocoder-download first)
cargo ingest geocoder-build

# From archive files (CI pattern)
cargo ingest geocoder-build \
  --oa-archive data/shared/oa/us_south.zip \
  --oa-archive data/shared/oa/us_west.zip \
  --heap-mb 512

# OA-only (skip OSM)
cargo ingest geocoder-build --oa-archive us_data.zip --skip-osm
```

| Flag | Default | Description |
|------|---------|-------------|
| `--heap-mb` | 256 | Tantivy writer heap size in MB |
| `--oa-archive` | (none) | Path to an OA archive (`.zip` or `.tar.zst`). Repeatable. |
| `--skip-osm` | false | Skip the OSM PBF (at `data/shared/osm/us-latest.osm.pbf`) |

The index is written to `data/shared/geocoder_index/`.

### `geocoder-pack`

Packs the index directory into `data/shared/geocoder_index.tar.zst`.

```bash
cargo ingest geocoder-pack
```

### `geocoder-unpack`

Extracts `data/shared/geocoder_index.tar.zst` to `data/shared/geocoder_index/`.

```bash
cargo ingest geocoder-unpack
```

### `geocoder-verify`

Runs smoke tests against the local geocoder index. Searches a set of known
US addresses (defined in `packages/geocoder_index/smoke_tests.toml`) and
verifies that returned coordinates are within the configured tolerance.

Exits with a non-zero status if any test fails.

```bash
cargo ingest geocoder-verify
```

Output:

```
Index documents: 31456789

[PASS] 100 N State St, Chicago, IL
       expected: (41.8827, -87.6278)
       actual:   (41.8825, -87.6280)
       matched:  100 NORTH STATE STREET, CHICAGO, IL
       score:    12.34

[FAIL] 350 5th Ave, New York, NY
       no match found

=== 9/10 smoke tests passed ===
```

To update the test addresses, edit `packages/geocoder_index/smoke_tests.toml`:

```toml
default_tolerance = 0.01  # ~1.1 km

[[tests]]
address = "100 N State St, Chicago, IL"
lat = 41.8827
lon = -87.6278

[[tests]]
address = "350 5th Ave, New York, NY"
lat = 40.7484
lon = -73.9856
tolerance = 0.02  # per-test override
```

### `geocoder-search`

Searches the geocoder index for one or more addresses. Useful for ad-hoc
lookups and debugging.

```bash
# Single address
cargo ingest geocoder-search "100 N State St, Chicago, IL"

# Multiple addresses
cargo ingest geocoder-search \
  "100 N State St, Chicago, IL" \
  "1600 Pennsylvania Ave NW, Washington, DC"
```

### `geocoder-compare`

Compares Tantivy hit rates against other providers using addresses already in the geocode cache.
Useful for validating the Tantivy index coverage.

```bash
cargo ingest geocoder-compare
```

### `pull-r2-file`

Downloads a single file from R2 by key. Generic helper used by the CI workflow
to pull OA zips.

```bash
cargo ingest pull-r2-file --key oa-data/us_south.zip --dest data/shared/oa/us_south.zip
```

## Architecture

### Data sources

| Source | Free? | Auth? | Content | Size |
|--------|-------|-------|---------|------|
| [Geofabrik US PBF](https://download.geofabrik.de/north-america/us-latest.osm.pbf) | Yes | No | OSM addresses + venues | ~9 GB |
| [OpenAddresses](https://batch.openaddresses.io) | Yes | Account required | Authoritative address points | ~26 GB (4 US zips) |

OSM data updates daily on Geofabrik. OpenAddresses updates roughly monthly.

### Tantivy schema

Each address document has these fields:

| Field | Type | Description |
|-------|------|-------------|
| `street` | TEXT (tokenized) | Normalized street address (e.g. `100 NORTH STATE STREET`) |
| `city` | TEXT (tokenized) | City name |
| `state` | STRING (exact) | Two-letter state code |
| `postcode` | STRING (exact) | ZIP code |
| `full_address` | TEXT (tokenized) | Combined `street, city, state` for broad matching |
| `lat` | f64 | Latitude |
| `lon` | f64 | Longitude |
| `source` | STRING | `openaddresses` or `osm` |

### Query strategy

Searches use a 4-level `DisjunctionMaxQuery` cascade (takes the best-scoring match):

1. **Exact phrase** on `street` + exact `city` + exact `state` (highest boost)
2. **Term match** on `street` + exact `city` + exact `state`
3. **Phrase match** on `full_address`
4. **Fuzzy term** match on `full_address` (lowest boost, catches typos)

Hits above the exact-match score threshold (8.0) are classified as `Exact`;
lower scores are `Approximate`.

### Address normalization

Addresses are normalized before indexing and searching using ~200 synonym pairs:

- Direction abbreviations: `N` -> `NORTH`, `SW` -> `SOUTHWEST`
- Street type abbreviations: `ST` -> `STREET`, `AVE` -> `AVENUE`, `BLVD` -> `BOULEVARD`
- Case folded to uppercase, punctuation stripped, whitespace collapsed

### Storage

```
data/shared/
  geocoder_index/           # Unpacked Tantivy index (used at runtime)
    meta.json               # Tantivy index metadata
    *.managed.json          # Segment metadata
    *.store, *.idx, ...     # Index data files
  geocoder_index.tar.zst    # Packed archive (for R2 transfer)
```

The archive on R2 is at key `shared/geocoder_index.tar.zst` (same prefix as
`shared/boundaries.duckdb` and `shared/geocode_cache.duckdb`).

### Provider chain integration

The Tantivy geocoder is registered at priority 2 in
`packages/geocoder/services/tantivy_index.toml`. When `geocode` runs:

1. Census Bureau resolves what it can (exact batch matching)
2. Tantivy resolves remaining addresses (fast local full-text search)
3. Nominatim handles whatever is left (slow, rate-limited)

If the Tantivy index is not available (`data/shared/geocoder_index/meta.json`
doesn't exist), the provider is silently skipped.

## File reference

| File | Description |
|------|-------------|
| `packages/geocoder_index/src/lib.rs` | Index builder (`build_index`) and searcher (`GeocoderIndex`) |
| `packages/geocoder_index/src/schema.rs` | Tantivy schema definition and tokenizer registration |
| `packages/geocoder_index/src/query.rs` | 4-level DisjunctionMax query builder |
| `packages/geocoder_index/src/normalize.rs` | Address normalization (case, abbreviations, whitespace) |
| `packages/geocoder_index/src/synonyms.rs` | ~200 street type and directional synonym pairs |
| `packages/geocoder_index/src/openaddresses.rs` | OA CSV parser (directory, `.zip`, `.tar.zst`) |
| `packages/geocoder_index/src/osm.rs` | OSM PBF address extractor |
| `packages/geocoder_index/src/archive.rs` | tar+zstd pack/unpack utilities |
| `packages/geocoder_index/src/download.rs` | HTTP download helper |
| `packages/geocoder_index/src/verify.rs` | Smoke test runner (loads `smoke_tests.toml`) |
| `packages/geocoder_index/smoke_tests.toml` | Smoke test addresses with expected coordinates |
| `packages/geocoder/src/tantivy_index.rs` | Tantivy provider (wraps `GeocoderIndex` for the geocode pipeline) |
| `packages/geocoder/services/tantivy_index.toml` | Provider config (priority 2, enabled) |
| `.github/workflows/build-geocoder-index.yml` | CI workflow to build and upload the index |
| `.github/workflows/verify-geocoder-index.yml` | CI workflow to verify the index (smoke tests + ad-hoc search) |
