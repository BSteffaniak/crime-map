#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Tantivy-based geocoder index for US address lookup.
//!
//! In-process Tantivy full-text search index built from `OpenAddresses`
//! CSV data and OpenStreetMap PBF extracts.
//!
//! # Architecture
//!
//! - **Index time**: Address data is normalized (abbreviations expanded,
//!   punctuation stripped, uppercased) and indexed into a Tantivy
//!   `MmapDirectory`.
//! - **Query time**: Input addresses are normalized identically, then
//!   searched via a cascading `DisjunctionMaxQuery` that tries exact
//!   phrase matching, fuzzy matching, and full-address fallback.
//! - **Sync**: The index directory is packed into a `.tar.zst` archive
//!   for upload/download to Cloudflare R2.
//!
//! # Usage
//!
//! ```rust,no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use crime_map_geocoder_index::GeocoderIndex;
//!
//! let index = GeocoderIndex::open("data/shared/geocoder_index")?;
//! let result = index.search("100 N STATE ST", "Chicago", "IL").await?;
//! if let Some(hit) = result {
//!     println!("({}, {})", hit.latitude, hit.longitude);
//! }
//! # Ok(())
//! # }
//! ```

pub mod archive;
pub mod download;
pub mod normalize;
pub mod openaddresses;
pub mod osm;
pub mod query;
pub mod schema;
pub mod synonyms;
pub mod verify;

use std::path::{Path, PathBuf};
use std::time::Instant;

use tantivy::collector::TopDocs;
use tantivy::schema::Value;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, doc};

pub use crime_map_geocoder_index_models::{
    AddressSource, GeocoderIndexConfig, IndexStats, SearchResult,
};
use schema::GeocoderFields;

/// Default path for the geocoder index relative to `data/shared/`.
pub const DEFAULT_INDEX_DIR_NAME: &str = "geocoder_index";

/// Default path for the geocoder index archive.
pub const DEFAULT_ARCHIVE_NAME: &str = "geocoder_index.tar.zst";

/// Returns the default path for the geocoder index directory.
#[must_use]
pub fn default_index_dir() -> PathBuf {
    crime_map_database::paths::shared_dir().join(DEFAULT_INDEX_DIR_NAME)
}

/// Returns the default path for the geocoder index archive.
#[must_use]
pub fn default_archive_path() -> PathBuf {
    crime_map_database::paths::shared_dir().join(DEFAULT_ARCHIVE_NAME)
}

/// Returns the default path for `OpenAddresses` data.
#[must_use]
pub fn default_openaddresses_dir() -> PathBuf {
    crime_map_database::paths::shared_dir().join("openaddresses")
}

/// Returns the default path for OSM PBF data.
#[must_use]
pub fn default_osm_pbf_path() -> PathBuf {
    crime_map_database::paths::shared_dir()
        .join("osm")
        .join("us-latest.osm.pbf")
}

/// Errors from geocoder index operations.
#[derive(Debug, thiserror::Error)]
pub enum GeocoderIndexError {
    /// Tantivy error.
    #[error("Tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),

    /// `OpenAddresses` parsing error.
    #[error("OpenAddresses error: {0}")]
    OpenAddresses(#[from] openaddresses::OaError),

    /// OSM PBF parsing error.
    #[error("OSM error: {0}")]
    Osm(#[from] osm::OsmError),

    /// Download error.
    #[error("Download error: {0}")]
    Download(#[from] download::DownloadError),

    /// Archive error.
    #[error("Archive error: {0}")]
    Archive(#[from] archive::ArchiveError),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Index directory not found.
    #[error("Index directory not found: {0}")]
    IndexNotFound(String),

    /// Async task join error.
    #[error("Task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    /// Generic error.
    #[error("{0}")]
    Other(String),
}

/// A handle to an opened geocoder index for searching.
///
/// The index is backed by memory-mapped files and supports concurrent
/// searches from multiple threads/tasks.
pub struct GeocoderIndex {
    schema: tantivy::schema::Schema,
    fields: GeocoderFields,
    reader: IndexReader,
}

impl GeocoderIndex {
    /// Opens an existing geocoder index from a directory.
    ///
    /// The index must have been previously built with [`build_index`].
    ///
    /// # Errors
    ///
    /// Returns an error if the directory does not exist or does not
    /// contain a valid Tantivy index.
    pub fn open(index_dir: impl AsRef<Path>) -> Result<Self, GeocoderIndexError> {
        let index_dir = index_dir.as_ref();
        if !index_dir.exists() {
            return Err(GeocoderIndexError::IndexNotFound(
                index_dir.display().to_string(),
            ));
        }

        log::info!("Opening geocoder index at {}", index_dir.display());

        let index = Index::open_in_dir(index_dir)?;
        schema::register_tokenizers(&index);

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        let schema = index.schema();
        let fields = GeocoderFields::from_schema(&schema);

        Ok(Self {
            schema,
            fields,
            reader,
        })
    }

    /// Returns `true` if the default index directory exists and
    /// contains an index.
    #[must_use]
    pub fn is_available() -> bool {
        let dir = default_index_dir();
        dir.exists() && dir.join("meta.json").exists()
    }

    /// Searches the index for a matching address.
    ///
    /// Returns the best matching result, or `None` if no match was
    /// found above the minimum score threshold.
    ///
    /// This is an async wrapper around sync Tantivy search, dispatched
    /// to a blocking thread pool.
    ///
    /// # Errors
    ///
    /// Returns an error if the search fails (e.g., index corruption).
    pub async fn search(
        &self,
        street: &str,
        city: &str,
        state: &str,
    ) -> Result<Option<SearchResult>, GeocoderIndexError> {
        let reader = self.reader.clone();
        let schema = self.schema.clone();
        let fields = self.fields.clone();
        let street = street.to_string();
        let city = city.to_string();
        let state = state.to_string();

        tokio::task::spawn_blocking(move || {
            search_sync(&reader, &schema, &fields, &street, &city, &state)
        })
        .await?
    }

    /// Synchronous search (for use in non-async contexts).
    ///
    /// # Errors
    ///
    /// Returns an error if the search fails.
    pub fn search_sync(
        &self,
        street: &str,
        city: &str,
        state: &str,
    ) -> Result<Option<SearchResult>, GeocoderIndexError> {
        search_sync(
            &self.reader,
            &self.schema,
            &self.fields,
            street,
            city,
            state,
        )
    }

    /// Returns the total number of documents in the index.
    #[must_use]
    pub fn num_docs(&self) -> u64 {
        let searcher = self.reader.searcher();
        searcher.num_docs()
    }
}

/// Internal sync search implementation shared by both async and sync
/// public APIs.
fn search_sync(
    reader: &IndexReader,
    schema: &tantivy::schema::Schema,
    fields: &GeocoderFields,
    street: &str,
    city: &str,
    state: &str,
) -> Result<Option<SearchResult>, GeocoderIndexError> {
    let searcher = reader.searcher();
    let query = query::build_geocode_query(schema, fields, street, city, state);

    let top_docs = searcher.search(&query, &TopDocs::with_limit(1))?;

    let Some((score, doc_address)) = top_docs.first() else {
        return Ok(None);
    };

    let doc: TantivyDocument = searcher.doc(*doc_address)?;

    let lat = doc
        .get_first(fields.lat)
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let lon = doc
        .get_first(fields.lon)
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let matched_street = doc
        .get_first(fields.street)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let matched_city = doc
        .get_first(fields.city)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let matched_state = doc
        .get_first(fields.state)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let source_tag = doc
        .get_first(fields.source)
        .and_then(|v| v.as_str())
        .unwrap_or("oa");
    let source = AddressSource::from_str_tag(source_tag).unwrap_or(AddressSource::OpenAddresses);

    Ok(Some(SearchResult {
        latitude: lat,
        longitude: lon,
        matched_street,
        matched_city,
        matched_state,
        source,
        score: *score,
    }))
}

/// Configuration for building a geocoder index.
///
/// Specifies the data sources to include. At least one of `oa_dir`,
/// `oa_archives`, or `osm_pbf` must be provided.
pub struct BuildConfig<'a> {
    /// Directory containing extracted `OpenAddresses` CSV files.
    pub oa_dir: Option<&'a Path>,
    /// Paths to `OpenAddresses` archive files (`.zip` or `.tar.zst`).
    /// Each archive is streamed in-memory without extraction to disk.
    /// Multiple archives are indexed sequentially into the same index.
    pub oa_archives: &'a [PathBuf],
    /// Path to a US OSM PBF extract.
    pub osm_pbf: Option<&'a Path>,
    /// Tantivy writer heap size in bytes.
    pub writer_heap_bytes: usize,
}

/// Builds a geocoder index from `OpenAddresses` and OSM data.
///
/// This is the main entry point for index construction. It:
/// 1. Creates or overwrites the index directory
/// 2. Parses `OpenAddresses` CSV files (from directory or `.tar.zst` archive)
/// 3. Parses the OSM PBF file for address nodes
/// 4. Commits and optimizes the index
///
/// # Errors
///
/// Returns an error if data parsing or index writing fails.
pub async fn build_index(
    index_dir: &Path,
    config: BuildConfig<'_>,
) -> Result<IndexStats, GeocoderIndexError> {
    let index_dir = index_dir.to_path_buf();
    let oa_dir = config.oa_dir.map(Path::to_path_buf);
    let oa_archives = config.oa_archives.to_vec();
    let osm_pbf = config.osm_pbf.map(Path::to_path_buf);
    let writer_heap_bytes = config.writer_heap_bytes;

    tokio::task::spawn_blocking(move || {
        build_index_sync(
            &index_dir,
            oa_dir.as_deref(),
            &oa_archives,
            osm_pbf.as_deref(),
            writer_heap_bytes,
        )
    })
    .await?
}

/// Synchronous index build implementation.
fn build_index_sync(
    index_dir: &Path,
    oa_dir: Option<&Path>,
    oa_archives: &[PathBuf],
    osm_pbf: Option<&Path>,
    writer_heap_bytes: usize,
) -> Result<IndexStats, GeocoderIndexError> {
    let start = Instant::now();

    // Create or recreate the index directory
    if index_dir.exists() {
        log::info!("Removing existing index at {}", index_dir.display());
        std::fs::remove_dir_all(index_dir)?;
    }
    std::fs::create_dir_all(index_dir)?;

    let schema = schema::build_schema();
    let index = Index::create_in_dir(index_dir, schema.clone())?;
    schema::register_tokenizers(&index);

    let mut writer: IndexWriter = index.writer(writer_heap_bytes)?;
    let fields = GeocoderFields::from_schema(&schema);

    let mut oa_count = 0u64;
    let mut osm_count = 0u64;
    let mut total_count = 0u64;

    // Phase 1: Index OpenAddresses data
    if let Some(oa_dir) = oa_dir {
        if oa_dir.exists() {
            log::info!("Indexing OpenAddresses data from {}", oa_dir.display());
            let count = openaddresses::parse_directory(oa_dir, |addr| {
                add_document(&writer, &fields, &addr, AddressSource::OpenAddresses);
                total_count += 1;
                if total_count.is_multiple_of(1_000_000) {
                    log::info!("  indexed {total_count} records...");
                }
            })?;
            oa_count = count;
            log::info!("  OpenAddresses (dir): {oa_count} records indexed");
        } else {
            log::warn!("OpenAddresses directory not found: {}", oa_dir.display());
        }
    }

    // Phase 1b: Index OpenAddresses data from archives (.tar.zst or .zip)
    for (i, archive) in oa_archives.iter().enumerate() {
        if !archive.exists() {
            log::warn!("OpenAddresses archive not found: {}", archive.display());
            continue;
        }

        log::info!(
            "Indexing OpenAddresses archive [{}/{}]: {}",
            i + 1,
            oa_archives.len(),
            archive.display()
        );

        let archive_name = archive.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let is_zip = archive_name.to_ascii_lowercase().ends_with(".zip");

        let count = if is_zip {
            openaddresses::parse_zip_archive(archive, |addr| {
                add_document(&writer, &fields, &addr, AddressSource::OpenAddresses);
                total_count += 1;
                if total_count.is_multiple_of(1_000_000) {
                    log::info!("  indexed {total_count} records...");
                }
            })?
        } else {
            openaddresses::parse_tar_zst_archive(archive, |addr| {
                add_document(&writer, &fields, &addr, AddressSource::OpenAddresses);
                total_count += 1;
                if total_count.is_multiple_of(1_000_000) {
                    log::info!("  indexed {total_count} records...");
                }
            })?
        };

        oa_count += count;
        log::info!("  OpenAddresses (archive): {count} records indexed");
    }

    // Phase 2: Index OSM data
    if let Some(osm_pbf) = osm_pbf {
        if osm_pbf.exists() {
            log::info!("Indexing OSM data from {}", osm_pbf.display());
            let addresses = osm::parse_pbf(osm_pbf)?;
            osm_count = addresses.len() as u64;
            for addr in &addresses {
                add_document(&writer, &fields, addr, AddressSource::Osm);
                total_count += 1;
                if total_count.is_multiple_of(1_000_000) {
                    log::info!("  indexed {total_count} records...");
                }
            }
            log::info!("  OSM: {osm_count} records indexed");
        } else {
            log::warn!("OSM PBF file not found: {}", osm_pbf.display());
        }
    }

    // Commit
    log::info!("Committing index ({total_count} total documents)...");
    writer.commit()?;

    // Optimize: merge down to a small number of segments for faster search
    log::info!("Optimizing index (merging segments)...");
    writer.wait_merging_threads()?;

    let elapsed = start.elapsed();

    // Calculate index size
    let index_size_bytes = dir_size(index_dir).unwrap_or(0);

    #[allow(clippy::cast_precision_loss)]
    let mb = index_size_bytes as f64 / 1_048_576.0;
    log::info!(
        "Index built: {total_count} documents, {mb:.1} MB, {:.1}s",
        elapsed.as_secs_f64()
    );

    Ok(IndexStats {
        total_documents: total_count,
        openaddresses_count: oa_count,
        osm_count,
        index_size_bytes,
        build_time_secs: elapsed.as_secs_f64(),
    })
}

/// Adds a single address document to the index writer.
fn add_document(
    writer: &IndexWriter,
    fields: &GeocoderFields,
    addr: &openaddresses::NormalizedAddress,
    source: AddressSource,
) {
    let result = writer.add_document(doc!(
        fields.street => addr.street.as_str(),
        fields.city => addr.city.as_str(),
        fields.state => addr.state.as_str(),
        fields.postcode => addr.postcode.as_str(),
        fields.lat => addr.lat,
        fields.lon => addr.lon,
        fields.source => source.as_str(),
        fields.full_address => addr.full_address.as_str(),
    ));

    if let Err(e) = result {
        log::trace!("Failed to add document: {e}");
    }
}

/// Recursively calculates the total size of a directory.
fn dir_size(path: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let ft = entry.file_type()?;
            if ft.is_file() {
                total += entry.metadata()?.len();
            } else if ft.is_dir() {
                total += dir_size(&entry.path())?;
            }
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn build_and_search_empty_index() {
        let tmp = std::env::temp_dir().join("geocoder_index_test_empty");
        let _ = std::fs::remove_dir_all(&tmp);

        let stats = build_index(
            &tmp,
            BuildConfig {
                oa_dir: None,
                oa_archives: &[],
                osm_pbf: None,
                writer_heap_bytes: 50_000_000,
            },
        )
        .await
        .unwrap();

        assert_eq!(stats.total_documents, 0);
        assert_eq!(stats.openaddresses_count, 0);
        assert_eq!(stats.osm_count, 0);

        let index = GeocoderIndex::open(&tmp).unwrap();
        let result = index.search("100 MAIN ST", "Chicago", "IL").await.unwrap();
        assert!(result.is_none());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn build_and_search_with_csv_data() {
        let tmp = std::env::temp_dir().join("geocoder_index_test_csv");
        let _ = std::fs::remove_dir_all(&tmp);

        // Create a test CSV file
        let oa_dir = tmp.join("oa");
        std::fs::create_dir_all(&oa_dir).unwrap();
        std::fs::write(
            oa_dir.join("test.csv"),
            "LON,LAT,NUMBER,STREET,UNIT,CITY,DISTRICT,REGION,POSTCODE,ID,HASH\n\
             -87.6278,41.8827,100,N STATE ST,,CHICAGO,,IL,60602,,\n\
             -77.0364,38.8951,1600,PENNSYLVANIA AVE NW,,WASHINGTON,,DC,20500,,\n",
        )
        .unwrap();

        let index_dir = tmp.join("index");
        let stats = build_index(
            &index_dir,
            BuildConfig {
                oa_dir: Some(&oa_dir),
                oa_archives: &[],
                osm_pbf: None,
                writer_heap_bytes: 50_000_000,
            },
        )
        .await
        .unwrap();

        assert_eq!(stats.total_documents, 2);
        assert_eq!(stats.openaddresses_count, 2);

        let index = GeocoderIndex::open(&index_dir).unwrap();

        // Search for Chicago address
        let result = index
            .search("100 N STATE ST", "CHICAGO", "IL")
            .await
            .unwrap();
        let hit = result.expect("should find Chicago address");
        assert!((hit.latitude - 41.8827).abs() < 0.01);
        assert!((hit.longitude - (-87.6278)).abs() < 0.01);

        // Search for DC address
        let result = index
            .search("1600 PENNSYLVANIA AVE NW", "WASHINGTON", "DC")
            .await
            .unwrap();
        let hit = result.expect("should find DC address");
        assert!((hit.latitude - 38.8951).abs() < 0.01);

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
