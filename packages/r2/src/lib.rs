#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Cloudflare R2 sync for `DuckDB` data files.
//!
//! Provides pull (download) and push (upload) operations between the local
//! `data/` directory and the `crime-map-data` R2 bucket.
//!
//! ## Smart sync
//!
//! Downloads and uploads use **size + MD5/ETag comparison** to skip transfers
//! when the local and remote files are identical. This dramatically reduces
//! bandwidth on incremental pipeline runs where most files haven't changed.
//! Pass `force = true` to bypass the check.
//!
//! # Environment Variables
//!
//! | Variable | Required | Description |
//! |---|---|---|
//! | `CLOUDFLARE_ACCOUNT_ID` | Yes | Cloudflare account ID (builds the R2 endpoint) |
//! | `R2_ACCESS_KEY_ID` | Yes | S3-compatible access key for R2 |
//! | `R2_SECRET_ACCESS_KEY` | Yes | S3-compatible secret key for R2 |
//!
//! Alternatively, set `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
//! `AWS_ENDPOINT_URL` directly (the AWS SDK reads these automatically).

use std::path::Path;

use aws_config::Region;
use aws_sdk_s3::config::{Credentials, StalledStreamProtectionConfig};
use crime_map_database::paths;
use crime_map_source::registry;

/// R2 bucket name for pipeline data.
const BUCKET: &str = "crime-map-data";

/// Files produced per partition/merged output.
const GENERATED_FILES: &[&str] = &[
    "incidents.pmtiles",
    "incidents.db",
    "counts.duckdb",
    "h3.duckdb",
    "analytics.duckdb",
    "metadata.json",
    "manifest.json",
];

/// Files produced by boundary generation.
const BOUNDARY_FILES: &[&str] = &["boundaries.pmtiles", "boundaries.db"];

/// Errors that can occur during R2 operations.
#[derive(Debug, thiserror::Error)]
pub enum R2Error {
    /// Missing required environment variable.
    #[error("Missing environment variable: {name}")]
    MissingEnv {
        /// Name of the missing environment variable.
        name: String,
    },

    /// S3 `GetObject` failed.
    #[error("Failed to download s3://{bucket}/{key}: {source}")]
    Download {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Underlying SDK error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// S3 `PutObject` failed.
    #[error("Failed to upload s3://{bucket}/{key}: {source}")]
    Upload {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Underlying SDK error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// S3 `HeadObject` failed.
    #[error("Failed to head s3://{bucket}/{key}: {source}")]
    Head {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Underlying SDK error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// S3 `DeleteObject` failed.
    #[error("Failed to delete s3://{bucket}/{key}: {source}")]
    Delete {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Underlying SDK error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// S3 `ListObjectsV2` failed.
    #[error("Failed to list s3://{bucket}/{prefix}: {source}")]
    List {
        /// Bucket name.
        bucket: String,
        /// Key prefix.
        prefix: String,
        /// Underlying SDK error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// I/O error reading or writing local files.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Maximum number of download attempts (initial + retries).
const MAX_DOWNLOAD_ATTEMPTS: u32 = 3;

/// Base delay between download retries (doubles each attempt).
const RETRY_BASE_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

/// Result of a sync batch: how many files were transferred vs skipped.
#[derive(Debug, Default, Clone, Copy)]
pub struct SyncStats {
    /// Number of files actually transferred (uploaded or downloaded).
    pub transferred: u64,
    /// Number of files skipped because they matched the remote.
    pub skipped: u64,
    /// Number of files not found (missing locally for upload, or missing
    /// remotely for download).
    pub not_found: u64,
}

impl SyncStats {
    /// Merge another stats into this one.
    pub const fn merge(&mut self, other: Self) {
        self.transferred += other.transferred;
        self.skipped += other.skipped;
        self.not_found += other.not_found;
    }

    /// Total number of files considered.
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.transferred + self.skipped + self.not_found
    }
}

impl std::fmt::Display for SyncStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} transferred, {} skipped (unchanged), {} not found",
            self.transferred, self.skipped, self.not_found
        )
    }
}

/// Remote object metadata from `HeadObject`.
struct RemoteMeta {
    /// Content length in bytes.
    size: u64,
    /// `ETag` (usually the MD5 hex digest surrounded by quotes for
    /// non-multipart uploads).
    etag: Option<String>,
}

/// Client for syncing `DuckDB` files with Cloudflare R2.
pub struct R2Client {
    client: aws_sdk_s3::Client,
}

impl R2Client {
    /// Creates a new R2 client from environment variables.
    ///
    /// Reads `CLOUDFLARE_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, and
    /// `R2_SECRET_ACCESS_KEY` from the environment.
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::MissingEnv`] if any required variable is unset.
    pub fn from_env() -> Result<Self, R2Error> {
        let account_id = require_env("CLOUDFLARE_ACCOUNT_ID")?;
        let access_key = require_env("R2_ACCESS_KEY_ID")?;
        let secret_key = require_env("R2_SECRET_ACCESS_KEY")?;

        let endpoint = format!("https://{account_id}.r2.cloudflarestorage.com");
        let creds = Credentials::new(&access_key, &secret_key, None, None, "r2-env");

        let config = aws_sdk_s3::Config::builder()
            .endpoint_url(&endpoint)
            .region(Region::new("auto"))
            .credentials_provider(creds)
            .force_path_style(true)
            .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
            .build();

        Ok(Self {
            client: aws_sdk_s3::Client::from_conf(config),
        })
    }

    // ── Source DuckDB files ──────────────────────────────────────────

    /// Pull per-source `DuckDB` files from R2 to local `data/sources/`.
    ///
    /// If `source_ids` is empty, pulls all known sources from the TOML
    /// registry. Missing R2 objects are skipped with a warning (expected on
    /// first run).
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Download`] on S3 failures, [`R2Error::Io`] on
    /// local filesystem errors.
    pub async fn pull_sources(&self, source_ids: &[String]) -> Result<SyncStats, R2Error> {
        let ids = resolve_source_ids(source_ids);
        paths::ensure_dir(&paths::sources_dir())?;

        let mut stats = SyncStats::default();
        for id in &ids {
            let key = format!("sources/{id}.duckdb");
            let local = paths::source_db_path(id);
            stats.merge(self.download(&key, &local).await?);
        }

        Ok(stats)
    }

    /// Push per-source `DuckDB` files from local `data/sources/` to R2.
    ///
    /// If `source_ids` is empty, pushes all known sources that have a local
    /// file. Sources without a local `DuckDB` file are skipped.
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Upload`] on S3 failures, [`R2Error::Io`] on
    /// local filesystem errors.
    pub async fn push_sources(&self, source_ids: &[String]) -> Result<SyncStats, R2Error> {
        let ids = resolve_source_ids(source_ids);

        let mut stats = SyncStats::default();
        for id in &ids {
            let key = format!("sources/{id}.duckdb");
            let local = paths::source_db_path(id);
            stats.merge(self.upload(&key, &local).await?);
        }

        Ok(stats)
    }

    // ── Shared databases ────────────────────────────────────────────

    /// Pull shared databases (`boundaries.duckdb`, `geocode_cache.duckdb`)
    /// from R2.
    ///
    /// Also pulls the geocoder index archive if it exists on R2.
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Download`] on S3 failures, [`R2Error::Io`] on
    /// local filesystem errors.
    pub async fn pull_shared(&self) -> Result<SyncStats, R2Error> {
        paths::ensure_dir(&paths::shared_dir())?;

        let mut stats = SyncStats::default();

        stats.merge(
            self.download("shared/boundaries.duckdb", &paths::boundaries_db_path())
                .await?,
        );

        stats.merge(
            self.download(
                "shared/geocode_cache.duckdb",
                &paths::geocode_cache_db_path(),
            )
            .await?,
        );

        // Pull geocoder index archive (optional — may not exist yet)
        let archive_path = paths::shared_dir().join("geocoder_index.tar.zst");
        stats.merge(
            self.download("shared/geocoder_index.tar.zst", &archive_path)
                .await?,
        );

        Ok(stats)
    }

    /// Push shared databases to R2.
    ///
    /// Also pushes the geocoder index archive if it exists locally.
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Upload`] on S3 failures, [`R2Error::Io`] on
    /// local filesystem errors.
    pub async fn push_shared(&self) -> Result<SyncStats, R2Error> {
        let mut stats = SyncStats::default();

        stats.merge(
            self.upload("shared/boundaries.duckdb", &paths::boundaries_db_path())
                .await?,
        );

        stats.merge(
            self.upload(
                "shared/geocode_cache.duckdb",
                &paths::geocode_cache_db_path(),
            )
            .await?,
        );

        // Push geocoder index archive if it exists
        let archive_path = paths::shared_dir().join("geocoder_index.tar.zst");
        stats.merge(
            self.upload("shared/geocoder_index.tar.zst", &archive_path)
                .await?,
        );

        Ok(stats)
    }

    // ── Generated outputs ───────────────────────────────────────────

    /// Push merged generated outputs to R2 under `generated/merged/`.
    ///
    /// Uploads all standard generated files from `dir` (incidents, counts,
    /// h3, analytics, boundaries, metadata, manifest).
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Upload`] on S3 failures, [`R2Error::Io`] on
    /// local filesystem errors.
    pub async fn push_generated_merged(&self, dir: &Path) -> Result<SyncStats, R2Error> {
        let mut stats = SyncStats::default();
        for &file in GENERATED_FILES.iter().chain(BOUNDARY_FILES.iter()) {
            let key = format!("generated/merged/{file}");
            let local = dir.join(file);
            stats.merge(self.upload(&key, &local).await?);
        }
        Ok(stats)
    }

    /// Pull merged generated outputs from R2 `generated/merged/` to `dir`.
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Download`] on S3 failures, [`R2Error::Io`] on
    /// local filesystem errors.
    pub async fn pull_generated_merged(&self, dir: &Path) -> Result<SyncStats, R2Error> {
        paths::ensure_dir(dir)?;
        let mut stats = SyncStats::default();
        for &file in GENERATED_FILES.iter().chain(BOUNDARY_FILES.iter()) {
            let key = format!("generated/merged/{file}");
            let local = dir.join(file);
            stats.merge(self.download(&key, &local).await?);
        }
        Ok(stats)
    }

    /// Push per-partition generated outputs to R2 under
    /// `generated/partitions/{name}/`.
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Upload`] on S3 failures, [`R2Error::Io`] on
    /// local filesystem errors.
    pub async fn push_generated_partition(
        &self,
        name: &str,
        dir: &Path,
    ) -> Result<SyncStats, R2Error> {
        let mut stats = SyncStats::default();
        for &file in GENERATED_FILES {
            let key = format!("generated/partitions/{name}/{file}");
            let local = dir.join(file);
            stats.merge(self.upload(&key, &local).await?);
        }
        Ok(stats)
    }

    /// Pull per-partition generated outputs from R2
    /// `generated/partitions/{name}/` to `dir`.
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Download`] on S3 failures, [`R2Error::Io`] on
    /// local filesystem errors.
    pub async fn pull_generated_partition(
        &self,
        name: &str,
        dir: &Path,
    ) -> Result<SyncStats, R2Error> {
        paths::ensure_dir(dir)?;
        let mut stats = SyncStats::default();
        for &file in GENERATED_FILES {
            let key = format!("generated/partitions/{name}/{file}");
            let local = dir.join(file);
            stats.merge(self.download(&key, &local).await?);
        }
        Ok(stats)
    }

    /// Push boundary outputs to R2 under `generated/boundaries/`.
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Upload`] on S3 failures, [`R2Error::Io`] on
    /// local filesystem errors.
    pub async fn push_generated_boundaries(&self, dir: &Path) -> Result<SyncStats, R2Error> {
        let mut stats = SyncStats::default();
        for &file in BOUNDARY_FILES {
            let key = format!("generated/boundaries/{file}");
            let local = dir.join(file);
            stats.merge(self.upload(&key, &local).await?);
        }
        Ok(stats)
    }

    /// Pull boundary outputs from R2 `generated/boundaries/` to `dir`.
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Download`] on S3 failures, [`R2Error::Io`] on
    /// local filesystem errors.
    pub async fn pull_generated_boundaries(&self, dir: &Path) -> Result<SyncStats, R2Error> {
        paths::ensure_dir(dir)?;
        let mut stats = SyncStats::default();
        for &file in BOUNDARY_FILES {
            let key = format!("generated/boundaries/{file}");
            let local = dir.join(file);
            stats.merge(self.download(&key, &local).await?);
        }
        Ok(stats)
    }

    /// List all partition names that have generated outputs on R2.
    ///
    /// Scans the `generated/partitions/` prefix and extracts distinct
    /// partition directory names.
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::List`] on S3 failures.
    pub async fn list_generated_partitions(&self) -> Result<Vec<String>, R2Error> {
        let keys = self.list_keys("generated/partitions/").await?;
        let mut names = std::collections::BTreeSet::new();
        for key in &keys {
            if let Some(name) = key
                .strip_prefix("generated/partitions/")
                .and_then(|rest| rest.split('/').next())
                .filter(|n| !n.is_empty())
            {
                names.insert(name.to_string());
            }
        }
        Ok(names.into_iter().collect())
    }

    // ── Low-level operations ────────────────────────────────────────

    /// Downloads an object from R2 to a local file.
    ///
    /// Uses **size + MD5/ETag comparison** to skip the download when the
    /// local file already matches the remote object. Retries up to
    /// [`MAX_DOWNLOAD_ATTEMPTS`] times on transient errors.
    ///
    /// Returns stats indicating whether the file was transferred, skipped,
    /// or not found.
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Download`] on S3 failures after all retries are
    /// exhausted, [`R2Error::Io`] on local filesystem errors.
    pub async fn download(&self, key: &str, local_path: &Path) -> Result<SyncStats, R2Error> {
        log::info!("Pulling s3://{BUCKET}/{key} -> {}", local_path.display());

        // Check if we can skip via smart sync
        if let Some(remote) = self.head(key).await? {
            if is_local_match(local_path, &remote).await {
                log::info!("  skipped (unchanged)");
                return Ok(SyncStats {
                    skipped: 1,
                    ..SyncStats::default()
                });
            }
        } else {
            // Object doesn't exist on R2
            log::warn!("  not found in R2 (first run?), skipping");
            return Ok(SyncStats {
                not_found: 1,
                ..SyncStats::default()
            });
        }

        let mut last_err: Option<R2Error> = None;

        for attempt in 1..=MAX_DOWNLOAD_ATTEMPTS {
            match self.download_once(key, local_path).await {
                Ok(()) => {
                    return Ok(SyncStats {
                        transferred: 1,
                        ..SyncStats::default()
                    });
                }
                Err(e @ R2Error::Download { .. }) if attempt < MAX_DOWNLOAD_ATTEMPTS => {
                    let delay = RETRY_BASE_DELAY * 2u32.saturating_pow(attempt - 1);
                    log::warn!(
                        "  download attempt {attempt}/{MAX_DOWNLOAD_ATTEMPTS} failed, \
                         retrying in {delay:.1?}..."
                    );
                    last_err = Some(e);
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }

        Err(last_err.unwrap_or_else(|| R2Error::Download {
            bucket: BUCKET.to_string(),
            key: key.to_string(),
            source: "all download attempts exhausted".into(),
        }))
    }

    /// Single download attempt (always transfers, no smart-sync check).
    async fn download_once(&self, key: &str, local_path: &Path) -> Result<(), R2Error> {
        let output = self
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .map_err(|e| R2Error::Download {
                bucket: BUCKET.to_string(),
                key: key.to_string(),
                source: Box::new(e),
            })?;

        let bytes = output.body.collect().await.map_err(|e| R2Error::Download {
            bucket: BUCKET.to_string(),
            key: key.to_string(),
            source: Box::new(e),
        })?;

        tokio::fs::write(local_path, bytes.into_bytes()).await?;
        let size = tokio::fs::metadata(local_path).await?.len();
        #[allow(clippy::cast_precision_loss)] // display-only MB value
        let mb = size as f64 / 1_048_576.0;
        log::info!("  downloaded {} ({mb:.1} MB)", local_path.display());

        Ok(())
    }

    /// Uploads a local file to R2.
    ///
    /// Uses **size + MD5/ETag comparison** to skip the upload when the
    /// remote object already matches the local file.
    ///
    /// Returns stats indicating whether the file was transferred, skipped,
    /// or not found.
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Upload`] on S3 failures, [`R2Error::Io`] on
    /// local filesystem errors.
    pub async fn upload(&self, key: &str, local_path: &Path) -> Result<SyncStats, R2Error> {
        if !local_path.exists() {
            log::warn!(
                "  {} does not exist locally, skipping",
                local_path.display()
            );
            return Ok(SyncStats {
                not_found: 1,
                ..SyncStats::default()
            });
        }

        // Check if remote already matches
        if let Some(remote) = self.head(key).await?
            && is_local_match(local_path, &remote).await
        {
            log::info!(
                "  {} -> s3://{BUCKET}/{key}: skipped (unchanged)",
                local_path.display()
            );
            return Ok(SyncStats {
                skipped: 1,
                ..SyncStats::default()
            });
        }

        let data = tokio::fs::read(local_path).await?;
        let size = data.len();
        #[allow(clippy::cast_precision_loss)] // display-only MB value
        let mb = size as f64 / 1_048_576.0;
        log::info!(
            "Pushing {} -> s3://{BUCKET}/{key} ({mb:.1} MB)",
            local_path.display(),
        );

        let body = aws_sdk_s3::primitives::ByteStream::from(data);

        self.client
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(body)
            .content_type("application/octet-stream")
            .send()
            .await
            .map_err(|e| R2Error::Upload {
                bucket: BUCKET.to_string(),
                key: key.to_string(),
                source: Box::new(e),
            })?;

        log::info!("  uploaded {key}");
        Ok(SyncStats {
            transferred: 1,
            ..SyncStats::default()
        })
    }

    /// Deletes an object from R2.
    ///
    /// Silently succeeds if the object doesn't exist (S3 `DeleteObject`
    /// is idempotent).
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Delete`] on S3 failures.
    pub async fn delete(&self, key: &str) -> Result<(), R2Error> {
        log::info!("Deleting s3://{BUCKET}/{key}");

        self.client
            .delete_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .map_err(|e| R2Error::Delete {
                bucket: BUCKET.to_string(),
                key: key.to_string(),
                source: Box::new(e),
            })?;

        Ok(())
    }

    /// Lists all object keys under a prefix in R2.
    ///
    /// Returns the full keys (not stripped of the prefix).
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::List`] on S3 failures.
    pub async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, R2Error> {
        log::info!("Listing s3://{BUCKET}/{prefix}*");

        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self.client.list_objects_v2().bucket(BUCKET).prefix(prefix);

            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let output = request.send().await.map_err(|e| R2Error::List {
                bucket: BUCKET.to_string(),
                prefix: prefix.to_string(),
                source: Box::new(e),
            })?;

            for obj in output.contents() {
                if let Some(key) = obj.key() {
                    keys.push(key.to_string());
                }
            }

            if output.is_truncated() == Some(true) {
                continuation_token = output.next_continuation_token().map(String::from);
            } else {
                break;
            }
        }

        log::info!("  found {} objects", keys.len());
        Ok(keys)
    }

    /// Fetch object metadata via `HeadObject`.
    ///
    /// Returns `None` if the object doesn't exist (`NotFound`).
    async fn head(&self, key: &str) -> Result<Option<RemoteMeta>, R2Error> {
        let result = self
            .client
            .head_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await;

        match result {
            Ok(output) => {
                let size = output.content_length().unwrap_or(0);
                #[allow(clippy::cast_sign_loss)] // S3 content-length is non-negative
                let size = size as u64;
                let etag = output.e_tag().map(str::to_string);
                Ok(Some(RemoteMeta { size, etag }))
            }
            Err(err) => {
                // NotFound is not an error — it means the object doesn't exist
                let service_err = err.as_service_error();
                if service_err
                    .is_some_and(aws_sdk_s3::operation::head_object::HeadObjectError::is_not_found)
                {
                    return Ok(None);
                }
                Err(R2Error::Head {
                    bucket: BUCKET.to_string(),
                    key: key.to_string(),
                    source: Box::new(err),
                })
            }
        }
    }
}

/// Checks whether a local file matches the remote object.
///
/// 1. If the local file doesn't exist, returns `false`.
/// 2. Compares file sizes — if they differ, returns `false` immediately.
/// 3. If sizes match AND the remote `ETag` looks like an MD5 hex digest
///    (non-multipart upload), computes the local file's MD5 and compares.
/// 4. If the `ETag` is a multipart `ETag` (contains `-`), falls back to
///    size-only comparison.
async fn is_local_match(local_path: &Path, remote: &RemoteMeta) -> bool {
    let Ok(meta) = tokio::fs::metadata(local_path).await else {
        return false;
    };

    let local_size = meta.len();
    if local_size != remote.size {
        return false;
    }

    // Sizes match — try MD5/ETag comparison for stronger guarantee
    if let Some(etag) = &remote.etag {
        let clean = etag.trim_matches('"');
        // Multipart ETags contain a `-` (e.g. "abc123-5") — skip MD5 check
        if !clean.contains('-') && clean.len() == 32 {
            // Looks like an MD5 hex digest — compute local MD5
            if let Ok(local_md5) = compute_md5(local_path).await {
                return local_md5 == clean;
            }
        }
    }

    // Fall back to size-only match (sizes were equal)
    true
}

/// Compute MD5 hex digest of a local file.
async fn compute_md5(path: &Path) -> Result<String, std::io::Error> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || compute_md5_sync(&path))
        .await
        .map_err(std::io::Error::other)?
}

/// Synchronous MD5 computation (runs in blocking thread).
fn compute_md5_sync(path: &Path) -> Result<String, std::io::Error> {
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let mut context = md5::Context::new();
    let mut buffer = vec![0u8; 256 * 1024]; // 256 KB chunks
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        context.consume(&buffer[..n]);
    }
    Ok(format!("{:x}", context.finalize()))
}

/// Resolves source IDs: if `source_ids` is empty, returns all known source
/// IDs from the compile-time TOML registry.
fn resolve_source_ids(source_ids: &[String]) -> Vec<String> {
    if source_ids.is_empty() {
        registry::all_sources()
            .into_iter()
            .map(|s| s.id().to_string())
            .collect()
    } else {
        source_ids.to_vec()
    }
}

/// Reads a required environment variable.
fn require_env(name: &str) -> Result<String, R2Error> {
    std::env::var(name).map_err(|_| R2Error::MissingEnv {
        name: name.to_string(),
    })
}

/// Returns the list of generated file names per partition/merged output.
#[must_use]
pub const fn generated_files() -> &'static [&'static str] {
    GENERATED_FILES
}

/// Returns the list of boundary file names.
#[must_use]
pub const fn boundary_files() -> &'static [&'static str] {
    BOUNDARY_FILES
}

/// Returns the R2 key path for a generated partition directory.
#[must_use]
pub fn generated_partition_prefix(name: &str) -> String {
    format!("generated/partitions/{name}/")
}

/// Returns the generated boundaries R2 prefix.
#[must_use]
pub const fn generated_boundaries_prefix() -> &'static str {
    "generated/boundaries/"
}

/// Returns the generated merged R2 prefix.
#[must_use]
pub const fn generated_merged_prefix() -> &'static str {
    "generated/merged/"
}
