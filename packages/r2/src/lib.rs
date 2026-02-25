#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Cloudflare R2 sync for `DuckDB` data files.
//!
//! Provides pull (download) and push (upload) operations between the local
//! `data/` directory and the `crime-map-data` R2 bucket.
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
    pub async fn pull_sources(&self, source_ids: &[String]) -> Result<u64, R2Error> {
        let ids = resolve_source_ids(source_ids);
        paths::ensure_dir(&paths::sources_dir())?;

        let mut count = 0u64;
        for id in &ids {
            let key = format!("sources/{id}.duckdb");
            let local = paths::source_db_path(id);
            if self.download(&key, &local).await? {
                count += 1;
            }
        }

        Ok(count)
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
    pub async fn push_sources(&self, source_ids: &[String]) -> Result<u64, R2Error> {
        let ids = resolve_source_ids(source_ids);

        let mut count = 0u64;
        for id in &ids {
            let key = format!("sources/{id}.duckdb");
            let local = paths::source_db_path(id);
            if self.upload(&key, &local).await? {
                count += 1;
            }
        }

        Ok(count)
    }

    /// Pull shared databases (`boundaries.duckdb`, `geocode_cache.duckdb`)
    /// from R2.
    ///
    /// Also pulls the geocoder index archive if it exists on R2.
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Download`] on S3 failures, [`R2Error::Io`] on
    /// local filesystem errors.
    pub async fn pull_shared(&self) -> Result<u64, R2Error> {
        paths::ensure_dir(&paths::shared_dir())?;

        let mut count = 0u64;

        if self
            .download("shared/boundaries.duckdb", &paths::boundaries_db_path())
            .await?
        {
            count += 1;
        }

        if self
            .download(
                "shared/geocode_cache.duckdb",
                &paths::geocode_cache_db_path(),
            )
            .await?
        {
            count += 1;
        }

        // Pull geocoder index archive (optional — may not exist yet)
        let archive_path = paths::shared_dir().join("geocoder_index.tar.zst");
        if self
            .download("shared/geocoder_index.tar.zst", &archive_path)
            .await?
        {
            count += 1;
        }

        Ok(count)
    }

    /// Push shared databases to R2.
    ///
    /// Also pushes the geocoder index archive if it exists locally.
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Upload`] on S3 failures, [`R2Error::Io`] on
    /// local filesystem errors.
    pub async fn push_shared(&self) -> Result<u64, R2Error> {
        let mut count = 0u64;

        if self
            .upload("shared/boundaries.duckdb", &paths::boundaries_db_path())
            .await?
        {
            count += 1;
        }

        if self
            .upload(
                "shared/geocode_cache.duckdb",
                &paths::geocode_cache_db_path(),
            )
            .await?
        {
            count += 1;
        }

        // Push geocoder index archive if it exists
        let archive_path = paths::shared_dir().join("geocoder_index.tar.zst");
        if self
            .upload("shared/geocoder_index.tar.zst", &archive_path)
            .await?
        {
            count += 1;
        }

        Ok(count)
    }

    /// Downloads an object from R2 to a local file.
    ///
    /// Retries up to [`MAX_DOWNLOAD_ATTEMPTS`] times on transient errors
    /// (network failures, incomplete body streams). Uses exponential backoff
    /// between attempts.
    ///
    /// Returns `true` if the file was downloaded, `false` if the object
    /// doesn't exist in R2 (logged as a warning).
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Download`] on S3 failures after all retries are
    /// exhausted, [`R2Error::Io`] on local filesystem errors.
    pub async fn download(&self, key: &str, local_path: &Path) -> Result<bool, R2Error> {
        log::info!("Pulling s3://{BUCKET}/{key} -> {}", local_path.display());

        let mut last_err: Option<R2Error> = None;

        for attempt in 1..=MAX_DOWNLOAD_ATTEMPTS {
            match self.download_once(key, local_path).await {
                Ok(found) => return Ok(found),
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

    /// Single download attempt. Separated from [`Self::download`] to keep
    /// the retry loop clean.
    async fn download_once(&self, key: &str, local_path: &Path) -> Result<bool, R2Error> {
        let result = self
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await;

        let output = match result {
            Ok(output) => output,
            Err(err) => {
                // Check for NoSuchKey (expected on first run)
                let service_err = err.as_service_error();
                if service_err
                    .is_some_and(aws_sdk_s3::operation::get_object::GetObjectError::is_no_such_key)
                {
                    log::warn!("  not found in R2 (first run?), skipping");
                    return Ok(false);
                }
                return Err(R2Error::Download {
                    bucket: BUCKET.to_string(),
                    key: key.to_string(),
                    source: Box::new(err),
                });
            }
        };

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

        Ok(true)
    }

    /// Uploads a local file to R2.
    ///
    /// Returns `true` if the file was uploaded, `false` if the local file
    /// doesn't exist (logged as a warning).
    ///
    /// # Errors
    ///
    /// Returns [`R2Error::Upload`] on S3 failures, [`R2Error::Io`] on
    /// local filesystem errors.
    pub async fn upload(&self, key: &str, local_path: &Path) -> Result<bool, R2Error> {
        if !local_path.exists() {
            log::warn!(
                "  {} does not exist locally, skipping",
                local_path.display()
            );
            return Ok(false);
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
        Ok(true)
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
