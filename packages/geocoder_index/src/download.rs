//! Data download helpers for `OpenAddresses` and OSM PBF.
//!
//! Downloads raw address data from public sources and stores it in
//! the local `data/shared/` directory.

use std::path::Path;

use futures::StreamExt as _;
use tokio::io::AsyncWriteExt as _;

/// Default URL for the US OSM PBF extract from Geofabrik.
pub const GEOFABRIK_US_PBF_URL: &str =
    "https://download.geofabrik.de/north-america/us-latest.osm.pbf";

/// Downloads a file from a URL to a local path with progress logging.
///
/// Uses streaming to avoid loading the entire file into memory.
///
/// # Errors
///
/// Returns an error if the HTTP request fails, the response is not
/// successful, or the local file cannot be written.
pub async fn download_file(url: &str, dest: &Path) -> Result<u64, DownloadError> {
    log::info!("Downloading {url}");
    log::info!("  -> {}", dest.display());

    // Ensure parent directory exists
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| DownloadError::Io {
                path: parent.display().to_string(),
                source: e,
            })?;
    }

    let client = reqwest::Client::builder()
        .user_agent("crime-map-geocoder-index/0.1")
        .build()
        .map_err(DownloadError::Http)?;

    let response = client.get(url).send().await.map_err(DownloadError::Http)?;

    if !response.status().is_success() {
        return Err(DownloadError::HttpStatus {
            url: url.to_string(),
            status: response.status().as_u16(),
        });
    }

    let total_size = response.content_length();
    if let Some(size) = total_size {
        #[allow(clippy::cast_precision_loss)]
        let mb = size as f64 / 1_048_576.0;
        log::info!("  file size: {mb:.1} MB");
    }

    let mut file = tokio::fs::File::create(dest)
        .await
        .map_err(|e| DownloadError::Io {
            path: dest.display().to_string(),
            source: e,
        })?;

    let mut stream = response.bytes_stream();
    let mut downloaded: u64 = 0;
    let mut last_log_mb: u64 = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(DownloadError::Http)?;
        file.write_all(&chunk)
            .await
            .map_err(|e| DownloadError::Io {
                path: dest.display().to_string(),
                source: e,
            })?;
        downloaded += chunk.len() as u64;

        // Log progress every 100 MB
        let current_mb = downloaded / (100 * 1_048_576);
        if current_mb > last_log_mb {
            last_log_mb = current_mb;
            #[allow(clippy::cast_precision_loss)]
            let mb = downloaded as f64 / 1_048_576.0;
            if let Some(total) = total_size {
                #[allow(clippy::cast_precision_loss)]
                let pct = (downloaded as f64 / total as f64) * 100.0;
                log::info!("  downloaded {mb:.0} MB ({pct:.0}%)");
            } else {
                log::info!("  downloaded {mb:.0} MB");
            }
        }
    }

    file.flush().await.map_err(|e| DownloadError::Io {
        path: dest.display().to_string(),
        source: e,
    })?;

    #[allow(clippy::cast_precision_loss)]
    let mb = downloaded as f64 / 1_048_576.0;
    log::info!("  download complete: {mb:.1} MB");

    Ok(downloaded)
}

/// Errors from download operations.
#[derive(Debug, thiserror::Error)]
pub enum DownloadError {
    /// HTTP request error.
    #[error("HTTP error: {0}")]
    Http(reqwest::Error),

    /// Non-success HTTP status.
    #[error("HTTP {status} for {url}")]
    HttpStatus {
        /// Request URL.
        url: String,
        /// HTTP status code.
        status: u16,
    },

    /// I/O error writing to disk.
    #[error("I/O error at {path}: {source}")]
    Io {
        /// Path that caused the error.
        path: String,
        /// Underlying I/O error.
        source: std::io::Error,
    },
}
