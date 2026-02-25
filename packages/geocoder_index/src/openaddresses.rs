//! `OpenAddresses` CSV parser.
//!
//! Parses the standardized `OpenAddresses` CSV format and yields
//! normalized address records for indexing.

use std::io::Read;
use std::path::Path;

use serde::Deserialize;

use crate::normalize;

/// A raw record from an `OpenAddresses` CSV file.
#[derive(Debug, Deserialize)]
pub struct OaRecord {
    /// Longitude (WGS84).
    #[serde(rename = "LON")]
    pub lon: f64,
    /// Latitude (WGS84).
    #[serde(rename = "LAT")]
    pub lat: f64,
    /// House number.
    #[serde(rename = "NUMBER", default)]
    pub number: String,
    /// Street name.
    #[serde(rename = "STREET", default)]
    pub street: String,
    /// Unit/apartment.
    #[serde(rename = "UNIT", default)]
    pub unit: Option<String>,
    /// City name.
    #[serde(rename = "CITY", default)]
    pub city: Option<String>,
    /// District/county.
    #[serde(rename = "DISTRICT", default)]
    pub district: Option<String>,
    /// State/province/region.
    #[serde(rename = "REGION", default)]
    pub region: Option<String>,
    /// Postal/ZIP code.
    #[serde(rename = "POSTCODE", default)]
    pub postcode: Option<String>,
    /// Source-specific ID.
    #[serde(rename = "ID", default)]
    pub id: Option<String>,
    /// Content hash.
    #[serde(rename = "HASH", default)]
    pub hash: Option<String>,
}

/// A normalized address record ready for indexing.
#[derive(Debug, Clone)]
pub struct NormalizedAddress {
    /// Normalized street (e.g., "100 NORTH STATE STREET").
    pub street: String,
    /// Normalized city name.
    pub city: String,
    /// Two-letter state code.
    pub state: String,
    /// ZIP/postal code.
    pub postcode: String,
    /// Composite full address for the `full_address` field.
    pub full_address: String,
    /// Latitude.
    pub lat: f64,
    /// Longitude.
    pub lon: f64,
}

impl OaRecord {
    /// Converts this raw record into a normalized address.
    ///
    /// Returns `None` if the record is missing required fields
    /// (number, street, or valid coordinates).
    #[must_use]
    pub fn to_normalized(&self) -> Option<NormalizedAddress> {
        let number = self.number.trim();
        let street_raw = self.street.trim();

        if number.is_empty() || street_raw.is_empty() {
            return None;
        }

        if !self.lat.is_finite() || !self.lon.is_finite() {
            return None;
        }

        // Skip obviously invalid coordinates
        if self.lat < -90.0 || self.lat > 90.0 || self.lon < -180.0 || self.lon > 180.0 {
            return None;
        }

        let street = normalize::normalize_street(number, street_raw);
        if street.is_empty() {
            return None;
        }

        let city = self
            .city
            .as_deref()
            .map(normalize::normalize)
            .unwrap_or_default();
        let state = self
            .region
            .as_deref()
            .map(normalize::normalize_state)
            .unwrap_or_default();
        let postcode = self
            .postcode
            .as_deref()
            .map(|p| p.trim().to_string())
            .unwrap_or_default();

        let full_address = normalize::build_full_address(&street, &city, &state);

        Some(NormalizedAddress {
            street,
            city,
            state,
            postcode,
            full_address,
            lat: self.lat,
            lon: self.lon,
        })
    }
}

/// Reads and parses all `OpenAddresses` CSV files in a directory.
///
/// Yields normalized addresses from all `.csv` files found recursively.
///
/// # Errors
///
/// Returns an error if a CSV file cannot be read or parsed.
pub fn parse_directory(
    dir: &Path,
    mut on_record: impl FnMut(NormalizedAddress),
) -> Result<u64, OaError> {
    let mut total = 0u64;

    if !dir.exists() {
        return Err(OaError::DirectoryNotFound(dir.display().to_string()));
    }

    let csv_files = collect_csv_files(dir)?;
    if csv_files.is_empty() {
        log::warn!("No CSV files found in {}", dir.display());
        return Ok(0);
    }

    log::info!("Found {} OpenAddresses CSV files", csv_files.len());

    for csv_path in &csv_files {
        match parse_single_file(csv_path, &mut on_record) {
            Ok(count) => {
                total += count;
                log::debug!("  parsed {} records from {}", count, csv_path.display());
            }
            Err(e) => {
                log::warn!("  skipping {}: {e}", csv_path.display());
            }
        }
    }

    Ok(total)
}

/// Reads and parses `OpenAddresses` CSV files from a `.tar.zst` archive.
///
/// Streams the archive in-memory: no files are extracted to disk.
/// Each `.csv` entry in the archive is parsed for address records.
///
/// # Errors
///
/// Returns an error if the archive cannot be read or a CSV entry
/// cannot be parsed.
pub fn parse_tar_zst_archive(
    archive_path: &Path,
    mut on_record: impl FnMut(NormalizedAddress),
) -> Result<u64, OaError> {
    if !archive_path.exists() {
        return Err(OaError::DirectoryNotFound(
            archive_path.display().to_string(),
        ));
    }

    log::info!(
        "Streaming OpenAddresses data from archive: {}",
        archive_path.display()
    );

    let file = std::fs::File::open(archive_path).map_err(|e| OaError::Io {
        path: archive_path.display().to_string(),
        source: e,
    })?;

    let decoder = zstd::Decoder::new(file).map_err(|e| OaError::Io {
        path: archive_path.display().to_string(),
        source: e,
    })?;

    let mut archive = tar::Archive::new(decoder);
    let mut total = 0u64;
    let mut csv_files_seen = 0u64;

    let entries = archive.entries().map_err(|e| OaError::Io {
        path: archive_path.display().to_string(),
        source: e,
    })?;

    for entry_result in entries {
        let entry = match entry_result {
            Ok(e) => e,
            Err(e) => {
                log::warn!("  skipping corrupt tar entry: {e}");
                continue;
            }
        };

        let entry_path = match entry.header().path() {
            Ok(p) => p.to_path_buf(),
            Err(e) => {
                log::trace!("  skipping entry with unreadable path: {e}");
                continue;
            }
        };

        // Only process .csv files
        let is_csv = entry_path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("csv"));
        if !is_csv {
            continue;
        }

        csv_files_seen += 1;
        match parse_csv_reader(entry, &mut on_record) {
            Ok(count) => {
                total += count;
                if csv_files_seen.is_multiple_of(100) {
                    log::info!("  processed {csv_files_seen} CSV files, {total} records so far...");
                }
            }
            Err(e) => {
                log::warn!("  skipping {}: {e}", entry_path.display());
            }
        }
    }

    log::info!("  archive complete: {csv_files_seen} CSV files, {total} records");

    Ok(total)
}

/// Reads and parses `OpenAddresses` CSV files from a `.zip` archive.
///
/// Reads zip entries sequentially; no files are extracted to disk.
/// Each `.csv` entry in the archive is parsed for address records.
///
/// # Errors
///
/// Returns an error if the archive cannot be read or a CSV entry
/// cannot be parsed.
pub fn parse_zip_archive(
    archive_path: &Path,
    mut on_record: impl FnMut(NormalizedAddress),
) -> Result<u64, OaError> {
    if !archive_path.exists() {
        return Err(OaError::DirectoryNotFound(
            archive_path.display().to_string(),
        ));
    }

    log::info!(
        "Streaming OpenAddresses data from zip archive: {}",
        archive_path.display()
    );

    let file = std::fs::File::open(archive_path).map_err(|e| OaError::Io {
        path: archive_path.display().to_string(),
        source: e,
    })?;

    let mut archive = zip::ZipArchive::new(file).map_err(|e| OaError::Io {
        path: archive_path.display().to_string(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
    })?;

    let mut total = 0u64;
    let mut csv_files_seen = 0u64;

    for i in 0..archive.len() {
        let entry = match archive.by_index(i) {
            Ok(e) => e,
            Err(e) => {
                log::warn!("  skipping corrupt zip entry {i}: {e}");
                continue;
            }
        };

        let entry_name = entry.name().to_string();

        // Only process .csv files
        if !entry_name.to_ascii_lowercase().ends_with(".csv") {
            continue;
        }

        csv_files_seen += 1;
        match parse_csv_reader(entry, &mut on_record) {
            Ok(count) => {
                total += count;
                if csv_files_seen.is_multiple_of(100) {
                    log::info!("  processed {csv_files_seen} CSV files, {total} records so far...");
                }
            }
            Err(e) => {
                log::warn!("  skipping {entry_name}: {e}");
            }
        }
    }

    log::info!("  zip complete: {csv_files_seen} CSV files, {total} records");

    Ok(total)
}

/// Parses a single `OpenAddresses` CSV file.
fn parse_single_file(
    path: &Path,
    on_record: &mut impl FnMut(NormalizedAddress),
) -> Result<u64, OaError> {
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(path)
        .map_err(|e| OaError::Csv {
            path: path.display().to_string(),
            source: e,
        })?;

    let mut count = 0u64;
    for result in reader.deserialize::<OaRecord>() {
        let record = match result {
            Ok(r) => r,
            Err(e) => {
                log::trace!("  skipping malformed row: {e}");
                continue;
            }
        };

        if let Some(normalized) = record.to_normalized() {
            on_record(normalized);
            count += 1;
        }
    }

    Ok(count)
}

/// Parses `OpenAddresses` CSV records from any `Read` source.
fn parse_csv_reader(
    reader: impl Read,
    on_record: &mut impl FnMut(NormalizedAddress),
) -> Result<u64, OaError> {
    let mut csv_reader = csv::ReaderBuilder::new().flexible(true).from_reader(reader);

    let mut count = 0u64;
    for result in csv_reader.deserialize::<OaRecord>() {
        let record = match result {
            Ok(r) => r,
            Err(e) => {
                log::trace!("  skipping malformed row: {e}");
                continue;
            }
        };

        if let Some(normalized) = record.to_normalized() {
            on_record(normalized);
            count += 1;
        }
    }

    Ok(count)
}

/// Recursively collects all `.csv` files under a directory.
fn collect_csv_files(dir: &Path) -> Result<Vec<std::path::PathBuf>, OaError> {
    let mut files = Vec::new();
    collect_csv_files_recursive(dir, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_csv_files_recursive(
    dir: &Path,
    files: &mut Vec<std::path::PathBuf>,
) -> Result<(), OaError> {
    let entries = std::fs::read_dir(dir).map_err(|e| OaError::Io {
        path: dir.display().to_string(),
        source: e,
    })?;

    for entry in entries {
        let entry = entry.map_err(|e| OaError::Io {
            path: dir.display().to_string(),
            source: e,
        })?;

        let path = entry.path();
        if path.is_dir() {
            collect_csv_files_recursive(&path, files)?;
        } else if path.extension().is_some_and(|ext| ext == "csv") {
            files.push(path);
        }
    }

    Ok(())
}

/// Errors from `OpenAddresses` parsing.
#[derive(Debug, thiserror::Error)]
pub enum OaError {
    /// CSV parsing error.
    #[error("CSV error in {path}: {source}")]
    Csv {
        /// Path to the CSV file.
        path: String,
        /// Underlying CSV error.
        source: csv::Error,
    },

    /// I/O error reading directory or file.
    #[error("I/O error at {path}: {source}")]
    Io {
        /// Path that caused the error.
        path: String,
        /// Underlying I/O error.
        source: std::io::Error,
    },

    /// Directory does not exist.
    #[error("Directory not found: {0}")]
    DirectoryNotFound(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_basic_record() {
        let record = OaRecord {
            lon: -87.6278,
            lat: 41.8827,
            number: "100".to_string(),
            street: "N STATE ST".to_string(),
            unit: None,
            city: Some("CHICAGO".to_string()),
            district: None,
            region: Some("IL".to_string()),
            postcode: Some("60602".to_string()),
            id: None,
            hash: None,
        };

        let normalized = record.to_normalized().unwrap();
        assert_eq!(normalized.street, "100 NORTH STATE STREET");
        assert_eq!(normalized.city, "CHICAGO");
        assert_eq!(normalized.state, "IL");
        assert!((normalized.lat - 41.8827).abs() < 1e-4);
    }

    #[test]
    fn skips_missing_number() {
        let record = OaRecord {
            lon: -87.6278,
            lat: 41.8827,
            number: String::new(),
            street: "STATE ST".to_string(),
            unit: None,
            city: None,
            district: None,
            region: None,
            postcode: None,
            id: None,
            hash: None,
        };
        assert!(record.to_normalized().is_none());
    }

    #[test]
    fn skips_invalid_coordinates() {
        let record = OaRecord {
            lon: f64::NAN,
            lat: 41.8827,
            number: "100".to_string(),
            street: "MAIN ST".to_string(),
            unit: None,
            city: None,
            district: None,
            region: None,
            postcode: None,
            id: None,
            hash: None,
        };
        assert!(record.to_normalized().is_none());
    }

    #[test]
    fn parses_zip_archive() {
        use std::io::Write;

        let tmp = std::env::temp_dir().join("oa_zip_test");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let zip_path = tmp.join("test.zip");
        let csv_data = b"LON,LAT,NUMBER,STREET,UNIT,CITY,DISTRICT,REGION,POSTCODE,ID,HASH\n\
            -87.6278,41.8827,100,N STATE ST,,CHICAGO,,IL,60602,,\n\
            -77.0364,38.8951,1600,PENNSYLVANIA AVE NW,,WASHINGTON,,DC,20500,,\n";

        // Build a zip archive containing a single CSV
        let file = std::fs::File::create(&zip_path).unwrap();
        let mut zip_writer = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        zip_writer.start_file("us/addresses.csv", options).unwrap();
        zip_writer.write_all(csv_data).unwrap();
        zip_writer.finish().unwrap();

        let mut results = Vec::new();
        let count = parse_zip_archive(&zip_path, |addr| results.push(addr)).unwrap();

        assert_eq!(count, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].street, "100 NORTH STATE STREET");
        assert_eq!(results[0].city, "CHICAGO");
        assert_eq!(results[1].city, "WASHINGTON");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn parses_tar_zst_archive() {
        let tmp = std::env::temp_dir().join("oa_tar_zst_test");
        let _ = std::fs::remove_dir_all(&tmp);
        std::fs::create_dir_all(&tmp).unwrap();

        let csv_data = b"LON,LAT,NUMBER,STREET,UNIT,CITY,DISTRICT,REGION,POSTCODE,ID,HASH\n\
            -87.6278,41.8827,200,W MADISON ST,,CHICAGO,,IL,60606,,\n";

        // Build a tar.zst archive containing a single CSV
        let archive_path = tmp.join("test.tar.zst");
        let file = std::fs::File::create(&archive_path).unwrap();
        let encoder = zstd::Encoder::new(file, 1).unwrap();
        let mut tar_builder = tar::Builder::new(encoder);

        let mut header = tar::Header::new_gnu();
        header.set_size(csv_data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder
            .append_data(&mut header, "us/test.csv", &csv_data[..])
            .unwrap();

        let encoder = tar_builder.into_inner().unwrap();
        encoder.finish().unwrap();

        let mut results = Vec::new();
        let count = parse_tar_zst_archive(&archive_path, |addr| results.push(addr)).unwrap();

        assert_eq!(count, 1);
        assert_eq!(results[0].street, "200 WEST MADISON STREET");
        assert_eq!(results[0].city, "CHICAGO");
        assert_eq!(results[0].state, "IL");

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
