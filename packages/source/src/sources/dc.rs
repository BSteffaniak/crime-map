//! Washington DC Metropolitan Police Department crime data source.
//!
//! Uses DC's `ArcGIS` REST API.
//! Endpoint: <https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer>

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use crime_map_source_models::NormalizedIncident;
use serde::Deserialize;

use crate::arcgis::{ArcGisConfig, fetch_arcgis};
use crate::type_mapping::map_crime_type;
use crate::{CrimeSource, FetchOptions, SourceError};

const QUERY_URL: &str =
    "https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/8/query";

/// DC Metropolitan Police Department crime data source.
pub struct DcSource;

impl DcSource {
    /// Creates a new DC data source.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for DcSource {
    fn default() -> Self {
        Self::new()
    }
}

/// Raw record from the DC `ArcGIS` API (flattened attributes).
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Record {
    #[serde(default, alias = "CCN")]
    ccn: Option<String>,
    #[serde(default, alias = "REPORT_DAT")]
    report_dat: Option<f64>,
    #[serde(default, alias = "OFFENSE")]
    offense: Option<String>,
    #[serde(default, alias = "METHOD")]
    method: Option<String>,
    #[serde(default, alias = "BLOCK")]
    block: Option<String>,
    #[serde(default, alias = "WARD")]
    ward: Option<String>,
    #[serde(default, alias = "NEIGHBORHOOD_CLUSTER")]
    neighborhood_cluster: Option<String>,
    #[serde(default, alias = "LATITUDE")]
    latitude: Option<f64>,
    #[serde(default, alias = "LONGITUDE")]
    longitude: Option<f64>,
}

/// Parses an epoch-millisecond timestamp (common in `ArcGIS`) to a UTC datetime.
fn parse_epoch_ms(ms: f64) -> Option<DateTime<Utc>> {
    #[allow(clippy::cast_possible_truncation)]
    let secs = (ms / 1000.0) as i64;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let nsecs = ((ms % 1000.0) * 1_000_000.0) as u32;
    DateTime::from_timestamp(secs, nsecs)
}

#[async_trait]
impl CrimeSource for DcSource {
    fn id(&self) -> &'static str {
        "dc_mpd"
    }

    fn name(&self) -> &'static str {
        "DC Metropolitan Police Department"
    }

    async fn fetch(&self, options: &FetchOptions) -> Result<PathBuf, SourceError> {
        fetch_arcgis(
            &ArcGisConfig {
                query_url: QUERY_URL,
                output_filename: "dc_crimes.json",
                label: "DC",
                page_size: 2000,
                where_clause: None,
            },
            options,
        )
        .await
    }

    async fn normalize(&self, raw_path: &Path) -> Result<Vec<NormalizedIncident>, SourceError> {
        let data = std::fs::read_to_string(raw_path)?;
        let records: Vec<Record> = serde_json::from_str(&data)?;
        let raw_count = records.len();
        let mut incidents = Vec::with_capacity(raw_count);

        for record in records {
            let latitude = match record.latitude {
                Some(v) if v != 0.0 => v,
                _ => continue,
            };
            let longitude = match record.longitude {
                Some(v) if v != 0.0 => v,
                _ => continue,
            };

            let source_incident_id = match &record.ccn {
                Some(id) if !id.is_empty() => id.clone(),
                _ => continue,
            };

            let offense = record.offense.unwrap_or_default();
            let subcategory = map_crime_type(&offense);

            let occurred_at = record
                .report_dat
                .and_then(parse_epoch_ms)
                .unwrap_or_else(Utc::now);

            let description = if let Some(method) = &record.method {
                Some(format!("{offense} ({method})"))
            } else {
                Some(offense)
            };

            incidents.push(NormalizedIncident {
                source_incident_id,
                subcategory,
                longitude,
                latitude,
                occurred_at,
                reported_at: None,
                description,
                block_address: record.block,
                city: "Washington".to_string(),
                state: "DC".to_string(),
                arrest_made: None,
                domestic: None,
                location_type: record.ward,
            });
        }

        log::info!(
            "Normalized {} incidents from {} raw records",
            incidents.len(),
            raw_count
        );
        Ok(incidents)
    }
}
