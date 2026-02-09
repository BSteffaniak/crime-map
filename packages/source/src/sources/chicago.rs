//! Chicago Police Department crime data source.
//!
//! Uses the City of Chicago's Socrata Open Data API to fetch crime incidents.
//! Dataset: <https://data.cityofchicago.org/resource/ijzp-q8t2>

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use crime_map_source_models::NormalizedIncident;
use serde::Deserialize;

use crate::socrata::{SocrataConfig, fetch_socrata};
use crate::type_mapping::map_crime_type;
use crate::{CrimeSource, FetchOptions, SourceError};

const API_URL: &str = "https://data.cityofchicago.org/resource/ijzp-q8t2.json";

/// Chicago PD crime data source.
pub struct ChicagoSource;

impl ChicagoSource {
    /// Creates a new Chicago data source.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for ChicagoSource {
    fn default() -> Self {
        Self::new()
    }
}

/// Raw record shape from the Chicago Socrata API.
#[derive(Debug, Deserialize)]
struct Record {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    case_number: Option<String>,
    #[serde(default)]
    date: Option<String>,
    #[serde(default)]
    block: Option<String>,
    #[serde(default)]
    primary_type: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    location_description: Option<String>,
    #[serde(default)]
    arrest: Option<bool>,
    #[serde(default)]
    domestic: Option<bool>,
    #[serde(default)]
    latitude: Option<String>,
    #[serde(default)]
    longitude: Option<String>,
}

#[async_trait]
impl CrimeSource for ChicagoSource {
    fn id(&self) -> &'static str {
        "chicago_pd"
    }

    fn name(&self) -> &'static str {
        "Chicago Police Department"
    }

    async fn fetch(&self, options: &FetchOptions) -> Result<PathBuf, SourceError> {
        fetch_socrata(
            &SocrataConfig {
                api_url: API_URL,
                date_column: "date",
                output_filename: "chicago_crimes.json",
                label: "Chicago",
                page_size: 50_000,
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
            let Some((latitude, longitude)) =
                parse_lat_lng_str(record.latitude.as_ref(), record.longitude.as_ref())
            else {
                continue;
            };

            let source_incident_id = record.case_number.or(record.id).unwrap_or_default();
            if source_incident_id.is_empty() {
                continue;
            }

            let primary_type = record.primary_type.unwrap_or_default();
            let subcategory = map_crime_type(&primary_type);

            let occurred_at = record
                .date
                .as_deref()
                .and_then(parse_socrata_date)
                .unwrap_or_else(Utc::now);

            let description_text = record.description.unwrap_or_default();
            let full_description = if description_text.is_empty() {
                primary_type.clone()
            } else {
                format!("{primary_type}: {description_text}")
            };

            incidents.push(NormalizedIncident {
                source_incident_id,
                subcategory,
                longitude,
                latitude,
                occurred_at,
                reported_at: None,
                description: Some(full_description),
                block_address: record.block,
                city: "Chicago".to_string(),
                state: "IL".to_string(),
                arrest_made: record.arrest,
                domestic: record.domestic,
                location_type: record.location_description,
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

/// Parses a Socrata datetime string (ISO 8601 with optional fractional seconds).
pub(crate) fn parse_socrata_date(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(naive.and_utc());
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(naive.and_utc());
    }
    None
}

/// Parses lat/lng from optional string fields. Returns `None` if missing,
/// unparseable, or zero.
pub(crate) fn parse_lat_lng_str(lat: Option<&String>, lng: Option<&String>) -> Option<(f64, f64)> {
    let lat_str = lat?.as_str();
    let lng_str = lng?.as_str();
    let latitude = lat_str.parse::<f64>().ok()?;
    let longitude = lng_str.parse::<f64>().ok()?;
    if latitude == 0.0 || longitude == 0.0 {
        return None;
    }
    Some((latitude, longitude))
}

/// Parses lat/lng from optional f64 fields.
pub(crate) fn parse_lat_lng_f64(lat: Option<f64>, lng: Option<f64>) -> Option<(f64, f64)> {
    let latitude = lat?;
    let longitude = lng?;
    if latitude == 0.0 || longitude == 0.0 {
        return None;
    }
    Some((latitude, longitude))
}
