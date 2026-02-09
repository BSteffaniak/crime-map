//! Denver Police Department crime data source.
//!
//! Uses Denver's Socrata Open Data API.
//! Dataset: <https://data.denvergov.org/resource/j6g8-fkyh>

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use crime_map_source_models::NormalizedIncident;
use serde::Deserialize;

use crate::parsing::{parse_lat_lng_f64, parse_socrata_date};
use crate::socrata::{SocrataConfig, fetch_socrata};
use crate::type_mapping::map_crime_type;
use crate::{CrimeSource, FetchOptions, SourceError};

const API_URL: &str = "https://data.denvergov.org/resource/j6g8-fkyh.json";

/// Denver PD crime data source.
pub struct DenverSource;

impl DenverSource {
    /// Creates a new Denver data source.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for DenverSource {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Record {
    #[serde(default)]
    incident_id: Option<String>,
    #[serde(default)]
    first_occurrence_date: Option<String>,
    #[serde(default)]
    last_occurrence_date: Option<String>,
    #[serde(default)]
    reported_date: Option<String>,
    #[serde(default)]
    offense_type_id: Option<String>,
    #[serde(default)]
    offense_category_id: Option<String>,
    #[serde(default)]
    incident_address: Option<String>,
    #[serde(default)]
    neighborhood_id: Option<String>,
    #[serde(default)]
    geo_lat: Option<f64>,
    #[serde(default)]
    geo_lon: Option<f64>,
    #[serde(default)]
    is_crime: Option<String>,
    #[serde(default)]
    is_traffic: Option<String>,
}

#[async_trait]
impl CrimeSource for DenverSource {
    fn id(&self) -> &'static str {
        "denver_pd"
    }

    fn name(&self) -> &'static str {
        "Denver Police Department"
    }

    async fn fetch(&self, options: &FetchOptions) -> Result<PathBuf, SourceError> {
        fetch_socrata(
            &SocrataConfig {
                api_url: API_URL,
                date_column: "first_occurrence_date",
                output_filename: "denver_crimes.json",
                label: "Denver",
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
            let Some((latitude, longitude)) = parse_lat_lng_f64(record.geo_lat, record.geo_lon)
            else {
                continue;
            };

            let source_incident_id = match &record.incident_id {
                Some(id) if !id.is_empty() => id.clone(),
                _ => continue,
            };

            let offense_type = record.offense_type_id.unwrap_or_default();
            let subcategory = map_crime_type(&offense_type);

            let occurred_at = record
                .first_occurrence_date
                .as_deref()
                .and_then(parse_socrata_date)
                .unwrap_or_else(chrono::Utc::now);

            let reported_at = record.reported_date.as_deref().and_then(parse_socrata_date);

            incidents.push(NormalizedIncident {
                source_incident_id,
                subcategory,
                longitude,
                latitude,
                occurred_at,
                reported_at,
                description: Some(offense_type),
                block_address: record.incident_address,
                city: "Denver".to_string(),
                state: "CO".to_string(),
                arrest_made: None,
                domestic: None,
                location_type: record.neighborhood_id,
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
