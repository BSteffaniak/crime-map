//! San Francisco Police Department crime data source.
//!
//! Uses SF's Socrata Open Data API.
//! Dataset: <https://data.sfgov.org/resource/wg3w-h783>

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use crime_map_source_models::NormalizedIncident;
use serde::Deserialize;

use crate::socrata::{SocrataConfig, fetch_socrata};
use crate::sources::chicago::{parse_lat_lng_str, parse_socrata_date};
use crate::type_mapping::map_crime_type;
use crate::{CrimeSource, FetchOptions, SourceError};

const API_URL: &str = "https://data.sfgov.org/resource/wg3w-h783.json";

/// SF Police Department crime data source.
pub struct SfSource;

impl SfSource {
    /// Creates a new SF data source.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for SfSource {
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
    incident_datetime: Option<String>,
    #[serde(default)]
    incident_category: Option<String>,
    #[serde(default)]
    incident_subcategory: Option<String>,
    #[serde(default)]
    incident_description: Option<String>,
    #[serde(default)]
    resolution: Option<String>,
    #[serde(default)]
    intersection: Option<String>,
    #[serde(default)]
    analysis_neighborhood: Option<String>,
    #[serde(default)]
    latitude: Option<String>,
    #[serde(default)]
    longitude: Option<String>,
}

#[async_trait]
impl CrimeSource for SfSource {
    fn id(&self) -> &'static str {
        "sf_pd"
    }

    fn name(&self) -> &'static str {
        "San Francisco Police Department"
    }

    async fn fetch(&self, options: &FetchOptions) -> Result<PathBuf, SourceError> {
        fetch_socrata(
            &SocrataConfig {
                api_url: API_URL,
                date_column: "incident_datetime",
                output_filename: "sf_crimes.json",
                label: "SF",
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

            let source_incident_id = match &record.incident_id {
                Some(id) if !id.is_empty() => id.clone(),
                _ => continue,
            };

            // SF has good category names — use incident_category for mapping
            let category_str = record.incident_category.unwrap_or_default();
            let subcategory = map_crime_type(&category_str);

            let occurred_at = record
                .incident_datetime
                .as_deref()
                .and_then(parse_socrata_date)
                .unwrap_or_else(chrono::Utc::now);

            let arrest_made = record
                .resolution
                .as_ref()
                .map(|s| s.to_lowercase().contains("arrest"));

            incidents.push(NormalizedIncident {
                source_incident_id,
                subcategory,
                longitude,
                latitude,
                occurred_at,
                reported_at: None,
                description: record.incident_description,
                block_address: record.intersection,
                city: "San Francisco".to_string(),
                state: "CA".to_string(),
                arrest_made,
                domestic: None,
                location_type: record.analysis_neighborhood,
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
