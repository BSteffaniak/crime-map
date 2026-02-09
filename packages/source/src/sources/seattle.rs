//! Seattle Police Department crime data source.
//!
//! Uses Seattle's Socrata Open Data API.
//! Dataset: <https://data.seattle.gov/resource/tazs-3rd5>

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use crime_map_source_models::NormalizedIncident;
use serde::Deserialize;

use crate::parsing::{parse_lat_lng_str, parse_socrata_date};
use crate::socrata::{SocrataConfig, fetch_socrata};
use crate::type_mapping::map_crime_type;
use crate::{CrimeSource, FetchOptions, SourceError};

const API_URL: &str = "https://data.seattle.gov/resource/tazs-3rd5.json";

/// Seattle PD crime data source.
pub struct SeattleSource;

impl SeattleSource {
    /// Creates a new Seattle data source.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for SeattleSource {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Record {
    #[serde(default)]
    report_number: Option<String>,
    #[serde(default)]
    offense_date: Option<String>,
    #[serde(default)]
    report_date_time: Option<String>,
    #[serde(default)]
    offense_sub_category: Option<String>,
    #[serde(default)]
    offense_category: Option<String>,
    #[serde(default)]
    nibrs_offense_code_description: Option<String>,
    #[serde(default, rename = "_100_block_address")]
    block_address: Option<String>,
    #[serde(default)]
    neighborhood: Option<String>,
    #[serde(default)]
    latitude: Option<String>,
    #[serde(default)]
    longitude: Option<String>,
}

#[async_trait]
impl CrimeSource for SeattleSource {
    fn id(&self) -> &'static str {
        "seattle_pd"
    }

    fn name(&self) -> &'static str {
        "Seattle Police Department"
    }

    async fn fetch(&self, options: &FetchOptions) -> Result<PathBuf, SourceError> {
        fetch_socrata(
            &SocrataConfig {
                api_url: API_URL,
                date_column: "offense_date",
                output_filename: "seattle_crimes.json",
                label: "Seattle",
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

            let source_incident_id = match &record.report_number {
                Some(id) if !id.is_empty() => id.clone(),
                _ => continue,
            };

            // Use the most specific crime type available
            let crime_str = record
                .offense_sub_category
                .as_deref()
                .or(record.nibrs_offense_code_description.as_deref())
                .or(record.offense_category.as_deref())
                .unwrap_or_default();
            let subcategory = map_crime_type(crime_str);

            let occurred_at = record
                .offense_date
                .as_deref()
                .and_then(parse_socrata_date)
                .unwrap_or_else(chrono::Utc::now);

            let reported_at = record
                .report_date_time
                .as_deref()
                .and_then(parse_socrata_date);

            incidents.push(NormalizedIncident {
                source_incident_id,
                subcategory,
                longitude,
                latitude,
                occurred_at,
                reported_at,
                description: record.nibrs_offense_code_description,
                block_address: record.block_address,
                city: "Seattle".to_string(),
                state: "WA".to_string(),
                arrest_made: None,
                domestic: None,
                location_type: record.neighborhood,
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
