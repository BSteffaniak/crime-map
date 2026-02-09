//! Boston Police Department crime data source.
//!
//! Uses Boston's CKAN Datastore API.
//! Endpoint: <https://data.boston.gov/api/3/action/datastore_search>

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use crime_map_source_models::NormalizedIncident;
use serde::Deserialize;

use crate::parsing::{parse_lat_lng_str, parse_socrata_date};
use crate::type_mapping::map_crime_type;
use crate::{CrimeSource, FetchOptions, SourceError};

/// Resource ID for the current-year crime incident reports.
const RESOURCE_ID: &str = "12cb3883-56f5-47de-afa5-3b1cf61b257b";
const CKAN_URL: &str = "https://data.boston.gov/api/3/action/datastore_search";

/// Boston PD crime data source.
pub struct BostonSource;

impl BostonSource {
    /// Creates a new Boston data source.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for BostonSource {
    fn default() -> Self {
        Self::new()
    }
}

/// Raw record from the CKAN Datastore API.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Record {
    #[serde(default, alias = "INCIDENT_NUMBER")]
    incident_number: Option<String>,
    #[serde(default, alias = "OCCURRED_ON_DATE")]
    occurred_on_date: Option<String>,
    #[serde(default, alias = "OFFENSE_DESCRIPTION")]
    offense_description: Option<String>,
    #[serde(default, alias = "OFFENSE_CODE_GROUP")]
    offense_code_group: Option<String>,
    #[serde(default, alias = "DISTRICT")]
    district: Option<String>,
    #[serde(default, alias = "STREET")]
    street: Option<String>,
    #[serde(default, alias = "Lat")]
    lat: Option<String>,
    #[serde(default, alias = "Long")]
    long: Option<String>,
}

#[async_trait]
impl CrimeSource for BostonSource {
    fn id(&self) -> &'static str {
        "boston_pd"
    }

    fn name(&self) -> &'static str {
        "Boston Police Department"
    }

    async fn fetch(&self, options: &FetchOptions) -> Result<PathBuf, SourceError> {
        let output_path = options.output_dir.join("boston_crimes.json");
        std::fs::create_dir_all(&options.output_dir)?;

        let client = reqwest::Client::new();
        let mut all_records: Vec<serde_json::Value> = Vec::new();
        let mut offset: u64 = 0;
        let page_size: u64 = 10_000;
        let fetch_limit = options.limit.unwrap_or(u64::MAX);

        loop {
            let remaining = fetch_limit.saturating_sub(offset);
            if remaining == 0 {
                break;
            }
            let page_limit = remaining.min(page_size);

            log::info!("Fetching Boston data: offset={offset}, limit={page_limit}");

            let response = client
                .get(CKAN_URL)
                .query(&[
                    ("resource_id", RESOURCE_ID),
                    ("limit", &page_limit.to_string()),
                    ("offset", &offset.to_string()),
                ])
                .send()
                .await?;
            let body: serde_json::Value = response.json().await?;

            let records = body
                .get("result")
                .and_then(|r| r.get("records"))
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();

            let count = records.len() as u64;
            if count == 0 {
                break;
            }

            all_records.extend(records);
            offset += count;

            if count < page_limit {
                break;
            }
        }

        log::info!("Downloaded {} Boston records total", all_records.len());
        let json = serde_json::to_string(&all_records)?;
        std::fs::write(&output_path, json)?;

        Ok(output_path)
    }

    async fn normalize(&self, raw_path: &Path) -> Result<Vec<NormalizedIncident>, SourceError> {
        let data = std::fs::read_to_string(raw_path)?;
        let records: Vec<Record> = serde_json::from_str(&data)?;
        let raw_count = records.len();
        let mut incidents = Vec::with_capacity(raw_count);

        for record in records {
            let Some((latitude, longitude)) =
                parse_lat_lng_str(record.lat.as_ref(), record.long.as_ref())
            else {
                continue;
            };

            let source_incident_id = match &record.incident_number {
                Some(id) if !id.is_empty() => id.clone(),
                _ => continue,
            };

            let offense = record.offense_description.unwrap_or_default();
            let subcategory = map_crime_type(&offense);

            let occurred_at = record
                .occurred_on_date
                .as_deref()
                .and_then(parse_socrata_date)
                .unwrap_or_else(chrono::Utc::now);

            incidents.push(NormalizedIncident {
                source_incident_id,
                subcategory,
                longitude,
                latitude,
                occurred_at,
                reported_at: None,
                description: Some(offense),
                block_address: record.street,
                city: "Boston".to_string(),
                state: "MA".to_string(),
                arrest_made: None,
                domestic: None,
                location_type: record.district,
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
