//! Chicago Police Department crime data source.
//!
//! Uses the City of Chicago's Socrata Open Data API to fetch crime incidents.
//! Dataset: <https://data.cityofchicago.org/resource/ijzp-q8t2>

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use crime_map_source_models::NormalizedIncident;
use serde::Deserialize;

use crate::type_mapping::map_crime_type;
use crate::{CrimeSource, FetchOptions, SourceError};

/// Socrata API endpoint for Chicago crime data.
const CHICAGO_API_URL: &str = "https://data.cityofchicago.org/resource/ijzp-q8t2.json";

/// Batch size for paginated API requests.
const PAGE_SIZE: u64 = 50_000;

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
struct ChicagoRecord {
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
        let output_path = options.output_dir.join("chicago_crimes.json");
        std::fs::create_dir_all(&options.output_dir)?;

        let client = reqwest::Client::new();
        let mut all_records: Vec<serde_json::Value> = Vec::new();
        let mut offset: u64 = 0;
        let fetch_limit = options.limit.unwrap_or(u64::MAX);

        loop {
            let remaining = fetch_limit.saturating_sub(offset);
            if remaining == 0 {
                break;
            }
            let page_limit = remaining.min(PAGE_SIZE);

            let mut url =
                format!("{CHICAGO_API_URL}?$limit={page_limit}&$offset={offset}&$order=date DESC");

            if let Some(since) = &options.since {
                let since_str = since.format("%Y-%m-%dT%H:%M:%S").to_string();
                url.push_str(&format!("&$where=date > '{since_str}'"));
            }

            log::info!("Fetching Chicago data: offset={offset}, limit={page_limit}");
            let response = client.get(&url).send().await?;
            let records: Vec<serde_json::Value> = response.json().await?;

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

        log::info!("Downloaded {} Chicago records total", all_records.len());
        let json = serde_json::to_string(&all_records)?;
        std::fs::write(&output_path, json)?;

        Ok(output_path)
    }

    async fn normalize(&self, raw_path: &Path) -> Result<Vec<NormalizedIncident>, SourceError> {
        let data = std::fs::read_to_string(raw_path)?;
        let records: Vec<ChicagoRecord> = serde_json::from_str(&data)?;

        let mut incidents = Vec::with_capacity(records.len());

        for record in records {
            let Some(lat_str) = &record.latitude else {
                continue;
            };
            let Some(lng_str) = &record.longitude else {
                continue;
            };

            let Ok(latitude) = lat_str.parse::<f64>() else {
                continue;
            };
            let Ok(longitude) = lng_str.parse::<f64>() else {
                continue;
            };

            if latitude == 0.0 || longitude == 0.0 {
                continue;
            }

            let source_incident_id = record.case_number.or(record.id).unwrap_or_default();

            if source_incident_id.is_empty() {
                continue;
            }

            let primary_type = record.primary_type.unwrap_or_default();
            let subcategory = map_crime_type(&primary_type);

            let occurred_at = record
                .date
                .as_deref()
                .and_then(parse_chicago_date)
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
            data.len()
        );

        Ok(incidents)
    }
}

/// Parses the Chicago date format: `"2024-01-15T10:30:00.000"`.
fn parse_chicago_date(s: &str) -> Option<DateTime<Utc>> {
    // Try the standard format with fractional seconds
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(naive.and_utc());
    }
    // Try without fractional seconds
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(naive.and_utc());
    }
    None
}
