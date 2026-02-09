//! Los Angeles Police Department crime data source.
//!
//! Uses the City of LA's Socrata Open Data API.
//! Dataset: <https://data.lacity.org/resource/2nrs-mtv8>

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{NaiveDateTime, NaiveTime, Utc};
use crime_map_source_models::NormalizedIncident;
use serde::Deserialize;

use crate::parsing::{parse_lat_lng_str, parse_socrata_date};
use crate::socrata::{SocrataConfig, fetch_socrata};
use crate::type_mapping::map_crime_type;
use crate::{CrimeSource, FetchOptions, SourceError};

const API_URL: &str = "https://data.lacity.org/resource/2nrs-mtv8.json";

/// LA Police Department crime data source.
pub struct LaSource;

impl LaSource {
    /// Creates a new LA data source.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for LaSource {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Record {
    #[serde(default)]
    dr_no: Option<String>,
    #[serde(default)]
    date_occ: Option<String>,
    #[serde(default)]
    time_occ: Option<String>,
    #[serde(default)]
    date_rptd: Option<String>,
    #[serde(default)]
    crm_cd_desc: Option<String>,
    #[serde(default)]
    premis_desc: Option<String>,
    #[serde(default)]
    location: Option<String>,
    #[serde(default)]
    area_name: Option<String>,
    #[serde(default)]
    status_desc: Option<String>,
    #[serde(default)]
    lat: Option<String>,
    #[serde(default)]
    lon: Option<String>,
}

#[async_trait]
impl CrimeSource for LaSource {
    fn id(&self) -> &'static str {
        "la_pd"
    }

    fn name(&self) -> &'static str {
        "Los Angeles Police Department"
    }

    async fn fetch(&self, options: &FetchOptions) -> Result<PathBuf, SourceError> {
        fetch_socrata(
            &SocrataConfig {
                api_url: API_URL,
                date_column: "date_occ",
                output_filename: "la_crimes.json",
                label: "LA",
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
                parse_lat_lng_str(record.lat.as_ref(), record.lon.as_ref())
            else {
                continue;
            };

            let source_incident_id = match &record.dr_no {
                Some(id) if !id.is_empty() => id.clone(),
                _ => continue,
            };

            let crime_desc = record.crm_cd_desc.unwrap_or_default();
            let subcategory = map_crime_type(&crime_desc);

            // Combine date_occ (ISO date) + time_occ (HHMM string)
            let occurred_at = record
                .date_occ
                .as_deref()
                .and_then(|d| {
                    let parsed = parse_socrata_date(d)?;
                    if let Some(time_str) = &record.time_occ
                        && time_str.len() == 4
                    {
                        let hour = time_str[..2].parse::<u32>().ok()?;
                        let min = time_str[2..].parse::<u32>().ok()?;
                        let naive_date = parsed.date_naive();
                        let naive_time = NaiveTime::from_hms_opt(hour, min, 0)?;
                        let dt = NaiveDateTime::new(naive_date, naive_time);
                        return Some(dt.and_utc());
                    }
                    Some(parsed)
                })
                .unwrap_or_else(Utc::now);

            let reported_at = record.date_rptd.as_deref().and_then(parse_socrata_date);

            let arrest_made = record
                .status_desc
                .as_ref()
                .map(|s| s.to_lowercase().contains("arrest"));

            incidents.push(NormalizedIncident {
                source_incident_id,
                subcategory,
                longitude,
                latitude,
                occurred_at,
                reported_at,
                description: Some(crime_desc),
                block_address: record.location,
                city: "Los Angeles".to_string(),
                state: "CA".to_string(),
                arrest_made,
                domestic: None,
                location_type: record.premis_desc,
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
