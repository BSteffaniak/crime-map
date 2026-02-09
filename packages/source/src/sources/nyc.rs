//! New York City Police Department crime data source.
//!
//! Uses NYC's Socrata Open Data API (NYPD Complaint Data Current).
//! Dataset: <https://data.cityofnewyork.us/resource/5uac-w243>

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use chrono::{NaiveDateTime, NaiveTime, Utc};
use crime_map_source_models::NormalizedIncident;
use serde::Deserialize;

use crate::socrata::{SocrataConfig, fetch_socrata};
use crate::sources::chicago::{parse_lat_lng_str, parse_socrata_date};
use crate::type_mapping::map_crime_type;
use crate::{CrimeSource, FetchOptions, SourceError};

const API_URL: &str = "https://data.cityofnewyork.us/resource/5uac-w243.json";

/// NYC Police Department crime data source.
pub struct NycSource;

impl NycSource {
    /// Creates a new NYC data source.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for NycSource {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Record {
    #[serde(default)]
    cmplnt_num: Option<String>,
    #[serde(default)]
    cmplnt_fr_dt: Option<String>,
    #[serde(default)]
    cmplnt_fr_tm: Option<String>,
    #[serde(default)]
    rpt_dt: Option<String>,
    #[serde(default)]
    ofns_desc: Option<String>,
    #[serde(default)]
    pd_desc: Option<String>,
    #[serde(default)]
    law_cat_cd: Option<String>,
    #[serde(default)]
    boro_nm: Option<String>,
    #[serde(default)]
    loc_of_occur_desc: Option<String>,
    #[serde(default)]
    prem_typ_desc: Option<String>,
    #[serde(default)]
    latitude: Option<String>,
    #[serde(default)]
    longitude: Option<String>,
}

#[async_trait]
impl CrimeSource for NycSource {
    fn id(&self) -> &'static str {
        "nyc_pd"
    }

    fn name(&self) -> &'static str {
        "New York City Police Department"
    }

    async fn fetch(&self, options: &FetchOptions) -> Result<PathBuf, SourceError> {
        fetch_socrata(
            &SocrataConfig {
                api_url: API_URL,
                date_column: "cmplnt_fr_dt",
                output_filename: "nyc_crimes.json",
                label: "NYC",
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

            let source_incident_id = match &record.cmplnt_num {
                Some(id) if !id.is_empty() => id.clone(),
                _ => continue,
            };

            let offense_desc = record.ofns_desc.unwrap_or_default();
            let subcategory = map_crime_type(&offense_desc);

            // Combine cmplnt_fr_dt (date) + cmplnt_fr_tm ("HH:MM:SS")
            let occurred_at = record
                .cmplnt_fr_dt
                .as_deref()
                .and_then(|d| {
                    let parsed = parse_socrata_date(d)?;
                    if let Some(time_str) = &record.cmplnt_fr_tm
                        && let Ok(time) = time_str.parse::<NaiveTime>()
                    {
                        let dt = NaiveDateTime::new(parsed.date_naive(), time);
                        return Some(dt.and_utc());
                    }
                    Some(parsed)
                })
                .unwrap_or_else(Utc::now);

            let reported_at = record.rpt_dt.as_deref().and_then(parse_socrata_date);

            let description = record.pd_desc.or(Some(offense_desc));

            incidents.push(NormalizedIncident {
                source_incident_id,
                subcategory,
                longitude,
                latitude,
                occurred_at,
                reported_at,
                description,
                block_address: record.loc_of_occur_desc,
                city: "New York".to_string(),
                state: "NY".to_string(),
                arrest_made: None,
                domestic: None,
                location_type: record.prem_typ_desc,
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
