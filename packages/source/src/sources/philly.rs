//! Philadelphia Police Department crime data source.
//!
//! Uses Philadelphia's Carto SQL API.
//! Endpoint: <https://phl.carto.com/api/v2/sql>

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use crime_map_source_models::NormalizedIncident;
use serde::Deserialize;

use crate::sources::chicago::parse_socrata_date;
use crate::type_mapping::map_crime_type;
use crate::{CrimeSource, FetchOptions, SourceError};

const CARTO_URL: &str = "https://phl.carto.com/api/v2/sql";

/// Philadelphia PD crime data source.
pub struct PhillySource;

impl PhillySource {
    /// Creates a new Philadelphia data source.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for PhillySource {
    fn default() -> Self {
        Self::new()
    }
}

/// Raw record from the Carto SQL API response.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Record {
    #[serde(default)]
    dc_key: Option<String>,
    #[serde(default)]
    objectid: Option<i64>,
    #[serde(default)]
    dispatch_date_time: Option<String>,
    #[serde(default)]
    text_general_code: Option<String>,
    #[serde(default)]
    dc_dist: Option<String>,
    #[serde(default)]
    location_block: Option<String>,
    #[serde(default)]
    point_x: Option<f64>,
    #[serde(default)]
    point_y: Option<f64>,
}

#[async_trait]
impl CrimeSource for PhillySource {
    fn id(&self) -> &'static str {
        "philly_pd"
    }

    fn name(&self) -> &'static str {
        "Philadelphia Police Department"
    }

    async fn fetch(&self, options: &FetchOptions) -> Result<PathBuf, SourceError> {
        let output_path = options.output_dir.join("philly_crimes.json");
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

            let query = if let Some(since) = &options.since {
                let since_str = since.format("%Y-%m-%d %H:%M:%S").to_string();
                format!(
                    "SELECT * FROM incidents_part1_part2 \
                     WHERE dispatch_date_time > '{since_str}' \
                     ORDER BY dispatch_date_time DESC \
                     LIMIT {page_limit} OFFSET {offset}"
                )
            } else {
                format!(
                    "SELECT * FROM incidents_part1_part2 \
                     ORDER BY dispatch_date_time DESC \
                     LIMIT {page_limit} OFFSET {offset}"
                )
            };

            log::info!("Fetching Philly data: offset={offset}, limit={page_limit}");
            let response = client.get(CARTO_URL).query(&[("q", &query)]).send().await?;
            let body: serde_json::Value = response.json().await?;

            let rows = body
                .get("rows")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();

            let count = rows.len() as u64;
            if count == 0 {
                break;
            }

            all_records.extend(rows);
            offset += count;

            if count < page_limit {
                break;
            }
        }

        log::info!("Downloaded {} Philly records total", all_records.len());
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
            let longitude = match record.point_x {
                Some(v) if v != 0.0 => v,
                _ => continue,
            };
            let latitude = match record.point_y {
                Some(v) if v != 0.0 => v,
                _ => continue,
            };

            // Use dc_key as primary ID, fall back to objectid
            let source_incident_id = match &record.dc_key {
                Some(id) if !id.is_empty() => id.clone(),
                _ => match record.objectid {
                    Some(oid) => oid.to_string(),
                    None => continue,
                },
            };

            let crime_type = record.text_general_code.unwrap_or_default();
            let subcategory = map_crime_type(&crime_type);

            let occurred_at = record
                .dispatch_date_time
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
                description: Some(crime_type),
                block_address: record.location_block,
                city: "Philadelphia".to_string(),
                state: "PA".to_string(),
                arrest_made: None,
                domestic: None,
                location_type: record.dc_dist,
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
