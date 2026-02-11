//! Config-driven crime data source definition.
//!
//! [`SourceDefinition`] captures everything unique about a data source in a
//! serializable config struct. A single generic implementation handles all
//! sources, eliminating per-city boilerplate.
//!
//! Pages of raw records are streamed through a [`tokio::sync::mpsc`] channel
//! so that normalization and database insertion happen incrementally rather
//! than buffering the entire dataset in memory.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use crime_map_source_models::NormalizedIncident;
use serde::Deserialize;
use tokio::sync::mpsc;

use crate::arcgis::{ArcGisConfig, fetch_arcgis};
use crate::carto::{CartoConfig, fetch_carto};
use crate::ckan::{CkanConfig, fetch_ckan};
use crate::odata::{ODataConfig, fetch_odata};
use crate::parsing::parse_socrata_date;
use crate::socrata::{SocrataConfig, fetch_socrata};
use crate::type_mapping::map_crime_type;
use crate::{FetchOptions, SourceError};

// ── Top-level source definition ──────────────────────────────────────────

/// A complete, config-driven crime data source definition.
///
/// Loaded from TOML files at compile time and used as the sole source
/// implementation.
#[derive(Debug, Deserialize)]
pub struct SourceDefinition {
    /// Unique identifier (e.g., `"chicago_pd"`).
    pub id: String,
    /// Human-readable name (e.g., `"Chicago Police Department"`).
    pub name: String,
    /// City name for the `NormalizedIncident`.
    pub city: String,
    /// Two-letter state abbreviation.
    pub state: String,
    /// Legacy output filename (kept for config compatibility).
    pub output_filename: String,
    /// How to fetch raw data from the API.
    pub fetcher: FetcherConfig,
    /// Field name mappings for normalization.
    pub fields: FieldMapping,
}

// ── Fetcher config ───────────────────────────────────────────────────────

/// How to fetch raw data from the source API.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FetcherConfig {
    /// Socrata SODA API (`$limit/$offset/$order/$where`).
    Socrata {
        /// Base Socrata API URL.
        api_url: String,
        /// Date column for ordering and filtering.
        date_column: String,
        /// Records per page.
        page_size: u64,
    },
    /// `ArcGIS` REST API (`resultOffset`/`resultRecordCount`).
    Arcgis {
        /// Query URLs (one per layer/year).
        query_urls: Vec<String>,
        /// Records per page.
        page_size: u64,
        /// Optional WHERE clause.
        where_clause: Option<String>,
        /// Date column for incremental `since` filtering (epoch-ms field).
        date_column: Option<String>,
    },
    /// CKAN Datastore API (`limit`/`offset`).
    Ckan {
        /// Base API URL.
        api_url: String,
        /// CKAN resource IDs (one per dataset/year).
        resource_ids: Vec<String>,
        /// Records per page.
        page_size: u64,
        /// Date column for incremental `since` filtering.
        date_column: Option<String>,
    },
    /// Carto SQL API (SQL `LIMIT`/`OFFSET`).
    Carto {
        /// Base Carto SQL API URL.
        api_url: String,
        /// Table name to query.
        table_name: String,
        /// Date column for ordering and filtering.
        date_column: String,
        /// Records per page.
        page_size: u64,
    },
    /// OData-style REST API (`$top`/`$skip`/`$orderby`).
    Odata {
        /// Base API URL (response is a bare JSON array).
        api_url: String,
        /// Date column for ordering and `$filter`.
        date_column: String,
        /// Records per page.
        page_size: u64,
    },
}

// ── Field mapping ────────────────────────────────────────────────────────

/// Maps source-specific JSON field names to canonical incident fields.
#[derive(Debug, Deserialize)]
pub struct FieldMapping {
    /// JSON field names for the incident ID, tried in order.
    pub incident_id: Vec<String>,
    /// JSON field names for the crime type, tried in order (first non-empty
    /// wins).
    pub crime_type: Vec<String>,
    /// How to extract the `occurred_at` timestamp.
    pub occurred_at: DateExtractor,
    /// Optional field name for `reported_at` (parsed as Socrata datetime).
    pub reported_at: Option<String>,
    /// Latitude coordinate field.
    pub lat: CoordField,
    /// Longitude coordinate field.
    pub lng: CoordField,
    /// How to build the description string.
    pub description: DescriptionExtractor,
    /// Optional field name for block address.
    pub block_address: Option<String>,
    /// Optional field name for location type.
    pub location_type: Option<String>,
    /// How to extract the arrest flag.
    #[serde(default)]
    pub arrest: ArrestExtractor,
    /// Optional domestic violence flag field (direct bool).
    pub domestic: Option<String>,
}

// ── Strategy enums ───────────────────────────────────────────────────────

/// How to extract the `occurred_at` timestamp from a raw record.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DateExtractor {
    /// Single Socrata datetime field.
    Simple {
        /// JSON field name.
        field: String,
    },
    /// Date field + 4-character HHMM time string.
    DatePlusHhmm {
        /// JSON field for the date portion.
        date_field: String,
        /// JSON field for the HHMM time string.
        time_field: String,
    },
    /// Date field + `"HH:MM:SS"` time string.
    DatePlusHhmmss {
        /// JSON field for the date portion.
        date_field: String,
        /// JSON field for the time string.
        time_field: String,
    },
    /// Epoch milliseconds (f64).
    EpochMs {
        /// JSON field name.
        field: String,
    },
    /// `MM/DD/YYYY` text date (no time component).
    MdyDate {
        /// JSON field name.
        field: String,
    },
}

/// A coordinate field and its type.
#[derive(Debug, Deserialize)]
pub struct CoordField {
    /// JSON field name.
    pub field: String,
    /// Whether the field is a string or f64.
    #[serde(rename = "type")]
    pub coord_type: CoordType,
}

/// Whether a coordinate is stored as a string or float in the API response.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordType {
    /// Coordinate is a JSON string that must be parsed to f64.
    String,
    /// Coordinate is a JSON number (f64).
    F64,
    /// Extract latitude from a `GeoJSON` Point or Socrata location object.
    ///
    /// `GeoJSON`: `{"type":"Point","coordinates":[-96.88,32.71]}` → returns `32.71`
    /// Socrata: `{"latitude":"32.71","longitude":"-96.88"}` → returns `32.71`
    PointLat,
    /// Extract longitude from a `GeoJSON` Point or Socrata location object.
    ///
    /// `GeoJSON`: `{"type":"Point","coordinates":[-96.88,32.71]}` → returns `-96.88`
    /// Socrata: `{"latitude":"32.71","longitude":"-96.88"}` → returns `-96.88`
    PointLng,
}

/// How to build the description string.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DescriptionExtractor {
    /// Use a single field directly.
    Single {
        /// JSON field name.
        field: String,
    },
    /// Combine multiple fields with a separator (skip empty fields).
    Combine {
        /// JSON field names to combine.
        fields: Vec<String>,
        /// Separator between non-empty values.
        separator: String,
    },
    /// Try fields in order, use the first non-empty value.
    FallbackChain {
        /// JSON field names, tried in order.
        fields: Vec<String>,
    },
}

/// How to extract the `arrest_made` flag.
#[derive(Debug, Default, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ArrestExtractor {
    /// No arrest information available.
    #[default]
    None,
    /// Direct boolean field.
    DirectBool {
        /// JSON field name.
        field: String,
    },
    /// String field checked for a configurable substring (case-insensitive).
    StringContains {
        /// JSON field name.
        field: String,
        /// Substring to search for (case-insensitive).
        contains: String,
    },
}

// ── Helper methods on extractors ─────────────────────────────────────────

/// Gets a string value from a JSON object by field name.
fn get_str<'a>(record: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    record.get(field)?.as_str()
}

/// Gets an f64 value from a JSON object by field name.
fn get_f64(record: &serde_json::Value, field: &str) -> Option<f64> {
    record.get(field)?.as_f64()
}

/// Gets a bool value from a JSON object by field name.
fn get_bool(record: &serde_json::Value, field: &str) -> Option<bool> {
    record.get(field)?.as_bool()
}

impl CoordField {
    /// Extracts a coordinate value from a JSON record.
    fn extract(&self, record: &serde_json::Value) -> Option<f64> {
        match self.coord_type {
            CoordType::String => {
                let s = get_str(record, &self.field)?;
                s.parse::<f64>().ok()
            }
            CoordType::F64 => get_f64(record, &self.field),
            CoordType::PointLat => {
                let obj = record.get(&self.field)?;
                // GeoJSON Point: {"type":"Point","coordinates":[lng, lat]}
                if let Some(coords) = obj.get("coordinates").and_then(|c| c.as_array()) {
                    return coords.get(1)?.as_f64();
                }
                // Socrata location: {"latitude":"32.71","longitude":"-96.88"}
                obj.get("latitude").and_then(|v| {
                    v.as_str()
                        .and_then(|s| s.parse().ok())
                        .or_else(|| v.as_f64())
                })
            }
            CoordType::PointLng => {
                let obj = record.get(&self.field)?;
                // GeoJSON Point: {"type":"Point","coordinates":[lng, lat]}
                if let Some(coords) = obj.get("coordinates").and_then(|c| c.as_array()) {
                    return coords.first()?.as_f64();
                }
                // Socrata location: {"latitude":"32.71","longitude":"-96.88"}
                obj.get("longitude").and_then(|v| {
                    v.as_str()
                        .and_then(|s| s.parse().ok())
                        .or_else(|| v.as_f64())
                })
            }
        }
    }
}

impl DateExtractor {
    /// Extracts a datetime from a JSON record.
    fn extract(&self, record: &serde_json::Value) -> Option<DateTime<Utc>> {
        match self {
            Self::Simple { field } => {
                let s = get_str(record, field)?;
                parse_socrata_date(s)
            }
            Self::DatePlusHhmm {
                date_field,
                time_field,
            } => {
                let date_str = get_str(record, date_field)?;
                let parsed = parse_socrata_date(date_str)?;
                if let Some(time_str) = get_str(record, time_field)
                    && time_str.len() == 4
                {
                    let hour = time_str[..2].parse::<u32>().ok()?;
                    let min = time_str[2..].parse::<u32>().ok()?;
                    let time = NaiveTime::from_hms_opt(hour, min, 0)?;
                    let dt = NaiveDateTime::new(parsed.date_naive(), time);
                    return Some(dt.and_utc());
                }
                Some(parsed)
            }
            Self::DatePlusHhmmss {
                date_field,
                time_field,
            } => {
                let date_str = get_str(record, date_field)?;
                let parsed = parse_socrata_date(date_str)?;
                if let Some(time_str) = get_str(record, time_field)
                    && let Ok(time) = time_str.parse::<NaiveTime>()
                {
                    let dt = NaiveDateTime::new(parsed.date_naive(), time);
                    return Some(dt.and_utc());
                }
                Some(parsed)
            }
            Self::EpochMs { field } => {
                let ms = get_f64(record, field)?;
                #[allow(clippy::cast_possible_truncation)]
                let secs = (ms / 1000.0) as i64;
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let nsecs = ((ms % 1000.0) * 1_000_000.0) as u32;
                DateTime::from_timestamp(secs, nsecs)
            }
            Self::MdyDate { field } => {
                let s = get_str(record, field)?;
                let date = NaiveDate::parse_from_str(s, "%m/%d/%Y").ok()?;
                Some(date.and_hms_opt(0, 0, 0)?.and_utc())
            }
        }
    }
}

impl DescriptionExtractor {
    /// Extracts a description string from a JSON record.
    fn extract(&self, record: &serde_json::Value) -> Option<String> {
        match self {
            Self::Single { field } => get_str(record, field).map(String::from),
            Self::Combine { fields, separator } => {
                let parts: Vec<&str> = fields
                    .iter()
                    .filter_map(|f| get_str(record, f))
                    .filter(|s| !s.is_empty())
                    .collect();
                if parts.is_empty() {
                    None
                } else {
                    Some(parts.join(separator))
                }
            }
            Self::FallbackChain { fields } => fields
                .iter()
                .filter_map(|f| get_str(record, f))
                .find(|s| !s.is_empty())
                .map(String::from),
        }
    }
}

impl ArrestExtractor {
    /// Extracts the arrest flag from a JSON record.
    fn extract(&self, record: &serde_json::Value) -> Option<bool> {
        match self {
            Self::None => Option::None,
            Self::DirectBool { field } => get_bool(record, field),
            Self::StringContains { field, contains } => {
                let s = get_str(record, field)?;
                Some(s.to_lowercase().contains(&contains.to_lowercase()))
            }
        }
    }
}

// ── Streaming fetch + normalize ───────────────────────────────────────

/// Channel buffer size — allows the fetcher to stay one page ahead of
/// the consumer (normalizer/inserter).
const PAGE_CHANNEL_BUFFER: usize = 2;

impl SourceDefinition {
    /// Returns the unique source identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the human-readable source name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the configured page size for this source's fetcher.
    #[must_use]
    pub const fn page_size(&self) -> u64 {
        match &self.fetcher {
            FetcherConfig::Socrata { page_size, .. }
            | FetcherConfig::Arcgis { page_size, .. }
            | FetcherConfig::Ckan { page_size, .. }
            | FetcherConfig::Carto { page_size, .. }
            | FetcherConfig::Odata { page_size, .. } => *page_size,
        }
    }

    /// Starts fetching pages in a background task and returns a receiver
    /// that yields one page of raw JSON records at a time.
    ///
    /// The caller should receive pages, call [`Self::normalize_page`] on
    /// each, and insert the results into the database immediately.
    ///
    /// A fetch error (if any) is returned via the [`tokio::task::JoinHandle`].
    #[must_use]
    pub fn fetch_pages(
        &self,
        options: &FetchOptions,
    ) -> (
        mpsc::Receiver<Vec<serde_json::Value>>,
        tokio::task::JoinHandle<Result<u64, SourceError>>,
    ) {
        let (tx, rx) = mpsc::channel(PAGE_CHANNEL_BUFFER);
        let fetcher = self.fetcher.clone();
        let name = self.name.clone();
        let options = options.clone();

        let handle = tokio::spawn(async move {
            match &fetcher {
                FetcherConfig::Socrata {
                    api_url,
                    date_column,
                    page_size,
                } => {
                    fetch_socrata(
                        &SocrataConfig {
                            api_url,
                            date_column,
                            label: &name,
                            page_size: *page_size,
                        },
                        &options,
                        &tx,
                    )
                    .await
                }
                FetcherConfig::Arcgis {
                    query_urls,
                    page_size,
                    where_clause,
                    date_column,
                } => {
                    fetch_arcgis(
                        &ArcGisConfig {
                            query_urls,
                            label: &name,
                            page_size: *page_size,
                            where_clause: where_clause.as_deref(),
                            date_column: date_column.as_deref(),
                        },
                        &options,
                        &tx,
                    )
                    .await
                }
                FetcherConfig::Ckan {
                    api_url,
                    resource_ids,
                    page_size,
                    date_column,
                } => {
                    fetch_ckan(
                        &CkanConfig {
                            api_url,
                            resource_ids,
                            label: &name,
                            page_size: *page_size,
                            date_column: date_column.as_deref(),
                        },
                        &options,
                        &tx,
                    )
                    .await
                }
                FetcherConfig::Carto {
                    api_url,
                    table_name,
                    date_column,
                    page_size,
                } => {
                    fetch_carto(
                        &CartoConfig {
                            api_url,
                            table_name,
                            date_column,
                            label: &name,
                            page_size: *page_size,
                        },
                        &options,
                        &tx,
                    )
                    .await
                }
                FetcherConfig::Odata {
                    api_url,
                    date_column,
                    page_size,
                } => {
                    fetch_odata(
                        &ODataConfig {
                            api_url,
                            date_column,
                            label: &name,
                            page_size: *page_size,
                        },
                        &options,
                        &tx,
                    )
                    .await
                }
            }
        });

        (rx, handle)
    }

    /// Normalizes a single page of raw JSON records into canonical
    /// [`NormalizedIncident`]s.
    pub fn normalize_page(&self, records: &[serde_json::Value]) -> Vec<NormalizedIncident> {
        let fields = &self.fields;
        let mut incidents = Vec::with_capacity(records.len());

        for record in records {
            // ── Lat/lng ──────────────────────────────────────────────
            let Some(latitude) = fields.lat.extract(record) else {
                continue;
            };
            let Some(longitude) = fields.lng.extract(record) else {
                continue;
            };

            // Reject zero coordinates
            if latitude == 0.0 || longitude == 0.0 {
                continue;
            }

            // ── Incident ID ──────────────────────────────────────────
            let Some(source_incident_id) = extract_incident_id(record, &fields.incident_id) else {
                continue;
            };

            // ── Crime type ───────────────────────────────────────────
            let crime_str = fields
                .crime_type
                .iter()
                .filter_map(|f| get_str(record, f))
                .find(|s| !s.is_empty())
                .unwrap_or_default();
            let subcategory = map_crime_type(crime_str);

            // ── Dates ────────────────────────────────────────────────
            let occurred_at = fields.occurred_at.extract(record).unwrap_or_else(Utc::now);

            let reported_at = fields
                .reported_at
                .as_deref()
                .and_then(|f| get_str(record, f))
                .and_then(parse_socrata_date);

            // ── Description ──────────────────────────────────────────
            let description = fields.description.extract(record);

            // ── Optional fields ──────────────────────────────────────
            let block_address = fields
                .block_address
                .as_deref()
                .and_then(|f| get_str(record, f))
                .map(String::from);

            let location_type = fields
                .location_type
                .as_deref()
                .and_then(|f| get_str(record, f))
                .map(String::from);

            let arrest_made = fields.arrest.extract(record);

            let domestic = fields.domestic.as_deref().and_then(|f| get_bool(record, f));

            incidents.push(NormalizedIncident {
                source_incident_id,
                subcategory,
                longitude,
                latitude,
                occurred_at,
                reported_at,
                description,
                block_address,
                city: self.city.clone(),
                state: self.state.clone(),
                arrest_made,
                domestic,
                location_type,
            });
        }

        incidents
    }
}

/// Tries each field name in order and returns the first non-empty string
/// value. Falls back to converting numeric values to strings.
fn extract_incident_id(record: &serde_json::Value, fields: &[String]) -> Option<String> {
    for field in fields {
        if let Some(s) = get_str(record, field)
            && !s.is_empty()
        {
            return Some(s.to_string());
        }
        // Some APIs return numeric IDs (e.g., Philly's objectid is i64)
        if let Some(n) = record.get(field).and_then(serde_json::Value::as_i64) {
            return Some(n.to_string());
        }
    }
    None
}

/// Parses a [`SourceDefinition`] from a TOML string.
///
/// # Errors
///
/// Returns an error if the TOML is malformed or missing required fields.
pub fn parse_source_toml(toml_str: &str) -> Result<SourceDefinition, String> {
    toml::de::from_str(toml_str).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_date() {
        let record = serde_json::json!({"date": "2024-01-15T14:30:00"});
        let extractor = DateExtractor::Simple {
            field: "date".to_string(),
        };
        let dt = extractor.extract(&record).unwrap();
        assert_eq!(dt.to_string(), "2024-01-15 14:30:00 UTC");
    }

    #[test]
    fn parses_date_plus_hhmm() {
        let record = serde_json::json!({"date_occ": "2024-01-15T00:00:00", "time_occ": "1430"});
        let extractor = DateExtractor::DatePlusHhmm {
            date_field: "date_occ".to_string(),
            time_field: "time_occ".to_string(),
        };
        let dt = extractor.extract(&record).unwrap();
        assert_eq!(dt.to_string(), "2024-01-15 14:30:00 UTC");
    }

    #[test]
    fn parses_epoch_ms() {
        let record = serde_json::json!({"report_dat": 1_705_312_200_000.0_f64});
        let extractor = DateExtractor::EpochMs {
            field: "report_dat".to_string(),
        };
        let dt = extractor.extract(&record).unwrap();
        assert_eq!(dt.date_naive().to_string(), "2024-01-15");
    }

    #[test]
    fn extracts_description_combine() {
        let record = serde_json::json!({"type": "THEFT", "detail": "FROM VEHICLE"});
        let extractor = DescriptionExtractor::Combine {
            fields: vec!["type".to_string(), "detail".to_string()],
            separator: ": ".to_string(),
        };
        assert_eq!(extractor.extract(&record).unwrap(), "THEFT: FROM VEHICLE");
    }

    #[test]
    fn extracts_description_fallback() {
        let record = serde_json::json!({"pd_desc": "", "ofns_desc": "ROBBERY"});
        let extractor = DescriptionExtractor::FallbackChain {
            fields: vec!["pd_desc".to_string(), "ofns_desc".to_string()],
        };
        assert_eq!(extractor.extract(&record).unwrap(), "ROBBERY");
    }

    #[test]
    fn extracts_arrest_string_contains() {
        let record = serde_json::json!({"status": "Adult Arrest"});
        let extractor = ArrestExtractor::StringContains {
            field: "status".to_string(),
            contains: "Arrest".to_string(),
        };
        assert_eq!(extractor.extract(&record), Some(true));
    }

    #[test]
    fn extracts_incident_id_fallback() {
        let record = serde_json::json!({"case_number": null, "id": "12345"});
        let fields = vec!["case_number".to_string(), "id".to_string()];
        assert_eq!(extract_incident_id(&record, &fields).unwrap(), "12345");
    }

    #[test]
    fn extracts_numeric_incident_id() {
        let record = serde_json::json!({"objectid": 42});
        let fields = vec!["objectid".to_string()];
        assert_eq!(extract_incident_id(&record, &fields).unwrap(), "42");
    }

    #[test]
    fn parses_chicago_toml() {
        let toml_str = include_str!("../sources/chicago.toml");
        let def = parse_source_toml(toml_str).unwrap();
        assert_eq!(def.id, "chicago_pd");
        assert_eq!(def.city, "Chicago");
        assert_eq!(def.state, "IL");
    }

    #[test]
    fn extracts_geojson_point_coords() {
        let record = serde_json::json!({
            "location": {
                "type": "Point",
                "coordinates": [-122.1994, 37.79242]
            }
        });
        let lat_field = CoordField {
            field: "location".to_string(),
            coord_type: CoordType::PointLat,
        };
        let lng_field = CoordField {
            field: "location".to_string(),
            coord_type: CoordType::PointLng,
        };
        assert!((lat_field.extract(&record).unwrap() - 37.79242).abs() < f64::EPSILON);
        assert!((lng_field.extract(&record).unwrap() - -122.1994).abs() < f64::EPSILON);
    }

    #[test]
    fn extracts_socrata_location_coords() {
        let record = serde_json::json!({
            "geocoded_column": {
                "latitude": "32.714063262",
                "longitude": "-96.888799822"
            }
        });
        let lat_field = CoordField {
            field: "geocoded_column".to_string(),
            coord_type: CoordType::PointLat,
        };
        let lng_field = CoordField {
            field: "geocoded_column".to_string(),
            coord_type: CoordType::PointLng,
        };
        assert!((lat_field.extract(&record).unwrap() - 32.714_063_262).abs() < 1e-6);
        assert!((lng_field.extract(&record).unwrap() - -96.888_799_822).abs() < 1e-6);
    }
}
