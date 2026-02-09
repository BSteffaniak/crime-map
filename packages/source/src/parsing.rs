//! Shared parsing utilities for crime data sources.
//!
//! Common date and coordinate parsing functions used across multiple source
//! implementations.

use chrono::{DateTime, NaiveDateTime, Utc};

/// Parses a Socrata datetime string (ISO 8601 with optional fractional seconds).
#[must_use]
pub fn parse_socrata_date(s: &str) -> Option<DateTime<Utc>> {
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
#[must_use]
pub fn parse_lat_lng_str(lat: Option<&String>, lng: Option<&String>) -> Option<(f64, f64)> {
    let lat_str = lat?.as_str();
    let lng_str = lng?.as_str();
    let latitude = lat_str.parse::<f64>().ok()?;
    let longitude = lng_str.parse::<f64>().ok()?;
    if latitude == 0.0 || longitude == 0.0 {
        return None;
    }
    Some((latitude, longitude))
}

/// Parses lat/lng from optional f64 fields. Returns `None` if missing or zero.
#[must_use]
pub fn parse_lat_lng_f64(lat: Option<f64>, lng: Option<f64>) -> Option<(f64, f64)> {
    let latitude = lat?;
    let longitude = lng?;
    if latitude == 0.0 || longitude == 0.0 {
        return None;
    }
    Some((latitude, longitude))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_socrata_date_with_fractional() {
        let dt = parse_socrata_date("2024-01-15T14:30:00.000").unwrap();
        assert_eq!(dt.to_string(), "2024-01-15 14:30:00 UTC");
    }

    #[test]
    fn parses_socrata_date_without_fractional() {
        let dt = parse_socrata_date("2024-01-15T14:30:00").unwrap();
        assert_eq!(dt.to_string(), "2024-01-15 14:30:00 UTC");
    }

    #[test]
    fn rejects_invalid_date() {
        assert!(parse_socrata_date("not-a-date").is_none());
    }

    #[test]
    fn parses_lat_lng_strings() {
        let lat = "41.8781".to_string();
        let lng = "-87.6298".to_string();
        let (la, lo) = parse_lat_lng_str(Some(&lat), Some(&lng)).unwrap();
        assert!((la - 41.8781).abs() < f64::EPSILON);
        assert!((lo - -87.6298).abs() < f64::EPSILON);
    }

    #[test]
    fn rejects_zero_lat_lng() {
        let lat = "0.0".to_string();
        let lng = "-87.6298".to_string();
        assert!(parse_lat_lng_str(Some(&lat), Some(&lng)).is_none());
    }

    #[test]
    fn rejects_missing_lat_lng() {
        let lng = "-87.6298".to_string();
        assert!(parse_lat_lng_str(None, Some(&lng)).is_none());
    }

    #[test]
    fn parses_f64_lat_lng() {
        let (la, lo) = parse_lat_lng_f64(Some(41.8781), Some(-87.6298)).unwrap();
        assert!((la - 41.8781).abs() < f64::EPSILON);
        assert!((lo - -87.6298).abs() < f64::EPSILON);
    }
}
