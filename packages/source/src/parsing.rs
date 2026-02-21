//! Shared parsing utilities for crime data sources.
//!
//! Common date and coordinate parsing functions used across multiple source
//! implementations.

use chrono::{DateTime, NaiveDateTime, Utc};

/// Parses a datetime string commonly found in Socrata and CKAN APIs.
///
/// Supports the following formats:
/// - `2024-01-15T14:30:00.000` (Socrata with fractional seconds)
/// - `2024-01-15T14:30:00` (Socrata without fractional seconds)
/// - `2024-01-15 14:30:00+00` (CKAN with timezone offset)
/// - `2024-01-15 14:30:00` (space-separated without timezone)
#[must_use]
pub fn parse_socrata_date(s: &str) -> Option<DateTime<Utc>> {
    // Try ISO 8601 with 'T' separator and optional fractional seconds
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(naive.and_utc());
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(naive.and_utc());
    }
    // Try space-separated with timezone offset (e.g., "2023-01-27 22:44:00+00")
    if let Ok(dt) = DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%#z") {
        return Some(dt.with_timezone(&Utc));
    }
    // Try space-separated without timezone
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(naive.and_utc());
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
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
    fn parses_ckan_date_with_tz() {
        let dt = parse_socrata_date("2023-01-27 22:44:00+00").unwrap();
        assert_eq!(dt.to_string(), "2023-01-27 22:44:00 UTC");
    }

    #[test]
    fn parses_space_separated_date() {
        let dt = parse_socrata_date("2024-01-15 14:30:00").unwrap();
        assert_eq!(dt.to_string(), "2024-01-15 14:30:00 UTC");
    }

    #[test]
    fn parses_space_separated_date_with_fractional() {
        let dt = parse_socrata_date("2025-01-31 21:01:00.000").unwrap();
        assert_eq!(dt.to_string(), "2025-01-31 21:01:00 UTC");
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
