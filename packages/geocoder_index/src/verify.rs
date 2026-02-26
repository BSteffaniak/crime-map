//! Smoke test runner for the geocoder index.
//!
//! Loads test addresses from the embedded `smoke_tests.toml` file and
//! searches the index for each one, verifying that the returned
//! coordinates are within a configurable tolerance of the expected
//! values.
//!
//! All tests must pass. If an address is not in the current data set,
//! the test entry should be updated or removed from the TOML file.

use serde::Deserialize;

use crate::{GeocoderIndex, GeocoderIndexError};

/// Embedded smoke test configuration (compiled into the binary).
const SMOKE_TESTS_TOML: &str = include_str!("../smoke_tests.toml");

/// Parsed smoke test configuration.
#[derive(Debug, Deserialize)]
struct SmokeTestConfig {
    /// Default coordinate tolerance in degrees.
    default_tolerance: f64,
    /// Individual test cases.
    tests: Vec<SmokeTestEntry>,
}

/// A single smoke test entry from the TOML file.
#[derive(Debug, Deserialize)]
struct SmokeTestEntry {
    /// Free-form address string (`"street, city, state"`).
    address: String,
    /// Expected latitude.
    lat: f64,
    /// Expected longitude.
    lon: f64,
    /// Optional per-test tolerance override.
    tolerance: Option<f64>,
}

/// Result of a single smoke test.
#[derive(Debug)]
pub struct SmokeTestResult {
    /// The address that was searched.
    pub address: String,
    /// Expected latitude.
    pub expected_lat: f64,
    /// Expected longitude.
    pub expected_lon: f64,
    /// Actual latitude returned by the index (if found).
    pub actual_lat: Option<f64>,
    /// Actual longitude returned by the index (if found).
    pub actual_lon: Option<f64>,
    /// Matched address string from the index (if found).
    pub matched_address: Option<String>,
    /// Tantivy score (if found).
    pub score: Option<f32>,
    /// Tolerance used for this test.
    pub tolerance: f64,
    /// Whether the test passed.
    pub passed: bool,
    /// Reason for failure (if any).
    pub failure_reason: Option<String>,
}

/// Aggregate report from running all smoke tests.
#[derive(Debug)]
pub struct SmokeTestReport {
    /// Individual test results.
    pub results: Vec<SmokeTestResult>,
    /// Number of tests that passed.
    pub passed: usize,
    /// Total number of tests.
    pub total: usize,
}

impl SmokeTestReport {
    /// Returns `true` if all tests passed.
    #[must_use]
    pub fn all_passed(&self) -> bool {
        self.passed == self.total
    }
}

/// Runs all smoke tests against the given geocoder index.
///
/// Each test searches for an address and checks that the returned
/// coordinates are within the configured tolerance.
///
/// # Errors
///
/// Returns an error if the TOML configuration cannot be parsed or
/// a search operation fails due to index corruption.
pub fn run_smoke_tests(index: &GeocoderIndex) -> Result<SmokeTestReport, GeocoderIndexError> {
    let config: SmokeTestConfig = toml::from_str(SMOKE_TESTS_TOML)
        .map_err(|e| GeocoderIndexError::Other(format!("Failed to parse smoke_tests.toml: {e}")))?;

    let mut results = Vec::with_capacity(config.tests.len());

    for entry in &config.tests {
        let tolerance = entry.tolerance.unwrap_or(config.default_tolerance);
        let result = run_single_test(index, entry, tolerance)?;
        results.push(result);
    }

    let passed = results.iter().filter(|r| r.passed).count();
    let total = results.len();

    Ok(SmokeTestReport {
        results,
        passed,
        total,
    })
}

/// Runs a single smoke test.
fn run_single_test(
    index: &GeocoderIndex,
    entry: &SmokeTestEntry,
    tolerance: f64,
) -> Result<SmokeTestResult, GeocoderIndexError> {
    // Parse "street, city, state" format
    let parts: Vec<&str> = entry.address.splitn(3, ',').collect();
    let (street, city, state) = match parts.len() {
        3 => (parts[0].trim(), parts[1].trim(), parts[2].trim()),
        2 => (parts[0].trim(), parts[1].trim(), ""),
        _ => (entry.address.trim(), "", ""),
    };

    let search_result = index.search_sync(street, city, state)?;

    match search_result {
        Some(hit) => {
            let dlat = (hit.latitude - entry.lat).abs();
            let dlon = (hit.longitude - entry.lon).abs();
            let within_tolerance = dlat <= tolerance && dlon <= tolerance;

            let failure_reason = if within_tolerance {
                None
            } else {
                Some(format!(
                    "coordinates too far: delta lat={dlat:.5}, lon={dlon:.5} (tolerance={tolerance})"
                ))
            };

            Ok(SmokeTestResult {
                address: entry.address.clone(),
                expected_lat: entry.lat,
                expected_lon: entry.lon,
                actual_lat: Some(hit.latitude),
                actual_lon: Some(hit.longitude),
                matched_address: Some(format!(
                    "{}, {}, {}",
                    hit.matched_street, hit.matched_city, hit.matched_state
                )),
                score: Some(hit.score),
                tolerance,
                passed: within_tolerance,
                failure_reason,
            })
        }
        None => Ok(SmokeTestResult {
            address: entry.address.clone(),
            expected_lat: entry.lat,
            expected_lon: entry.lon,
            actual_lat: None,
            actual_lon: None,
            matched_address: None,
            score: None,
            tolerance,
            passed: false,
            failure_reason: Some("no match found".to_string()),
        }),
    }
}

/// Returns the total number of documents in the index.
///
/// # Errors
///
/// Returns an error if the index searcher cannot be acquired.
pub fn document_count(index: &GeocoderIndex) -> Result<u64, GeocoderIndexError> {
    Ok(index.num_docs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_embedded_toml() {
        let config: SmokeTestConfig = toml::from_str(SMOKE_TESTS_TOML).unwrap();
        assert!(
            config.default_tolerance > 0.0,
            "default_tolerance must be positive"
        );
        assert!(
            !config.tests.is_empty(),
            "smoke_tests.toml must have at least one test"
        );

        for test in &config.tests {
            assert!(!test.address.is_empty(), "address must not be empty");
            assert!(
                test.address.contains(','),
                "address '{}' should be in 'street, city, state' format",
                test.address
            );
            assert!(
                (-90.0..=90.0).contains(&test.lat),
                "invalid lat {} for '{}'",
                test.lat,
                test.address
            );
            assert!(
                (-180.0..=180.0).contains(&test.lon),
                "invalid lon {} for '{}'",
                test.lon,
                test.address
            );
        }
    }
}
