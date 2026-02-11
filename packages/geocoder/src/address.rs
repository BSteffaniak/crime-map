//! Address cleaning and normalization for crime data.
//!
//! Crime data sources provide addresses in many formats:
//! - Block addresses: `"100 N STATE ST"`
//! - With noise words: `"4800 BLOCK OF SILVER HILL RD"`
//! - Privacy-masked: `"XX00 MAIN ST"`
//! - Intersections: `"1ST ST / MAIN AVE"`
//!
//! This module normalizes these into a form suitable for geocoding.

use regex::Regex;
use std::sync::LazyLock;

/// Regex for XX-masked house numbers (e.g., "XX00", "xx12").
static XX_MASK_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^XX(\d+)").expect("valid regex"));

/// Regex for "BLOCK OF" / "BLK OF" noise in addresses.
static BLOCK_OF_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)\s+BLOCK\s+OF\s+|\s+BLK\s+OF\s+").expect("valid regex"));

/// Regex for standalone "BLOCK" / "BL" / "BLK" after a house number,
/// with or without a space between the number and the keyword.
/// Matches: "100 BLOCK", "100BLOCK", "300BL", "300 BL", "100BLK", "100 BLK".
static BLOCK_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)^(\d+)\s*(?:BLOCK|BLK|BL)\s+").expect("valid regex"));

/// Regex for direction abbreviations that should be expanded (at end of
/// address fragments like "EB", "WB", "NB", "SB").
static DIRECTION_SUFFIX_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\s+(EB|WB|NB|SB)$").expect("valid regex"));

/// Non-geocodable address patterns.
static SKIP_PATTERNS: &[&str] = &[
    "UNKNOWN",
    "N/A",
    "NA",
    "NONE",
    "NOT AVAILABLE",
    "UNDETERMINED",
    "UNSPECIFIED",
];

/// Result of cleaning a block address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CleanedAddress {
    /// A street address suitable for geocoding.
    Street(String),
    /// An intersection (two streets). Census can't geocode these but
    /// Nominatim might with a free-form query.
    Intersection {
        /// First street.
        street1: String,
        /// Second street.
        street2: String,
    },
    /// The address is not geocodable (empty, unknown, garbage).
    NotGeocodable,
}

/// Cleans and normalizes a block address for geocoding.
///
/// Returns [`CleanedAddress::Street`] for normal addresses,
/// [`CleanedAddress::Intersection`] for cross-street patterns, or
/// [`CleanedAddress::NotGeocodable`] for garbage input.
#[must_use]
pub fn clean_block_address(raw: &str) -> CleanedAddress {
    let addr = raw.trim().to_uppercase();

    // Skip obvious non-addresses
    if addr.is_empty() || SKIP_PATTERNS.iter().any(|p| addr == *p) {
        return CleanedAddress::NotGeocodable;
    }

    // Remove direction suffixes (EB/WB/NB/SB)
    let addr = DIRECTION_SUFFIX_RE.replace_all(&addr, "").to_string();

    // Detect intersections: "A / B", "A & B", "A AND B"
    for sep in [" / ", " /", "/ ", " & ", " AND "] {
        if let Some(idx) = addr.find(sep) {
            let street1 = addr[..idx].trim().to_string();
            let street2 = addr[idx + sep.len()..].trim().to_string();
            if !street1.is_empty() && !street2.is_empty() {
                return CleanedAddress::Intersection { street1, street2 };
            }
        }
    }

    // Remove "BLOCK OF" / "BLK OF"
    let addr = BLOCK_OF_RE.replace_all(&addr, " ").to_string();

    // Remove standalone "BLOCK": "100 BLOCK MAIN ST" → "100 MAIN ST"
    let addr = BLOCK_RE.replace(&addr, "$1 ").to_string();

    // Handle XX-masked house numbers: "XX00" → "100"
    let addr = XX_MASK_RE.replace(&addr, "1$1").to_string();

    let addr = addr.trim().to_string();

    if addr.is_empty() {
        return CleanedAddress::NotGeocodable;
    }

    CleanedAddress::Street(addr)
}

/// Builds a one-line address string from components for the Census geocoder.
#[must_use]
pub fn build_one_line_address(street: &str, city: &str, state: &str) -> String {
    format!("{street}, {city}, {state}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleans_normal_address() {
        assert_eq!(
            clean_block_address("100 N STATE ST"),
            CleanedAddress::Street("100 N STATE ST".to_string())
        );
    }

    #[test]
    fn removes_block_of() {
        assert_eq!(
            clean_block_address("4800 BLOCK OF SILVER HILL RD"),
            CleanedAddress::Street("4800 SILVER HILL RD".to_string())
        );
    }

    #[test]
    fn removes_blk_of() {
        assert_eq!(
            clean_block_address("100 BLK OF MAIN ST"),
            CleanedAddress::Street("100 MAIN ST".to_string())
        );
    }

    #[test]
    fn removes_standalone_block() {
        assert_eq!(
            clean_block_address("100 BLOCK MAIN ST"),
            CleanedAddress::Street("100 MAIN ST".to_string())
        );
    }

    #[test]
    fn handles_xx_mask() {
        assert_eq!(
            clean_block_address("XX00 MAIN ST"),
            CleanedAddress::Street("100 MAIN ST".to_string())
        );
    }

    #[test]
    fn detects_intersection_slash() {
        assert_eq!(
            clean_block_address("1ST ST / MAIN AVE"),
            CleanedAddress::Intersection {
                street1: "1ST ST".to_string(),
                street2: "MAIN AVE".to_string(),
            }
        );
    }

    #[test]
    fn detects_intersection_and() {
        assert_eq!(
            clean_block_address("BROADWAY AND 5TH AVE"),
            CleanedAddress::Intersection {
                street1: "BROADWAY".to_string(),
                street2: "5TH AVE".to_string(),
            }
        );
    }

    #[test]
    fn strips_direction_suffixes() {
        assert_eq!(
            clean_block_address("MARLBORO PIKE WB"),
            CleanedAddress::Street("MARLBORO PIKE".to_string())
        );
    }

    #[test]
    fn skips_unknown() {
        assert_eq!(
            clean_block_address("UNKNOWN"),
            CleanedAddress::NotGeocodable
        );
    }

    #[test]
    fn skips_empty() {
        assert_eq!(clean_block_address(""), CleanedAddress::NotGeocodable);
    }

    #[test]
    fn skips_na() {
        assert_eq!(clean_block_address("N/A"), CleanedAddress::NotGeocodable);
    }

    #[test]
    fn handles_number_bl_no_space() {
        assert_eq!(
            clean_block_address("300BL CEDARLEAF AVE"),
            CleanedAddress::Street("300 CEDARLEAF AVE".to_string())
        );
    }

    #[test]
    fn handles_number_block_no_space() {
        assert_eq!(
            clean_block_address("5900BLOCK FISHER RD"),
            CleanedAddress::Street("5900 FISHER RD".to_string())
        );
    }

    #[test]
    fn handles_number_blk_no_space() {
        assert_eq!(
            clean_block_address("100BLK MAIN ST"),
            CleanedAddress::Street("100 MAIN ST".to_string())
        );
    }

    #[test]
    fn handles_number_bl_with_space() {
        assert_eq!(
            clean_block_address("300 BL CEDARLEAF AVE"),
            CleanedAddress::Street("300 CEDARLEAF AVE".to_string())
        );
    }
}
