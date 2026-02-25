//! OpenStreetMap PBF address extractor.
//!
//! Parses a US OSM PBF extract and extracts address records from nodes
//! and dense nodes that have `addr:housenumber` and `addr:street` tags.
//!
//! Way addresses are skipped in v1 because they require a two-pass
//! approach to resolve node coordinates (first pass indexes node
//! positions, second pass resolves ways). Nodes cover the vast
//! majority of OSM addresses.

use std::path::Path;

use crate::normalize;
use crate::openaddresses::NormalizedAddress;

/// Parses an OSM PBF file and extracts address records.
///
/// Uses `osmpbf`'s parallel reader (`par_map_reduce`) for fast
/// multi-threaded decoding. Only nodes and dense nodes with
/// `addr:housenumber` + `addr:street` tags are extracted.
///
/// # Errors
///
/// Returns an error if the PBF file cannot be read or parsed.
pub fn parse_pbf(path: &Path) -> Result<Vec<NormalizedAddress>, OsmError> {
    use osmpbf::{Element, ElementReader};

    if !path.exists() {
        return Err(OsmError::FileNotFound(path.display().to_string()));
    }

    log::info!("Parsing OSM PBF: {}", path.display());

    let reader = ElementReader::from_path(path).map_err(|e| OsmError::Parse {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;

    let addresses = reader
        .par_map_reduce(
            |element| {
                let mut results = Vec::new();
                match element {
                    Element::Node(node) => {
                        if let Some(addr) =
                            extract_address_from_tags(node.tags(), node.lat(), node.lon())
                        {
                            results.push(addr);
                        }
                    }
                    Element::DenseNode(node) => {
                        if let Some(addr) =
                            extract_address_from_tags(node.tags(), node.lat(), node.lon())
                        {
                            results.push(addr);
                        }
                    }
                    Element::Way(_) | Element::Relation(_) => {
                        // Skip ways and relations for v1.
                        // Ways would require a two-pass approach to resolve
                        // node coordinates.
                    }
                }
                results
            },
            Vec::new,
            |mut a, mut b| {
                a.append(&mut b);
                a
            },
        )
        .map_err(|e| OsmError::Parse {
            path: path.display().to_string(),
            message: e.to_string(),
        })?;

    log::info!("Extracted {} addresses from OSM PBF", addresses.len());
    Ok(addresses)
}

/// Extracts an address from OSM tags if `addr:housenumber` and
/// `addr:street` are present.
fn extract_address_from_tags<'a>(
    tags: impl Iterator<Item = (&'a str, &'a str)>,
    lat: f64,
    lon: f64,
) -> Option<NormalizedAddress> {
    if !lat.is_finite() || !lon.is_finite() {
        return None;
    }
    if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
        return None;
    }

    let mut housenumber: Option<&str> = None;
    let mut street: Option<&str> = None;
    let mut city: Option<&str> = None;
    let mut state: Option<&str> = None;
    let mut postcode: Option<&str> = None;

    for (key, value) in tags {
        match key {
            "addr:housenumber" => housenumber = Some(value),
            "addr:street" => street = Some(value),
            "addr:city" => city = Some(value),
            "addr:state" => state = Some(value),
            "addr:postcode" => postcode = Some(value),
            _ => {}
        }
    }

    let number = housenumber?.trim();
    let street_name = street?.trim();

    if number.is_empty() || street_name.is_empty() {
        return None;
    }

    let norm_street = normalize::normalize_street(number, street_name);
    if norm_street.is_empty() {
        return None;
    }

    let norm_city = city.map(normalize::normalize).unwrap_or_default();
    let norm_state = state.map(normalize::normalize_state).unwrap_or_default();
    let postcode_val = postcode.map(|p| p.trim().to_string()).unwrap_or_default();

    let full_address = normalize::build_full_address(&norm_street, &norm_city, &norm_state);

    Some(NormalizedAddress {
        street: norm_street,
        city: norm_city,
        state: norm_state,
        postcode: postcode_val,
        full_address,
        lat,
        lon,
    })
}

/// Errors from OSM PBF parsing.
#[derive(Debug, thiserror::Error)]
pub enum OsmError {
    /// PBF file not found.
    #[error("OSM PBF file not found: {0}")]
    FileNotFound(String),

    /// PBF parsing error.
    #[error("OSM PBF parse error in {path}: {message}")]
    Parse {
        /// Path to the PBF file.
        path: String,
        /// Error description.
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_address_from_tags() {
        let tags = vec![
            ("addr:housenumber", "100"),
            ("addr:street", "N State St"),
            ("addr:city", "Chicago"),
            ("addr:state", "IL"),
            ("addr:postcode", "60602"),
            ("name", "Some Place"),
        ];

        let addr = extract_address_from_tags(tags.into_iter(), 41.8827, -87.6278).unwrap();

        assert_eq!(addr.street, "100 NORTH STATE STREET");
        assert_eq!(addr.city, "CHICAGO");
        assert_eq!(addr.state, "IL");
        assert_eq!(addr.postcode, "60602");
        assert!((addr.lat - 41.8827).abs() < 1e-4);
    }

    #[test]
    fn skips_missing_housenumber() {
        let tags = vec![("addr:street", "Main St")];
        assert!(extract_address_from_tags(tags.into_iter(), 41.0, -87.0).is_none());
    }

    #[test]
    fn skips_missing_street() {
        let tags = vec![("addr:housenumber", "100")];
        assert!(extract_address_from_tags(tags.into_iter(), 41.0, -87.0).is_none());
    }
}
