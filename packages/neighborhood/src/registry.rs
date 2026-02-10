//! Compile-time registry of neighborhood boundary data sources.
//!
//! Each entry is a `(name, toml_content)` pair embedded via `include_str!`.
//! Adding a new city requires creating a TOML file in `sources/` and adding
//! a corresponding entry here.

use crime_map_neighborhood_models::NeighborhoodSource;

/// Number of registered neighborhood sources. Updated when new sources
/// are added. Enforced by a test.
#[cfg(test)]
const EXPECTED_SOURCE_COUNT: usize = 17;

/// Embedded TOML source definitions.
const SOURCE_TOMLS: &[(&str, &str)] = &[
    ("dc", include_str!("../sources/dc.toml")),
    ("chicago", include_str!("../sources/chicago.toml")),
    ("nyc", include_str!("../sources/nyc.toml")),
    ("los_angeles", include_str!("../sources/los_angeles.toml")),
    (
        "san_francisco",
        include_str!("../sources/san_francisco.toml"),
    ),
    ("philadelphia", include_str!("../sources/philadelphia.toml")),
    ("boston", include_str!("../sources/boston.toml")),
    ("denver", include_str!("../sources/denver.toml")),
    ("atlanta", include_str!("../sources/atlanta.toml")),
    ("baltimore", include_str!("../sources/baltimore.toml")),
    ("detroit", include_str!("../sources/detroit.toml")),
    ("minneapolis", include_str!("../sources/minneapolis.toml")),
    ("dallas", include_str!("../sources/dallas.toml")),
    ("pittsburgh", include_str!("../sources/pittsburgh.toml")),
    ("cincinnati", include_str!("../sources/cincinnati.toml")),
    ("kansas_city", include_str!("../sources/kansas_city.toml")),
    ("tampa", include_str!("../sources/tampa.toml")),
];

/// Returns all registered neighborhood sources.
///
/// # Panics
///
/// Panics if any embedded TOML file fails to parse. Since these are
/// compile-time constants, parse failures indicate a development error
/// and are caught during CI.
#[must_use]
pub fn all_sources() -> Vec<NeighborhoodSource> {
    SOURCE_TOMLS
        .iter()
        .map(|(name, toml_str)| {
            toml::de::from_str(toml_str)
                .unwrap_or_else(|e| panic!("Failed to parse neighborhood source '{name}': {e}"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn loads_all_sources() {
        let sources = all_sources();
        assert_eq!(
            sources.len(),
            EXPECTED_SOURCE_COUNT,
            "Expected {EXPECTED_SOURCE_COUNT} neighborhood sources, found {}. \
             Update EXPECTED_SOURCE_COUNT after adding/removing sources.",
            sources.len()
        );
    }

    #[test]
    fn source_ids_are_unique() {
        let sources = all_sources();
        let mut seen = BTreeSet::new();
        for source in &sources {
            assert!(
                seen.insert(&source.id),
                "Duplicate neighborhood source ID: {}",
                source.id
            );
        }
    }

    #[test]
    fn all_sources_have_required_fields() {
        for source in &all_sources() {
            assert!(!source.id.is_empty(), "Source has empty id");
            assert!(
                !source.name.is_empty(),
                "Source {} has empty name",
                source.id
            );
            assert!(
                !source.city.is_empty(),
                "Source {} has empty city",
                source.id
            );
            assert!(
                source.state.len() == 2,
                "Source {} has invalid state: {}",
                source.id,
                source.state
            );
            assert!(
                !source.fields.name.is_empty(),
                "Source {} has empty name field",
                source.id
            );
        }
    }
}
