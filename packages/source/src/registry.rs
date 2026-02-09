//! Source registry — loads all source definitions from embedded TOML configs.
//!
//! Each `.toml` file in `packages/source/sources/` is baked into the binary
//! at compile time via [`include_str!`]. Adding a new source is as simple as
//! creating a new TOML file and adding it to the list below.

use crate::source_def::{SourceDefinition, parse_source_toml};

/// TOML configs embedded at compile time.
const SOURCE_TOMLS: &[(&str, &str)] = &[
    ("chicago", include_str!("../sources/chicago.toml")),
    ("la", include_str!("../sources/la.toml")),
    ("sf", include_str!("../sources/sf.toml")),
    ("seattle", include_str!("../sources/seattle.toml")),
    ("nyc", include_str!("../sources/nyc.toml")),
    ("denver", include_str!("../sources/denver.toml")),
    ("dc", include_str!("../sources/dc.toml")),
    ("philly", include_str!("../sources/philly.toml")),
    ("boston", include_str!("../sources/boston.toml")),
];

/// Returns all configured source definitions, parsed from embedded TOML.
///
/// # Panics
///
/// Panics if any TOML config is malformed (this is a compile-time guarantee
/// since the configs are embedded).
#[must_use]
pub fn all_sources() -> Vec<SourceDefinition> {
    SOURCE_TOMLS
        .iter()
        .map(|(name, toml)| {
            parse_source_toml(toml).unwrap_or_else(|e| panic!("Failed to parse {name}.toml: {e}"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_all_sources() {
        let sources = all_sources();
        assert_eq!(sources.len(), 9);
    }

    #[test]
    fn source_ids_are_unique() {
        let sources = all_sources();
        let mut ids: Vec<&str> = sources.iter().map(|s| s.id.as_str()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 9);
    }

    #[test]
    fn all_sources_have_required_fields() {
        for source in &all_sources() {
            assert!(!source.id.is_empty(), "source id is empty");
            assert!(!source.name.is_empty(), "source name is empty");
            assert!(!source.city.is_empty(), "source city is empty");
            assert!(!source.state.is_empty(), "source state is empty");
            assert!(
                !source.fields.incident_id.is_empty(),
                "{}: no incident_id fields",
                source.id
            );
            assert!(
                !source.fields.crime_type.is_empty(),
                "{}: no crime_type fields",
                source.id
            );
        }
    }
}
