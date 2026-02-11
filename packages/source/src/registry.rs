//! Source registry — loads all source definitions from embedded TOML configs.
//!
//! Each `.toml` file in `packages/source/sources/` is baked into the binary
//! at compile time via [`include_str!`]. Adding a new source is as simple as
//! creating a new TOML file and adding it to the list below.

use crate::source_def::{SourceDefinition, parse_source_toml};

/// TOML configs embedded at compile time.
const SOURCE_TOMLS: &[(&str, &str)] = &[
    // ── Socrata sources ──────────────────────────────────────────────
    ("chicago", include_str!("../sources/chicago.toml")),
    ("la", include_str!("../sources/la.toml")),
    (
        "la_historical",
        include_str!("../sources/la_historical.toml"),
    ),
    ("sf", include_str!("../sources/sf.toml")),
    ("seattle", include_str!("../sources/seattle.toml")),
    ("nyc", include_str!("../sources/nyc.toml")),
    ("denver", include_str!("../sources/denver.toml")),
    (
        "montgomery_county_md",
        include_str!("../sources/montgomery_county_md.toml"),
    ),
    ("pg_county_md", include_str!("../sources/pg_county_md.toml")),
    (
        "pg_county_md_historical",
        include_str!("../sources/pg_county_md_historical.toml"),
    ),
    ("dallas", include_str!("../sources/dallas.toml")),
    ("oakland", include_str!("../sources/oakland.toml")),
    ("cincinnati", include_str!("../sources/cincinnati.toml")),
    (
        "cincinnati_current",
        include_str!("../sources/cincinnati_current.toml"),
    ),
    ("baton_rouge", include_str!("../sources/baton_rouge.toml")),
    (
        "baton_rouge_historical",
        include_str!("../sources/baton_rouge_historical.toml"),
    ),
    ("gainesville", include_str!("../sources/gainesville.toml")),
    ("kansas_city", include_str!("../sources/kansas_city.toml")),
    ("cambridge", include_str!("../sources/cambridge.toml")),
    ("mesa", include_str!("../sources/mesa.toml")),
    ("everett", include_str!("../sources/everett.toml")),
    // ── ArcGIS sources ───────────────────────────────────────────────
    ("dc", include_str!("../sources/dc.toml")),
    ("baltimore_md", include_str!("../sources/baltimore_md.toml")),
    (
        "baltimore_county_md",
        include_str!("../sources/baltimore_county_md.toml"),
    ),
    (
        "baltimore_nibrs_md",
        include_str!("../sources/baltimore_nibrs_md.toml"),
    ),
    (
        "baltimore_historical_md",
        include_str!("../sources/baltimore_historical_md.toml"),
    ),
    (
        "prince_william_va",
        include_str!("../sources/prince_william_va.toml"),
    ),
    ("fairfax_va", include_str!("../sources/fairfax_va.toml")),
    ("atlanta", include_str!("../sources/atlanta.toml")),
    ("detroit", include_str!("../sources/detroit.toml")),
    ("charlotte", include_str!("../sources/charlotte.toml")),
    ("minneapolis", include_str!("../sources/minneapolis.toml")),
    ("tampa", include_str!("../sources/tampa.toml")),
    ("las_vegas", include_str!("../sources/las_vegas.toml")),
    ("raleigh", include_str!("../sources/raleigh.toml")),
    ("lynchburg_va", include_str!("../sources/lynchburg_va.toml")),
    (
        "chesterfield_va",
        include_str!("../sources/chesterfield_va.toml"),
    ),
    ("houston", include_str!("../sources/houston.toml")),
    ("nashville", include_str!("../sources/nashville.toml")),
    // ── Carto sources ────────────────────────────────────────────────
    ("philly", include_str!("../sources/philly.toml")),
    // ── CKAN sources ─────────────────────────────────────────────────
    ("boston", include_str!("../sources/boston.toml")),
    ("pittsburgh", include_str!("../sources/pittsburgh.toml")),
    (
        "pittsburgh_current",
        include_str!("../sources/pittsburgh_current.toml"),
    ),
    // ── OData sources ────────────────────────────────────────────────
    ("arlington_va", include_str!("../sources/arlington_va.toml")),
];

/// Total number of configured sources (used in tests).
#[cfg(test)]
const EXPECTED_SOURCE_COUNT: usize = 44;

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
        assert_eq!(sources.len(), EXPECTED_SOURCE_COUNT);
    }

    #[test]
    fn source_ids_are_unique() {
        let sources = all_sources();
        let mut ids: Vec<&str> = sources.iter().map(|s| s.id.as_str()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), EXPECTED_SOURCE_COUNT);
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
