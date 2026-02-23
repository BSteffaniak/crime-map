//! US state FIPS code utilities.
//!
//! Provides mappings between two-digit FIPS codes, two-letter state
//! abbreviations, and full state names for the 50 US states + DC.

/// US state FIPS codes for the 50 states + DC.
pub const STATE_FIPS: &[&str] = &[
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12", "13", "15", "16", "17", "18", "19",
    "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35",
    "36", "37", "38", "39", "40", "41", "42", "44", "45", "46", "47", "48", "49", "50", "51", "53",
    "54", "55", "56",
];

/// Maps a two-digit FIPS code to the corresponding two-letter state
/// abbreviation.
///
/// Returns `"??"` for unrecognized codes.
#[must_use]
pub fn state_abbr(fips: &str) -> &'static str {
    match fips {
        "01" => "AL",
        "02" => "AK",
        "04" => "AZ",
        "05" => "AR",
        "06" => "CA",
        "08" => "CO",
        "09" => "CT",
        "10" => "DE",
        "11" => "DC",
        "12" => "FL",
        "13" => "GA",
        "15" => "HI",
        "16" => "ID",
        "17" => "IL",
        "18" => "IN",
        "19" => "IA",
        "20" => "KS",
        "21" => "KY",
        "22" => "LA",
        "23" => "ME",
        "24" => "MD",
        "25" => "MA",
        "26" => "MI",
        "27" => "MN",
        "28" => "MS",
        "29" => "MO",
        "30" => "MT",
        "31" => "NE",
        "32" => "NV",
        "33" => "NH",
        "34" => "NJ",
        "35" => "NM",
        "36" => "NY",
        "37" => "NC",
        "38" => "ND",
        "39" => "OH",
        "40" => "OK",
        "41" => "OR",
        "42" => "PA",
        "44" => "RI",
        "45" => "SC",
        "46" => "SD",
        "47" => "TN",
        "48" => "TX",
        "49" => "UT",
        "50" => "VT",
        "51" => "VA",
        "53" => "WA",
        "54" => "WV",
        "55" => "WI",
        "56" => "WY",
        _ => "??",
    }
}

/// Maps a two-digit FIPS code to the full state name.
///
/// Returns `"Unknown"` for unrecognized codes.
#[must_use]
pub fn state_name(fips: &str) -> &'static str {
    match fips {
        "01" => "Alabama",
        "02" => "Alaska",
        "04" => "Arizona",
        "05" => "Arkansas",
        "06" => "California",
        "08" => "Colorado",
        "09" => "Connecticut",
        "10" => "Delaware",
        "11" => "District of Columbia",
        "12" => "Florida",
        "13" => "Georgia",
        "15" => "Hawaii",
        "16" => "Idaho",
        "17" => "Illinois",
        "18" => "Indiana",
        "19" => "Iowa",
        "20" => "Kansas",
        "21" => "Kentucky",
        "22" => "Louisiana",
        "23" => "Maine",
        "24" => "Maryland",
        "25" => "Massachusetts",
        "26" => "Michigan",
        "27" => "Minnesota",
        "28" => "Mississippi",
        "29" => "Missouri",
        "30" => "Montana",
        "31" => "Nebraska",
        "32" => "Nevada",
        "33" => "New Hampshire",
        "34" => "New Jersey",
        "35" => "New Mexico",
        "36" => "New York",
        "37" => "North Carolina",
        "38" => "North Dakota",
        "39" => "Ohio",
        "40" => "Oklahoma",
        "41" => "Oregon",
        "42" => "Pennsylvania",
        "44" => "Rhode Island",
        "45" => "South Carolina",
        "46" => "South Dakota",
        "47" => "Tennessee",
        "48" => "Texas",
        "49" => "Utah",
        "50" => "Vermont",
        "51" => "Virginia",
        "53" => "Washington",
        "54" => "West Virginia",
        "55" => "Wisconsin",
        "56" => "Wyoming",
        _ => "Unknown",
    }
}

/// Maps a two-letter state abbreviation to the corresponding FIPS code.
///
/// Returns `None` for unrecognized abbreviations.
#[must_use]
pub fn abbr_to_fips(abbr: &str) -> Option<&'static str> {
    match abbr.to_uppercase().as_str() {
        "AL" => Some("01"),
        "AK" => Some("02"),
        "AZ" => Some("04"),
        "AR" => Some("05"),
        "CA" => Some("06"),
        "CO" => Some("08"),
        "CT" => Some("09"),
        "DE" => Some("10"),
        "DC" => Some("11"),
        "FL" => Some("12"),
        "GA" => Some("13"),
        "HI" => Some("15"),
        "ID" => Some("16"),
        "IL" => Some("17"),
        "IN" => Some("18"),
        "IA" => Some("19"),
        "KS" => Some("20"),
        "KY" => Some("21"),
        "LA" => Some("22"),
        "ME" => Some("23"),
        "MD" => Some("24"),
        "MA" => Some("25"),
        "MI" => Some("26"),
        "MN" => Some("27"),
        "MS" => Some("28"),
        "MO" => Some("29"),
        "MT" => Some("30"),
        "NE" => Some("31"),
        "NV" => Some("32"),
        "NH" => Some("33"),
        "NJ" => Some("34"),
        "NM" => Some("35"),
        "NY" => Some("36"),
        "NC" => Some("37"),
        "ND" => Some("38"),
        "OH" => Some("39"),
        "OK" => Some("40"),
        "OR" => Some("41"),
        "PA" => Some("42"),
        "RI" => Some("44"),
        "SC" => Some("45"),
        "SD" => Some("46"),
        "TN" => Some("47"),
        "TX" => Some("48"),
        "UT" => Some("49"),
        "VT" => Some("50"),
        "VA" => Some("51"),
        "WA" => Some("53"),
        "WV" => Some("54"),
        "WI" => Some("55"),
        "WY" => Some("56"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fips_count() {
        assert_eq!(STATE_FIPS.len(), 51);
    }

    #[test]
    fn abbr_roundtrip() {
        for fips in STATE_FIPS {
            let abbr = state_abbr(fips);
            assert_ne!(abbr, "??", "unknown FIPS: {fips}");
            assert_eq!(
                abbr_to_fips(abbr),
                Some(*fips),
                "roundtrip failed for {fips} -> {abbr}"
            );
        }
    }

    #[test]
    fn name_coverage() {
        for fips in STATE_FIPS {
            assert_ne!(state_name(fips), "Unknown", "no name for FIPS: {fips}");
        }
    }

    #[test]
    fn unknown_fips() {
        assert_eq!(state_abbr("99"), "??");
        assert_eq!(state_name("99"), "Unknown");
        assert_eq!(abbr_to_fips("XX"), None);
    }

    #[test]
    fn case_insensitive_abbr_to_fips() {
        assert_eq!(abbr_to_fips("ca"), Some("06"));
        assert_eq!(abbr_to_fips("Ca"), Some("06"));
        assert_eq!(abbr_to_fips("CA"), Some("06"));
    }
}
