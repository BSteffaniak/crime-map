#![allow(clippy::too_many_lines)]
//! Street type and directional synonym tables.
//!
//! These tables map common abbreviations to their canonical expanded
//! forms. They are applied symmetrically at index time and query time
//! so that "100 N STATE ST" matches "100 NORTH STATE STREET".

use std::collections::BTreeMap;
use std::sync::LazyLock;

/// Maps street type abbreviations to their canonical full form.
///
/// Source: USPS Publication 28 (Appendix C) street suffix abbreviations.
static STREET_TYPES: LazyLock<BTreeMap<&'static str, &'static str>> = LazyLock::new(|| {
    BTreeMap::from([
        ("ALY", "ALLEY"),
        ("ANX", "ANNEX"),
        ("ARC", "ARCADE"),
        ("AVE", "AVENUE"),
        ("AV", "AVENUE"),
        ("BYU", "BAYOU"),
        ("BCH", "BEACH"),
        ("BND", "BEND"),
        ("BLF", "BLUFF"),
        ("BLFS", "BLUFFS"),
        ("BTM", "BOTTOM"),
        ("BLVD", "BOULEVARD"),
        ("BLV", "BOULEVARD"),
        ("BR", "BRANCH"),
        ("BRG", "BRIDGE"),
        ("BRK", "BROOK"),
        ("BRKS", "BROOKS"),
        ("BG", "BURG"),
        ("BGS", "BURGS"),
        ("BYP", "BYPASS"),
        ("CP", "CAMP"),
        ("CYN", "CANYON"),
        ("CPE", "CAPE"),
        ("CSWY", "CAUSEWAY"),
        ("CTR", "CENTER"),
        ("CNTR", "CENTER"),
        ("CIR", "CIRCLE"),
        ("CIRS", "CIRCLES"),
        ("CLF", "CLIFF"),
        ("CLFS", "CLIFFS"),
        ("CLB", "CLUB"),
        ("CMN", "COMMON"),
        ("CMNS", "COMMONS"),
        ("COR", "CORNER"),
        ("CORS", "CORNERS"),
        ("CRSE", "COURSE"),
        ("CT", "COURT"),
        ("CTS", "COURTS"),
        ("CV", "COVE"),
        ("CVS", "COVES"),
        ("CRK", "CREEK"),
        ("CRES", "CRESCENT"),
        ("CRST", "CREST"),
        ("XING", "CROSSING"),
        ("XRD", "CROSSROAD"),
        ("XRDS", "CROSSROADS"),
        ("CURV", "CURVE"),
        ("DL", "DALE"),
        ("DM", "DAM"),
        ("DV", "DIVIDE"),
        ("DR", "DRIVE"),
        ("DRS", "DRIVES"),
        ("EST", "ESTATE"),
        ("ESTS", "ESTATES"),
        ("EXPY", "EXPRESSWAY"),
        ("EXT", "EXTENSION"),
        ("EXTS", "EXTENSIONS"),
        ("FLS", "FALLS"),
        ("FRY", "FERRY"),
        ("FLD", "FIELD"),
        ("FLDS", "FIELDS"),
        ("FLT", "FLAT"),
        ("FLTS", "FLATS"),
        ("FRD", "FORD"),
        ("FRDS", "FORDS"),
        ("FRST", "FOREST"),
        ("FRG", "FORGE"),
        ("FRGS", "FORGES"),
        ("FRK", "FORK"),
        ("FRKS", "FORKS"),
        ("FT", "FORT"),
        ("FWY", "FREEWAY"),
        ("GDN", "GARDEN"),
        ("GDNS", "GARDENS"),
        ("GTWY", "GATEWAY"),
        ("GLN", "GLEN"),
        ("GLNS", "GLENS"),
        ("GRN", "GREEN"),
        ("GRNS", "GREENS"),
        ("GRV", "GROVE"),
        ("GRVS", "GROVES"),
        ("HBR", "HARBOR"),
        ("HBRS", "HARBORS"),
        ("HVN", "HAVEN"),
        ("HTS", "HEIGHTS"),
        ("HWY", "HIGHWAY"),
        ("HL", "HILL"),
        ("HLS", "HILLS"),
        ("HOLW", "HOLLOW"),
        ("INLT", "INLET"),
        ("IS", "ISLAND"),
        ("ISS", "ISLANDS"),
        ("JCT", "JUNCTION"),
        ("JCTS", "JUNCTIONS"),
        ("KY", "KEY"),
        ("KYS", "KEYS"),
        ("KNL", "KNOLL"),
        ("KNLS", "KNOLLS"),
        ("LK", "LAKE"),
        ("LKS", "LAKES"),
        ("LNDG", "LANDING"),
        ("LN", "LANE"),
        ("LGT", "LIGHT"),
        ("LGTS", "LIGHTS"),
        ("LF", "LOAF"),
        ("LCK", "LOCK"),
        ("LCKS", "LOCKS"),
        ("LDG", "LODGE"),
        ("LP", "LOOP"),
        ("MNR", "MANOR"),
        ("MNRS", "MANORS"),
        ("MDW", "MEADOW"),
        ("MDWS", "MEADOWS"),
        ("ML", "MILL"),
        ("MLS", "MILLS"),
        ("MSN", "MISSION"),
        ("MT", "MOUNT"),
        ("MTN", "MOUNTAIN"),
        ("MTNS", "MOUNTAINS"),
        ("NCK", "NECK"),
        ("ORCH", "ORCHARD"),
        ("OVLK", "OVERLOOK"),
        ("PKWY", "PARKWAY"),
        ("PKY", "PARKWAY"),
        ("PKWYS", "PARKWAYS"),
        ("PASS", "PASS"),
        ("PSGE", "PASSAGE"),
        ("PH", "PENTHOUSE"),
        ("PNE", "PINE"),
        ("PNES", "PINES"),
        ("PL", "PLACE"),
        ("PLN", "PLAIN"),
        ("PLNS", "PLAINS"),
        ("PLZ", "PLAZA"),
        ("PT", "POINT"),
        ("PTS", "POINTS"),
        ("PRT", "PORT"),
        ("PRTS", "PORTS"),
        ("PR", "PRAIRIE"),
        ("RADL", "RADIAL"),
        ("RNCH", "RANCH"),
        ("RPD", "RAPID"),
        ("RPDS", "RAPIDS"),
        ("RST", "REST"),
        ("RDG", "RIDGE"),
        ("RDGS", "RIDGES"),
        ("RIV", "RIVER"),
        ("RD", "ROAD"),
        ("RDS", "ROADS"),
        ("RTE", "ROUTE"),
        ("SHL", "SHOAL"),
        ("SHLS", "SHOALS"),
        ("SHR", "SHORE"),
        ("SHRS", "SHORES"),
        ("SKWY", "SKYWAY"),
        ("SPG", "SPRING"),
        ("SPGS", "SPRINGS"),
        ("SQ", "SQUARE"),
        ("SQS", "SQUARES"),
        ("STA", "STATION"),
        ("STRA", "STRAVENUE"),
        ("STRM", "STREAM"),
        ("ST", "STREET"),
        ("STR", "STREET"),
        ("STS", "STREETS"),
        ("SMT", "SUMMIT"),
        ("TER", "TERRACE"),
        ("TRWY", "THROUGHWAY"),
        ("TRCE", "TRACE"),
        ("TRAK", "TRACK"),
        ("TRL", "TRAIL"),
        ("TRLR", "TRAILER"),
        ("TUNL", "TUNNEL"),
        ("TPKE", "TURNPIKE"),
        ("UN", "UNION"),
        ("UNS", "UNIONS"),
        ("VLY", "VALLEY"),
        ("VLYS", "VALLEYS"),
        ("VIA", "VIADUCT"),
        ("VW", "VIEW"),
        ("VWS", "VIEWS"),
        ("VLG", "VILLAGE"),
        ("VLGS", "VILLAGES"),
        ("VL", "VILLE"),
        ("VIS", "VISTA"),
        ("WL", "WELL"),
        ("WLS", "WELLS"),
    ])
});

/// Maps directional abbreviations to their full form.
static DIRECTIONALS: LazyLock<BTreeMap<&'static str, &'static str>> = LazyLock::new(|| {
    BTreeMap::from([
        ("N", "NORTH"),
        ("S", "SOUTH"),
        ("E", "EAST"),
        ("W", "WEST"),
        ("NE", "NORTHEAST"),
        ("NW", "NORTHWEST"),
        ("SE", "SOUTHEAST"),
        ("SW", "SOUTHWEST"),
    ])
});

/// Expands a single token if it matches a known abbreviation.
///
/// Checks directional abbreviations first, then street types.
/// Returns the expanded form or the original token unchanged.
#[must_use]
pub fn expand_token(token: &str) -> &str {
    if let Some(expanded) = DIRECTIONALS.get(token) {
        return expanded;
    }
    if let Some(expanded) = STREET_TYPES.get(token) {
        return expanded;
    }
    token
}

/// Returns `true` if the token is a known street type abbreviation
/// or its canonical expanded form.
#[must_use]
pub fn is_street_type(token: &str) -> bool {
    STREET_TYPES.contains_key(token) || STREET_TYPES.values().any(|canonical| *canonical == token)
}

/// Returns `true` if the token is a known directional abbreviation
/// or its canonical expanded form.
#[must_use]
pub fn is_directional(token: &str) -> bool {
    DIRECTIONALS.contains_key(token) || DIRECTIONALS.values().any(|canonical| *canonical == token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_street_types() {
        assert_eq!(expand_token("ST"), "STREET");
        assert_eq!(expand_token("AVE"), "AVENUE");
        assert_eq!(expand_token("BLVD"), "BOULEVARD");
        assert_eq!(expand_token("DR"), "DRIVE");
        assert_eq!(expand_token("LN"), "LANE");
        assert_eq!(expand_token("CT"), "COURT");
        assert_eq!(expand_token("PL"), "PLACE");
        assert_eq!(expand_token("RD"), "ROAD");
        assert_eq!(expand_token("HWY"), "HIGHWAY");
        assert_eq!(expand_token("PKWY"), "PARKWAY");
    }

    #[test]
    fn expands_directionals() {
        assert_eq!(expand_token("N"), "NORTH");
        assert_eq!(expand_token("S"), "SOUTH");
        assert_eq!(expand_token("E"), "EAST");
        assert_eq!(expand_token("W"), "WEST");
        assert_eq!(expand_token("NE"), "NORTHEAST");
        assert_eq!(expand_token("SW"), "SOUTHWEST");
    }

    #[test]
    fn passes_through_unknown_tokens() {
        assert_eq!(expand_token("MAIN"), "MAIN");
        assert_eq!(expand_token("STATE"), "STATE");
        assert_eq!(expand_token("123"), "123");
    }

    #[test]
    fn identifies_street_types() {
        assert!(is_street_type("ST"));
        assert!(is_street_type("STREET"));
        assert!(is_street_type("AVE"));
        assert!(is_street_type("AVENUE"));
        assert!(!is_street_type("MAIN"));
    }

    #[test]
    fn identifies_directionals() {
        assert!(is_directional("N"));
        assert!(is_directional("NORTH"));
        assert!(is_directional("NE"));
        assert!(is_directional("NORTHEAST"));
        assert!(!is_directional("MAIN"));
    }
}
