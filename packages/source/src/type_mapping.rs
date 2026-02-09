//! Crime type mapping utilities.
//!
//! Maps source-specific crime type strings to the canonical
//! [`CrimeSubcategory`] taxonomy. Each data source has different naming
//! conventions, so we use pattern matching and keyword detection to classify.

use crime_map_crime_models::CrimeSubcategory;

/// Attempts to map a raw crime type string from any source to the canonical
/// subcategory.
///
/// This uses keyword-based matching and is case-insensitive. Returns
/// [`CrimeSubcategory::Unknown`] when no mapping can be determined.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn map_crime_type(raw: &str) -> CrimeSubcategory {
    let lower = raw.to_lowercase();

    // ── Violent crimes (check before property since some overlap) ────
    if contains_any(
        &lower,
        &[
            "homicide",
            "murder",
            "manslaughter",
            "killing",
            "negligent homicide",
        ],
    ) {
        return CrimeSubcategory::Homicide;
    }
    if contains_any(
        &lower,
        &[
            "sexual assault",
            "rape",
            "sex offense",
            "criminal sexual",
            "crim sexual",
            "sex abuse",
            "sexual abuse",
            "forcible sex",
            "sodomy",
            "indecent exposure",
        ],
    ) {
        return CrimeSubcategory::SexualAssault;
    }
    if lower.contains("robbery") {
        return CrimeSubcategory::Robbery;
    }
    if contains_any(
        &lower,
        &[
            "aggravated assault",
            "agg assault",
            "assault-aggravated",
            "felony assault",
            "assault w/dangerous weapon",
        ],
    ) {
        return CrimeSubcategory::AggravatedAssault;
    }
    if contains_any(
        &lower,
        &[
            "simple assault",
            "assault",
            "battery",
            "intimidation",
            "harassment",
            "menacing",
        ],
    ) {
        return CrimeSubcategory::SimpleAssault;
    }

    // ── Property crimes ─────────────────────────────────────────────
    if contains_any(
        &lower,
        &[
            "burglary",
            "breaking and entering",
            "break-in",
            "burg",
            "residential burglary",
            "commercial burglary",
        ],
    ) {
        return CrimeSubcategory::Burglary;
    }
    if contains_any(
        &lower,
        &[
            "motor vehicle theft",
            "vehicle theft",
            "auto theft",
            "auto-theft",
            "stolen vehicle",
            "carjacking",
            "vehicle - stolen",
            "grand theft auto",
            "gta",
        ],
    ) {
        return CrimeSubcategory::MotorVehicleTheft;
    }
    if contains_any(
        &lower,
        &[
            "larceny",
            "theft",
            "shoplifting",
            "pickpocket",
            "purse-snatching",
            "stolen property",
            "petit larceny",
            "grand larceny",
            "pocket-picking",
            "purse snatching",
        ],
    ) {
        return CrimeSubcategory::LarcenyTheft;
    }
    if lower.contains("arson") {
        return CrimeSubcategory::Arson;
    }
    if contains_any(
        &lower,
        &[
            "vandalism",
            "criminal damage",
            "criminal mischief",
            "destruction of property",
            "malicious mischief",
            "graffiti",
        ],
    ) {
        return CrimeSubcategory::Vandalism;
    }

    // ── Drug/narcotics ──────────────────────────────────────────────
    if contains_any(
        &lower,
        &[
            "drug sale",
            "drug manufacturing",
            "drug trafficking",
            "narcotics sale",
            "drug distribution",
        ],
    ) {
        return CrimeSubcategory::DrugSalesManufacturing;
    }
    if contains_any(&lower, &["drug equipment", "drug paraphernalia"]) {
        return CrimeSubcategory::DrugEquipment;
    }
    if contains_any(
        &lower,
        &[
            "drug",
            "narcotics",
            "controlled substance",
            "marijuana",
            "cannabis",
            "drug offense",
            "drug violation",
        ],
    ) {
        return CrimeSubcategory::DrugPossession;
    }

    // ── Public order ────────────────────────────────────────────────
    if contains_any(
        &lower,
        &[
            "weapons",
            "weapon",
            "firearm",
            "concealed carry",
            "unlawful use of weapon",
            "dangerous weapons",
        ],
    ) {
        return CrimeSubcategory::WeaponsViolation;
    }
    if contains_any(&lower, &["dui", "dwi", "driving under", "drunk driving"]) {
        return CrimeSubcategory::Dui;
    }
    if contains_any(
        &lower,
        &[
            "disorderly",
            "public intoxication",
            "disturbing the peace",
            "noise violation",
            "liquor law",
        ],
    ) {
        return CrimeSubcategory::DisorderlyConduct;
    }
    if contains_any(&lower, &["trespass", "criminal trespass"]) {
        return CrimeSubcategory::Trespassing;
    }
    if contains_any(&lower, &["prostitution", "solicitation", "sex work"]) {
        return CrimeSubcategory::Prostitution;
    }

    // ── Fraud/financial ─────────────────────────────────────────────
    if contains_any(&lower, &["identity theft", "identity fraud"]) {
        return CrimeSubcategory::IdentityTheft;
    }
    if contains_any(&lower, &["forgery", "counterfeit"]) {
        return CrimeSubcategory::Forgery;
    }
    if lower.contains("embezzlement") {
        return CrimeSubcategory::Embezzlement;
    }
    if contains_any(
        &lower,
        &[
            "fraud",
            "deceptive practice",
            "bad check",
            "wire fraud",
            "credit card",
        ],
    ) {
        return CrimeSubcategory::Fraud;
    }

    // ── Other ───────────────────────────────────────────────────────
    if contains_any(&lower, &["missing person", "missing"]) {
        return CrimeSubcategory::MissingPerson;
    }
    if contains_any(
        &lower,
        &[
            "non-criminal",
            "non criminal",
            "informational",
            "found property",
            "lost property",
            "civil sidewalks",
        ],
    ) {
        return CrimeSubcategory::NonCriminal;
    }

    CrimeSubcategory::Unknown
}

/// Checks if `haystack` contains any of the given `needles`.
fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_chicago_types() {
        assert_eq!(map_crime_type("HOMICIDE"), CrimeSubcategory::Homicide);
        assert_eq!(map_crime_type("BATTERY"), CrimeSubcategory::SimpleAssault);
        assert_eq!(map_crime_type("BURGLARY"), CrimeSubcategory::Burglary);
        assert_eq!(map_crime_type("THEFT"), CrimeSubcategory::LarcenyTheft);
        assert_eq!(
            map_crime_type("MOTOR VEHICLE THEFT"),
            CrimeSubcategory::MotorVehicleTheft
        );
        assert_eq!(
            map_crime_type("NARCOTICS"),
            CrimeSubcategory::DrugPossession
        );
        assert_eq!(
            map_crime_type("CRIMINAL DAMAGE"),
            CrimeSubcategory::Vandalism
        );
        assert_eq!(
            map_crime_type("DECEPTIVE PRACTICE"),
            CrimeSubcategory::Fraud
        );
        assert_eq!(map_crime_type("ROBBERY"), CrimeSubcategory::Robbery);
        assert_eq!(
            map_crime_type("CRIM SEXUAL ASSAULT"),
            CrimeSubcategory::SexualAssault
        );
        assert_eq!(
            map_crime_type("WEAPONS VIOLATION"),
            CrimeSubcategory::WeaponsViolation
        );
    }

    #[test]
    fn unknown_fallback() {
        assert_eq!(
            map_crime_type("SOME_UNRECOGNIZED_TYPE"),
            CrimeSubcategory::Unknown
        );
    }

    #[test]
    fn maps_la_types() {
        assert_eq!(
            map_crime_type("VEHICLE - STOLEN"),
            CrimeSubcategory::MotorVehicleTheft
        );
        assert_eq!(
            map_crime_type("BATTERY - SIMPLE ASSAULT"),
            CrimeSubcategory::SimpleAssault
        );
        assert_eq!(
            map_crime_type("BURGLARY FROM VEHICLE"),
            CrimeSubcategory::Burglary
        );
    }

    #[test]
    fn maps_sf_types() {
        assert_eq!(
            map_crime_type("Drug Offense"),
            CrimeSubcategory::DrugPossession
        );
        assert_eq!(
            map_crime_type("Larceny Theft"),
            CrimeSubcategory::LarcenyTheft
        );
        assert_eq!(
            map_crime_type("Malicious Mischief"),
            CrimeSubcategory::Vandalism
        );
        assert_eq!(
            map_crime_type("Lost Property"),
            CrimeSubcategory::NonCriminal
        );
    }

    #[test]
    fn maps_nyc_types() {
        assert_eq!(
            map_crime_type("PETIT LARCENY"),
            CrimeSubcategory::LarcenyTheft
        );
        assert_eq!(
            map_crime_type("GRAND LARCENY"),
            CrimeSubcategory::LarcenyTheft
        );
        assert_eq!(
            map_crime_type("FELONY ASSAULT"),
            CrimeSubcategory::AggravatedAssault
        );
        assert_eq!(
            map_crime_type("DANGEROUS WEAPONS"),
            CrimeSubcategory::WeaponsViolation
        );
    }

    #[test]
    fn maps_dc_types() {
        assert_eq!(
            map_crime_type("ASSAULT W/DANGEROUS WEAPON"),
            CrimeSubcategory::AggravatedAssault
        );
        assert_eq!(map_crime_type("HOMICIDE"), CrimeSubcategory::Homicide);
        assert_eq!(map_crime_type("ROBBERY"), CrimeSubcategory::Robbery);
    }

    #[test]
    fn maps_denver_types() {
        assert_eq!(
            map_crime_type("auto-theft"),
            CrimeSubcategory::MotorVehicleTheft
        );
        assert_eq!(
            map_crime_type("drug-alcohol"),
            CrimeSubcategory::DrugPossession
        );
        assert_eq!(
            map_crime_type("theft-from-motor-vehicle"),
            CrimeSubcategory::LarcenyTheft
        );
    }
}
