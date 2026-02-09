#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions)]

//! Crime category taxonomy types and severity definitions.
//!
//! This crate defines the canonical hierarchical crime type taxonomy used
//! across the entire crime-map system. All data sources normalize their
//! source-specific crime types into this shared taxonomy.

use serde::{Deserialize, Serialize};
use strum_macros::{AsRefStr, Display, EnumString};

/// Severity level for a crime, from 1 (minimal) to 5 (critical).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    AsRefStr,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum CrimeSeverity {
    /// Level 1: Non-criminal or minor offenses
    Minimal = 1,
    /// Level 2: Low-level offenses (petty theft, trespassing)
    Low = 2,
    /// Level 3: Moderate offenses (burglary, drug possession)
    Moderate = 3,
    /// Level 4: Serious offenses (robbery, aggravated assault)
    High = 4,
    /// Level 5: Most severe offenses (homicide, sexual assault)
    Critical = 5,
}

impl CrimeSeverity {
    /// Returns the numeric value of this severity level.
    #[must_use]
    pub const fn value(self) -> u8 {
        self as u8
    }

    /// Creates a severity level from a numeric value.
    ///
    /// # Errors
    ///
    /// Returns an error if the value is not in the range 1-5.
    pub const fn from_value(value: u8) -> Result<Self, InvalidSeverityError> {
        match value {
            1 => Ok(Self::Minimal),
            2 => Ok(Self::Low),
            3 => Ok(Self::Moderate),
            4 => Ok(Self::High),
            5 => Ok(Self::Critical),
            _ => Err(InvalidSeverityError { value }),
        }
    }
}

/// Error returned when attempting to create a [`CrimeSeverity`] from an invalid
/// numeric value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidSeverityError {
    /// The invalid severity value that was provided.
    pub value: u8,
}

impl std::fmt::Display for InvalidSeverityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid severity value {}: expected 1-5", self.value)
    }
}

impl std::error::Error for InvalidSeverityError {}

/// Top-level crime category groupings.
///
/// Each variant maps to a set of more specific subcategories defined in
/// [`CrimeSubcategory`].
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    AsRefStr,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum CrimeCategory {
    /// Crimes against persons (homicide, assault, robbery, sexual assault)
    Violent,
    /// Crimes against property (burglary, theft, arson, vandalism)
    Property,
    /// Drug and narcotics offenses
    DrugNarcotics,
    /// Public order and quality-of-life offenses
    PublicOrder,
    /// Fraud, forgery, embezzlement, identity theft
    FraudFinancial,
    /// Offenses not fitting other categories
    Other,
}

impl CrimeCategory {
    /// Returns the default severity for this top-level category.
    #[must_use]
    pub const fn default_severity(self) -> CrimeSeverity {
        match self {
            Self::Violent => CrimeSeverity::High,
            Self::Property => CrimeSeverity::Moderate,
            Self::DrugNarcotics => CrimeSeverity::Moderate,
            Self::PublicOrder => CrimeSeverity::Low,
            Self::FraudFinancial => CrimeSeverity::Low,
            Self::Other => CrimeSeverity::Minimal,
        }
    }

    /// Returns all variants of this enum.
    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::Violent,
            Self::Property,
            Self::DrugNarcotics,
            Self::PublicOrder,
            Self::FraudFinancial,
            Self::Other,
        ]
    }
}

/// Specific crime subcategories within each top-level [`CrimeCategory`].
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    AsRefStr,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum CrimeSubcategory {
    // ── Violent ──────────────────────────────────────────
    /// Murder and non-negligent manslaughter
    Homicide,
    /// Forcible sexual offenses
    SexualAssault,
    /// Taking property by force or threat
    Robbery,
    /// Attack with intent to cause serious bodily harm
    AggravatedAssault,
    /// Minor physical altercation
    SimpleAssault,

    // ── Property ────────────────────────────────────────
    /// Unlawful entry to commit a felony or theft
    Burglary,
    /// Unlawful taking of property (shoplifting, pickpocket, etc.)
    LarcenyTheft,
    /// Theft of a motor vehicle
    MotorVehicleTheft,
    /// Intentional destruction of property by fire
    Arson,
    /// Willful destruction or damage of property
    Vandalism,

    // ── Drug/Narcotics ──────────────────────────────────
    /// Unlawful possession of controlled substances
    DrugPossession,
    /// Manufacturing, distribution, or sale of controlled substances
    DrugSalesManufacturing,
    /// Possession of drug paraphernalia
    DrugEquipment,

    // ── Public Order ────────────────────────────────────
    /// Unlawful possession or use of weapons
    WeaponsViolation,
    /// Driving under the influence
    Dui,
    /// Disturbing the peace
    DisorderlyConduct,
    /// Unlawful entry onto property
    Trespassing,
    /// Solicitation or related offenses
    Prostitution,

    // ── Fraud/Financial ─────────────────────────────────
    /// Intentional deception for financial gain
    Fraud,
    /// Creating or altering documents with intent to deceive
    Forgery,
    /// Misappropriation of entrusted funds
    Embezzlement,
    /// Unauthorized use of another's identity
    IdentityTheft,

    // ── Other ───────────────────────────────────────────
    /// Reported missing person
    MissingPerson,
    /// Non-criminal incidents (welfare checks, found property, etc.)
    NonCriminal,
    /// Offenses that don't map to any other subcategory
    Unknown,
}

impl CrimeSubcategory {
    /// Returns the parent [`CrimeCategory`] for this subcategory.
    #[must_use]
    pub const fn category(self) -> CrimeCategory {
        match self {
            Self::Homicide
            | Self::SexualAssault
            | Self::Robbery
            | Self::AggravatedAssault
            | Self::SimpleAssault => CrimeCategory::Violent,

            Self::Burglary
            | Self::LarcenyTheft
            | Self::MotorVehicleTheft
            | Self::Arson
            | Self::Vandalism => CrimeCategory::Property,

            Self::DrugPossession | Self::DrugSalesManufacturing | Self::DrugEquipment => {
                CrimeCategory::DrugNarcotics
            }

            Self::WeaponsViolation
            | Self::Dui
            | Self::DisorderlyConduct
            | Self::Trespassing
            | Self::Prostitution => CrimeCategory::PublicOrder,

            Self::Fraud | Self::Forgery | Self::Embezzlement | Self::IdentityTheft => {
                CrimeCategory::FraudFinancial
            }

            Self::MissingPerson | Self::NonCriminal | Self::Unknown => CrimeCategory::Other,
        }
    }

    /// Returns the severity for this specific subcategory.
    #[must_use]
    pub const fn severity(self) -> CrimeSeverity {
        match self {
            Self::Homicide | Self::SexualAssault => CrimeSeverity::Critical,
            Self::Robbery | Self::AggravatedAssault | Self::Arson => CrimeSeverity::High,
            Self::Burglary
            | Self::MotorVehicleTheft
            | Self::DrugSalesManufacturing
            | Self::WeaponsViolation
            | Self::IdentityTheft
            | Self::SimpleAssault => CrimeSeverity::Moderate,
            Self::LarcenyTheft
            | Self::Vandalism
            | Self::DrugPossession
            | Self::Dui
            | Self::Fraud
            | Self::Forgery
            | Self::Embezzlement
            | Self::Prostitution => CrimeSeverity::Low,
            Self::DrugEquipment
            | Self::DisorderlyConduct
            | Self::Trespassing
            | Self::MissingPerson
            | Self::NonCriminal
            | Self::Unknown => CrimeSeverity::Minimal,
        }
    }

    /// Returns all subcategories belonging to the given top-level category.
    #[must_use]
    pub fn for_category(category: CrimeCategory) -> Vec<Self> {
        Self::all()
            .iter()
            .copied()
            .filter(|sub| sub.category() == category)
            .collect()
    }

    /// Returns all variants of this enum.
    #[must_use]
    pub const fn all() -> &'static [Self] {
        &[
            Self::Homicide,
            Self::SexualAssault,
            Self::Robbery,
            Self::AggravatedAssault,
            Self::SimpleAssault,
            Self::Burglary,
            Self::LarcenyTheft,
            Self::MotorVehicleTheft,
            Self::Arson,
            Self::Vandalism,
            Self::DrugPossession,
            Self::DrugSalesManufacturing,
            Self::DrugEquipment,
            Self::WeaponsViolation,
            Self::Dui,
            Self::DisorderlyConduct,
            Self::Trespassing,
            Self::Prostitution,
            Self::Fraud,
            Self::Forgery,
            Self::Embezzlement,
            Self::IdentityTheft,
            Self::MissingPerson,
            Self::NonCriminal,
            Self::Unknown,
        ]
    }
}

/// A row in the `crime_categories` table representing one node in the taxonomy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrimeCategoryRecord {
    /// Database primary key.
    pub id: i32,
    /// Human-readable category name.
    pub name: String,
    /// Parent category ID for subcategories, `None` for top-level categories.
    pub parent_id: Option<i32>,
    /// Severity level (1-5).
    pub severity: u8,
    /// FBI NIBRS offense code, if applicable.
    pub nibrs_code: Option<String>,
    /// UCR Part classification (1 or 2), if applicable.
    pub ucr_part: Option<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subcategory_parent_consistency() {
        for sub in CrimeSubcategory::all() {
            let parent = sub.category();
            let subs = CrimeSubcategory::for_category(parent);
            assert!(
                subs.contains(sub),
                "{sub:?} claims parent {parent:?} but isn't in for_category result"
            );
        }
    }

    #[test]
    fn severity_range_valid() {
        for sub in CrimeSubcategory::all() {
            let val = sub.severity().value();
            assert!(
                (1..=5).contains(&val),
                "{sub:?} severity {val} out of range"
            );
        }
    }

    #[test]
    fn severity_from_value_roundtrip() {
        for v in 1..=5u8 {
            let severity = CrimeSeverity::from_value(v).unwrap();
            assert_eq!(severity.value(), v);
        }
        assert!(CrimeSeverity::from_value(0).is_err());
        assert!(CrimeSeverity::from_value(6).is_err());
    }
}
