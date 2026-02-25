//! Address normalization for geocoder index.
//!
//! Provides a deterministic normalization pipeline applied symmetrically
//! at index time and query time. This ensures that "100 N STATE ST" and
//! "100 NORTH STATE STREET" produce the same normalized form.

use regex::Regex;
use std::sync::LazyLock;

use crate::synonyms;

/// Regex to strip punctuation characters that do not contribute to
/// address matching.
static PUNCTUATION_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[.,#'/\\\-]+").expect("valid regex"));

/// Regex to collapse multiple whitespace characters into a single space.
static WHITESPACE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\s{2,}").expect("valid regex"));

/// Normalizes an address component (street, city, or full address).
///
/// The pipeline:
/// 1. Uppercase
/// 2. Strip punctuation (`.`, `,`, `#`, `'`, `/`, `\`, `-`)
/// 3. Expand abbreviations (ST→STREET, N→NORTH, etc.)
/// 4. Collapse whitespace
/// 5. Trim
#[must_use]
pub fn normalize(input: &str) -> String {
    let upper = input.to_uppercase();
    let no_punct = PUNCTUATION_RE.replace_all(&upper, " ");
    let tokens: Vec<&str> = no_punct.split_whitespace().collect();

    let expanded: Vec<&str> = tokens.iter().map(|t| synonyms::expand_token(t)).collect();

    let joined = expanded.join(" ");
    WHITESPACE_RE.replace_all(&joined, " ").trim().to_string()
}

/// Normalizes a street address by combining house number and street name.
///
/// If `number` is empty, returns just the normalized street name.
/// If `street` is empty, returns an empty string.
#[must_use]
pub fn normalize_street(number: &str, street: &str) -> String {
    let street = street.trim();
    if street.is_empty() {
        return String::new();
    }

    let number = number.trim();
    if number.is_empty() {
        return normalize(street);
    }

    normalize(&format!("{number} {street}"))
}

/// Normalizes a US state value.
///
/// If it's already a two-letter code, returns it uppercased.
/// Otherwise normalizes it as a regular string.
#[must_use]
pub fn normalize_state(state: &str) -> String {
    let trimmed = state.trim().to_uppercase();
    // US state codes are always 2 letters; pass through as-is
    if trimmed.len() == 2 && trimmed.chars().all(|c| c.is_ascii_alphabetic()) {
        return trimmed;
    }
    normalize(&trimmed)
}

/// Builds the `full_address` composite field for indexing.
///
/// Concatenates street, city, and state into a single normalized string.
#[must_use]
pub fn build_full_address(street: &str, city: &str, state: &str) -> String {
    let parts: Vec<&str> = [street, city, state]
        .iter()
        .copied()
        .filter(|p| !p.is_empty())
        .collect();
    normalize(&parts.join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_abbreviations() {
        assert_eq!(normalize("100 N STATE ST"), "100 NORTH STATE STREET");
    }

    #[test]
    fn normalizes_mixed_case() {
        assert_eq!(normalize("100 n state st"), "100 NORTH STATE STREET");
    }

    #[test]
    fn strips_punctuation() {
        assert_eq!(normalize("100 N. STATE ST."), "100 NORTH STATE STREET");
    }

    #[test]
    fn collapses_whitespace() {
        assert_eq!(normalize("100  N   STATE  ST"), "100 NORTH STATE STREET");
    }

    #[test]
    fn normalizes_boulevard() {
        assert_eq!(
            normalize("500 BLVD OF THE ALLIES"),
            "500 BOULEVARD OF THE ALLIES"
        );
    }

    #[test]
    fn normalizes_street_with_number() {
        assert_eq!(
            normalize_street("100", "N STATE ST"),
            "100 NORTH STATE STREET"
        );
    }

    #[test]
    fn normalizes_street_without_number() {
        assert_eq!(normalize_street("", "N STATE ST"), "NORTH STATE STREET");
    }

    #[test]
    fn normalizes_empty_street() {
        assert_eq!(normalize_street("100", ""), "");
    }

    #[test]
    fn normalizes_state_code() {
        assert_eq!(normalize_state("il"), "IL");
        assert_eq!(normalize_state("IL"), "IL");
        assert_eq!(normalize_state(" dc "), "DC");
    }

    #[test]
    fn builds_full_address() {
        assert_eq!(
            build_full_address("100 NORTH STATE STREET", "CHICAGO", "IL"),
            "100 NORTH STATE STREET CHICAGO IL"
        );
    }

    #[test]
    fn builds_full_address_skips_empty() {
        assert_eq!(
            build_full_address("100 NORTH STATE STREET", "", "IL"),
            "100 NORTH STATE STREET IL"
        );
    }
}
