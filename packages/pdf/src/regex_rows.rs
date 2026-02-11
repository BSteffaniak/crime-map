//! Regex-based row extraction from PDF text.
//!
//! Applies a regex with named capture groups against the full PDF text.
//! Each match becomes a JSON object keyed by the group names.

use crate::PdfError;

/// Extracts records from `text` using a regex with named capture groups.
///
/// # Errors
///
/// Returns [`PdfError::Regex`] if the pattern fails to compile.
pub fn extract(text: &str, pattern: &str) -> Result<Vec<serde_json::Value>, PdfError> {
    let re = regex::Regex::new(pattern)?;

    let group_names: Vec<&str> = re
        .capture_names()
        .flatten() // skip the implicit whole-match group (None)
        .collect();

    let mut records = Vec::new();

    for caps in re.captures_iter(text) {
        let mut map = serde_json::Map::new();
        for &name in &group_names {
            let value = caps
                .name(name)
                .map(|m| m.as_str().trim())
                .unwrap_or_default();
            map.insert(name.to_owned(), serde_json::Value::String(value.to_owned()));
        }
        records.push(serde_json::Value::Object(map));
    }

    log::debug!(
        "Regex pattern matched {} records ({} named groups)",
        records.len(),
        group_names.len()
    );

    Ok(records)
}
