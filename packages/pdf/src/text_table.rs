//! Column-position and delimiter-based table extraction from PDF text.
//!
//! Handles two common PDF table layouts:
//! - **Fixed-width columns**: fields are at known character positions
//! - **Delimited lines**: fields are separated by a delimiter character

/// Extracts records from text using fixed column positions.
///
/// Each non-empty line (after skipping `skip_header_lines`) is split at
/// the given `column_starts` positions to produce a JSON object keyed by
/// the corresponding `column_names`.
#[must_use]
pub fn extract(
    text: &str,
    column_starts: &[usize],
    column_names: &[String],
    skip_header_lines: usize,
) -> Vec<serde_json::Value> {
    let mut records = Vec::new();

    for line in text.lines().skip(skip_header_lines) {
        if line.trim().is_empty() {
            continue;
        }

        let mut map = serde_json::Map::new();

        for (i, name) in column_names.iter().enumerate() {
            let start = column_starts.get(i).copied().unwrap_or(0);
            let end = column_starts.get(i + 1).copied().unwrap_or(line.len());

            // Handle lines shorter than expected column positions
            let value = if start < line.len() {
                let actual_end = end.min(line.len());
                line[start..actual_end].trim()
            } else {
                ""
            };

            map.insert(name.clone(), serde_json::Value::String(value.to_owned()));
        }

        records.push(serde_json::Value::Object(map));
    }

    records
}

/// Extracts records from text using a delimiter.
///
/// Each non-empty line (after skipping `skip_header_lines`) is split by
/// `delimiter` to produce a JSON object keyed by `column_names`.
#[must_use]
pub fn extract_delimited(
    text: &str,
    delimiter: &str,
    column_names: &[String],
    skip_header_lines: usize,
) -> Vec<serde_json::Value> {
    let mut records = Vec::new();

    for line in text.lines().skip(skip_header_lines) {
        if line.trim().is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split(delimiter).collect();
        let mut map = serde_json::Map::new();

        for (i, name) in column_names.iter().enumerate() {
            let value = parts.get(i).map_or("", |s| s.trim());
            map.insert(name.clone(), serde_json::Value::String(value.to_owned()));
        }

        records.push(serde_json::Value::Object(map));
    }

    records
}
