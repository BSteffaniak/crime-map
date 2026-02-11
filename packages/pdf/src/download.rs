//! PDF download utilities.
//!
//! Helpers for downloading PDF files from URLs, with support for URL
//! templates that contain date placeholders.

use std::fmt::Write as _;

use chrono::Datelike as _;

/// Expands a URL template by replacing `{date}` placeholders with
/// formatted date strings.
///
/// Generates one URL per month between `start` and `end` (inclusive)
/// using the given `date_format` (e.g. `"%Y-%m"` for `"2024-01"`).
///
/// # Example
///
/// ```
/// use crime_map_pdf::download::expand_date_template;
/// use chrono::NaiveDate;
///
/// let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
/// let end = NaiveDate::from_ymd_opt(2024, 3, 1).unwrap();
///
/// let urls = expand_date_template(
///     "https://police.gov/bulletins/{date}.pdf",
///     start,
///     end,
///     "%Y-%m",
/// );
///
/// assert_eq!(urls, vec![
///     "https://police.gov/bulletins/2024-01.pdf",
///     "https://police.gov/bulletins/2024-02.pdf",
///     "https://police.gov/bulletins/2024-03.pdf",
/// ]);
/// ```
#[must_use]
pub fn expand_date_template(
    template: &str,
    start: chrono::NaiveDate,
    end: chrono::NaiveDate,
    date_format: &str,
) -> Vec<String> {
    let mut urls = Vec::new();
    let mut current = start;

    while current <= end {
        let mut formatted = String::new();
        write!(formatted, "{}", current.format(date_format)).unwrap();
        urls.push(template.replace("{date}", &formatted));

        // Advance to first day of next month
        current = if current.month() == 12 {
            chrono::NaiveDate::from_ymd_opt(current.year() + 1, 1, 1)
        } else {
            chrono::NaiveDate::from_ymd_opt(current.year(), current.month() + 1, 1)
        }
        .unwrap_or(end + chrono::Duration::days(1)); // break the loop if date overflow
    }

    urls
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn expands_monthly_template() {
        let start = NaiveDate::from_ymd_opt(2024, 10, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();

        let urls = expand_date_template("https://example.com/{date}.pdf", start, end, "%Y-%m");

        assert_eq!(
            urls,
            vec![
                "https://example.com/2024-10.pdf",
                "https://example.com/2024-11.pdf",
                "https://example.com/2024-12.pdf",
                "https://example.com/2025-01.pdf",
            ]
        );
    }
}
