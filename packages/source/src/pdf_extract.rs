//! PDF table extraction fetcher bridge.
//!
//! Wraps [`crime_map_pdf::PdfScraper`] to stream pages through the ingest
//! pipeline's [`tokio::sync::mpsc`] channel.

use std::sync::Arc;

use crime_map_pdf::{ExtractionStrategy, PdfScraper};
use crime_map_scraper::Scraper;
use tokio::sync::mpsc;

use crate::progress::ProgressCallback;
use crate::{FetchOptions, SourceError};

/// Configuration for the PDF extraction fetcher.
pub struct PdfExtractConfig<'a> {
    /// URLs of PDF files to download.
    pub urls: &'a [String],
    /// Human-readable label for log messages.
    pub label: &'a str,
    /// Extraction strategy name: `"regex_rows"`, `"text_table"`, or
    /// `"line_delimited"`.
    pub extraction_strategy: &'a str,
    /// Regex pattern (for `regex_rows` strategy).
    pub row_pattern: Option<&'a str>,
    /// Column start positions (for `text_table` strategy).
    pub column_boundaries: Option<&'a [u32]>,
    /// Column names (for `text_table` and `line_delimited` strategies).
    pub column_names: Option<&'a [String]>,
    /// Field delimiter (for `line_delimited` strategy).
    pub delimiter: Option<&'a str>,
    /// Number of header lines to skip.
    pub skip_header_lines: Option<usize>,
}

/// Fetches records from PDF files and sends them through the channel.
///
/// # Errors
///
/// Returns [`SourceError`] if any PDF download or extraction fails.
pub async fn fetch_pdf_extract(
    config: &PdfExtractConfig<'_>,
    options: &FetchOptions,
    tx: &mpsc::Sender<Vec<serde_json::Value>>,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<u64, SourceError> {
    let strategy = match config.extraction_strategy {
        "regex_rows" => ExtractionStrategy::RegexRows {
            pattern: config.row_pattern.unwrap_or(r"(?P<line>.+)").to_owned(),
        },
        "text_table" => ExtractionStrategy::TextTable {
            column_starts: config
                .column_boundaries
                .map(|b| b.iter().map(|&x| x as usize).collect())
                .unwrap_or_default(),
            column_names: config
                .column_names
                .map(<[String]>::to_vec)
                .unwrap_or_default(),
            skip_header_lines: config.skip_header_lines.unwrap_or(1),
        },
        "line_delimited" => ExtractionStrategy::LineDelimited {
            delimiter: config.delimiter.unwrap_or("|").to_owned(),
            column_names: config
                .column_names
                .map(<[String]>::to_vec)
                .unwrap_or_default(),
            skip_header_lines: config.skip_header_lines.unwrap_or(1),
        },
        other => {
            return Err(SourceError::Normalization {
                message: format!("unknown PDF extraction strategy: {other}"),
            });
        }
    };

    let scraper = PdfScraper::new(config.urls.to_vec(), strategy);

    log::info!(
        "[{}] Extracting records from {} PDF(s) using {} strategy",
        config.label,
        config.urls.len(),
        config.extraction_strategy
    );

    let mut total: u64 = 0;

    for page_num in 0..config.urls.len() {
        let page = scraper
            .fetch_page(u32::try_from(page_num).unwrap_or(u32::MAX))
            .await?;
        let count = page.records.len() as u64;
        total += count;
        progress.inc(count);

        if !page.records.is_empty() {
            tx.send(page.records).await.ok();
        }

        log::info!(
            "[{}] PDF {}/{}: {} records (total: {total})",
            config.label,
            page_num + 1,
            config.urls.len(),
            count
        );

        if let Some(limit) = options.limit
            && total >= limit
        {
            log::info!("[{}] Reached limit of {limit} records", config.label);
            break;
        }
    }

    log::info!(
        "[{}] PDF extraction complete — {total} records",
        config.label
    );
    progress.finish(format!(
        "[{}] download complete -- {total} records",
        config.label
    ));

    Ok(total)
}
