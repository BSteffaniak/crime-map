//! Generic paginated JSON API fetcher bridge.
//!
//! Wraps [`crime_map_scraper::json_paginated::JsonPaginatedScraper`] to
//! stream pages through the ingest pipeline's [`tokio::sync::mpsc`]
//! channel.

use std::collections::BTreeMap;
use std::sync::Arc;

use crime_map_scraper::Scraper;
use crime_map_scraper::json_paginated::{JsonPaginatedScraper, PaginationType, ResponseFormat};
use tokio::sync::mpsc;

use crate::progress::ProgressCallback;
use crate::{FetchOptions, SourceError};

/// Configuration for the generic paginated JSON fetcher.
pub struct JsonPaginatedConfig<'a> {
    /// Base API URL.
    pub api_url: &'a str,
    /// Human-readable label for log messages.
    pub label: &'a str,
    /// Pagination strategy: `"offset"`, `"page"`, or `"cursor"`.
    pub pagination: &'a str,
    /// Response format: `"bare_array"` or `"wrapped"`.
    pub response_format: Option<&'a str>,
    /// Dot-separated path to records array (for wrapped responses).
    pub records_path: Option<&'a str>,
    /// Records per page.
    pub page_size: u64,
    /// Override for the pagination query parameter name.
    pub page_param: Option<&'a str>,
    /// Override for the page-size query parameter name.
    pub size_param: Option<&'a str>,
    /// Delay between page fetches in milliseconds.
    pub delay_ms: Option<u64>,
    /// Additional HTTP headers.
    pub headers: &'a BTreeMap<String, String>,
}

/// Fetches records from a paginated JSON API and sends them through the
/// channel.
///
/// # Errors
///
/// Returns [`SourceError`] if any page fetch fails.
pub async fn fetch_json_paginated(
    config: &JsonPaginatedConfig<'_>,
    options: &FetchOptions,
    tx: &mpsc::Sender<Vec<serde_json::Value>>,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<u64, SourceError> {
    let pagination_param = config.page_param.unwrap_or(match config.pagination {
        "page" => "page",
        "cursor" => "cursor",
        // "offset" and anything else defaults to "offset"
        _ => "offset",
    });

    let pagination = match config.pagination {
        "page" => PaginationType::Page {
            param: pagination_param.to_owned(),
        },
        "cursor" => PaginationType::Cursor {
            param: pagination_param.to_owned(),
        },
        _ => PaginationType::Offset {
            param: pagination_param.to_owned(),
        },
    };

    let mut scraper = JsonPaginatedScraper::new(config.api_url)
        .with_pagination(pagination)
        .with_page_size(u32::try_from(config.page_size).unwrap_or(u32::MAX));

    if let Some(format) = config.response_format {
        let resp_format = if format == "wrapped" {
            ResponseFormat::Wrapped {
                data_path: config.records_path.unwrap_or("data").to_owned(),
            }
        } else {
            ResponseFormat::BareArray
        };
        scraper = scraper.with_response_format(resp_format);
    }

    if let Some(param) = config.size_param {
        scraper = scraper.with_page_size_param(param);
    }

    if let Some(ms) = config.delay_ms {
        scraper = scraper.with_delay_ms(ms);
    }

    for (key, value) in config.headers {
        scraper = scraper.with_header(key, value);
    }

    log::info!(
        "[{}] Fetching paginated JSON from {} (pagination={})",
        config.label,
        config.api_url,
        config.pagination
    );

    let mut total: u64 = 0;
    let mut page_num: u32 = 0;

    loop {
        let page = scraper.fetch_page(page_num).await?;
        let count = page.records.len() as u64;
        total += count;
        progress.inc(count);

        if !page.records.is_empty() {
            tx.send(page.records).await.ok();
        }

        log::info!(
            "[{}] Page {page_num}: {count} records (total: {total})",
            config.label
        );

        if !page.has_more || count == 0 {
            break;
        }

        if let Some(limit) = options.limit
            && total >= limit
        {
            log::info!("[{}] Reached limit of {limit} records", config.label);
            break;
        }

        page_num += 1;
    }

    Ok(total)
}
