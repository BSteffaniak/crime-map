//! HTTP retry helpers for transient errors.
//!
//! All data source fetchers should use [`send_json`] or [`send_text`]
//! instead of calling `reqwest::RequestBuilder::send()` directly. This
//! ensures every HTTP request gets automatic retry with exponential
//! backoff for transient failures (timeouts, connection resets, server
//! errors, rate limiting).
//!
//! # Usage
//!
//! ```ignore
//! use crate::retry;
//!
//! // Simple GET → JSON
//! let body = retry::send_json(|| client.get(&url)).await?;
//!
//! // GET with query params
//! let body = retry::send_json(|| client.get(&url).query(&params)).await?;
//!
//! // POST with JSON body
//! let body = retry::send_json(|| client.post(&url).json(&payload)).await?;
//!
//! // GET → text (HTML, CSV, etc.)
//! let html = retry::send_text(|| client.get(&url)).await?;
//! ```

use std::time::Duration;

use crate::SourceError;

/// Maximum number of retry attempts for transient HTTP errors.
///
/// With exponential backoff (2s, 4s, 8s) the total wait before giving
/// up is 14 seconds. Combined with the per-request timeout of 120s
/// this means a worst-case latency of ~8 minutes for a single request
/// (4 attempts × 120s timeout + 14s backoff).
const MAX_RETRIES: u32 = 3;

/// Sends an HTTP request and parses the response body as JSON.
///
/// The `build_request` closure is called on each attempt to construct a
/// fresh [`reqwest::RequestBuilder`] (since builders are consumed by
/// `.send()`). This allows retrying any request shape — GET, POST,
/// with headers, query params, JSON body, etc.
///
/// # Retry behaviour
///
/// Retries up to [`MAX_RETRIES`] times with exponential backoff on:
/// - Connection errors and timeouts
/// - Response body decode errors
/// - HTTP 429 (Too Many Requests)
/// - HTTP 5xx (Server Error)
///
/// Does **not** retry HTTP 4xx (except 429) — these are permanent.
///
/// # Errors
///
/// Returns [`SourceError`] if the request fails after all retries, the
/// server returns a non-retryable status code, or the response body
/// cannot be parsed as JSON.
#[allow(clippy::future_not_send)]
pub async fn send_json<F>(build_request: F) -> Result<serde_json::Value, SourceError>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    let response = send_inner(&build_request, MAX_RETRIES).await?;

    // Parse body with one extra retry — the server may have sent a
    // truncated response even though the status was 200.
    match response.json().await {
        Ok(value) => Ok(value),
        Err(first_err) => {
            log::warn!("JSON decode failed ({first_err}), retrying request once more...");
            tokio::time::sleep(Duration::from_secs(2)).await;

            let retry_response = build_request().send().await.map_err(SourceError::Http)?;

            let status = retry_response.status();
            if !status.is_success() {
                return Err(SourceError::Normalization {
                    message: format!("HTTP {status} on body-decode retry"),
                });
            }

            Ok(retry_response.json().await?)
        }
    }
}

/// Sends an HTTP request and returns the response body as a `String`.
///
/// Behaves identically to [`send_json`] but returns raw text instead of
/// parsed JSON. Useful for HTML pages, CSV downloads, and other
/// non-JSON responses.
///
/// # Errors
///
/// Returns [`SourceError`] if the request fails after all retries or the
/// body cannot be read as text.
#[allow(clippy::future_not_send)]
pub async fn send_text<F>(build_request: F) -> Result<String, SourceError>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    let response = send_inner(&build_request, MAX_RETRIES).await?;

    match response.text().await {
        Ok(text) => Ok(text),
        Err(first_err) => {
            log::warn!("Text decode failed ({first_err}), retrying request once more...");
            tokio::time::sleep(Duration::from_secs(2)).await;

            let retry_response = build_request().send().await.map_err(SourceError::Http)?;

            let status = retry_response.status();
            if !status.is_success() {
                return Err(SourceError::Normalization {
                    message: format!("HTTP {status} on body-decode retry"),
                });
            }

            Ok(retry_response.text().await.map_err(SourceError::Http)?)
        }
    }
}

/// Core retry loop shared by [`send_json`] and [`send_text`].
///
/// Sends the request built by `build_request`, retrying on transient
/// errors up to `max_retries` times with exponential backoff. Returns
/// the successful [`reqwest::Response`] (status 2xx or 3xx).
#[allow(clippy::future_not_send)]
async fn send_inner<F>(
    build_request: &F,
    max_retries: u32,
) -> Result<reqwest::Response, SourceError>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    let mut last_error: Option<SourceError> = None;

    for attempt in 0..=max_retries {
        if attempt > 0 {
            let delay = Duration::from_secs(1u64 << attempt); // 2s, 4s, 8s
            log::warn!("  retry {attempt}/{max_retries} in {delay:?}...");
            tokio::time::sleep(delay).await;
        }

        let result = build_request().send().await;

        match result {
            Err(e) => {
                if is_transient(&e) && attempt < max_retries {
                    log::warn!("  transient error: {e}");
                    last_error = Some(SourceError::Http(e));
                    continue;
                }
                return Err(SourceError::Http(e));
            }
            Ok(response) => {
                let status = response.status();

                // 429 Too Many Requests — always retry
                if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    if attempt < max_retries {
                        log::warn!("  HTTP 429 (rate limited)");
                        last_error = Some(SourceError::Normalization {
                            message: format!("HTTP {status}"),
                        });
                        continue;
                    }
                    return Err(SourceError::Normalization {
                        message: format!("HTTP {status} after {max_retries} retries"),
                    });
                }

                // 5xx Server Error — retry
                if status.is_server_error() {
                    if attempt < max_retries {
                        log::warn!("  HTTP {status} (server error)");
                        last_error = Some(SourceError::Normalization {
                            message: format!("HTTP {status}"),
                        });
                        continue;
                    }
                    return Err(SourceError::Normalization {
                        message: format!("HTTP {status} after {max_retries} retries"),
                    });
                }

                // 4xx Client Error (not 429) — permanent, don't retry
                if status.is_client_error() {
                    return Err(SourceError::Normalization {
                        message: format!("HTTP {status}"),
                    });
                }

                return Ok(response);
            }
        }
    }

    // Should be unreachable, but in case the loop exits without returning:
    Err(last_error.unwrap_or_else(|| SourceError::Normalization {
        message: "request failed after all retries".to_string(),
    }))
}

/// Returns `true` if the error is likely transient and worth retrying.
fn is_transient(e: &reqwest::Error) -> bool {
    e.is_timeout() || e.is_connect() || e.is_body() || e.is_decode() || e.is_request()
}
