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

/// Maximum number of retry attempts for transient HTTP errors
/// (connection failures, timeouts, server errors).
///
/// With exponential backoff (2s, 4s, 8s, 16s, 32s) the total wait
/// before giving up is 62 seconds. Combined with the per-request
/// timeout of 120s this means a worst-case latency of ~12 minutes for
/// a single request (6 attempts × 120s timeout + 62s backoff).
const MAX_RETRIES: u32 = 5;

/// Maximum number of full re-fetch attempts when the response body
/// cannot be decoded (truncated JSON, garbled response, etc.).
///
/// Each body-decode retry goes through [`send_inner`] again, so
/// connection-level retries still apply. Worst case: `(1 + MAX_BODY_RETRIES)`
/// × `(1 + MAX_RETRIES)` = 36 HTTP requests for a single logical call.
const MAX_BODY_RETRIES: u32 = 5;

/// Maximum length of the response body preview included in error logs.
const BODY_PREVIEW_LEN: usize = 500;

/// Sends an HTTP request and parses the response body as JSON.
///
/// The `build_request` closure is called on each attempt to construct a
/// fresh [`reqwest::RequestBuilder`] (since builders are consumed by
/// `.send()`). This allows retrying any request shape — GET, POST,
/// with headers, query params, JSON body, etc.
///
/// # Retry behaviour
///
/// Two layers of retry:
///
/// 1. **Connection-level** ([`send_inner`]): retries up to [`MAX_RETRIES`]
///    times with exponential backoff on connection errors, timeouts,
///    HTTP 429, and HTTP 5xx.
/// 2. **Body-decode**: if the response arrives successfully but the body
///    cannot be parsed as JSON (truncated response, garbled data), the
///    *entire* request is re-fetched up to [`MAX_BODY_RETRIES`] times,
///    each attempt going through the full connection-level retry loop.
///
/// Does **not** retry HTTP 4xx (except 429) — these are permanent.
///
/// # Errors
///
/// Returns [`SourceError`] if the request fails after all retries, the
/// server returns a non-retryable status code, or the response body
/// cannot be parsed as JSON after all body-decode retries.
#[allow(clippy::future_not_send, clippy::too_many_lines)]
pub async fn send_json<F>(build_request: F) -> Result<serde_json::Value, SourceError>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    for body_attempt in 0..=MAX_BODY_RETRIES {
        let response = send_inner(&build_request, MAX_RETRIES).await?;

        // Capture response metadata before consuming the body.
        let url = response.url().to_string();
        let status = response.status();
        let content_length = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let content_encoding = response
            .headers()
            .get(reqwest::header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        // Read the raw body as text first, then parse as JSON.
        // This lets us log the actual response content on failure.
        match response.text().await {
            Ok(text) => match serde_json::from_str(&text) {
                Ok(value) => return Ok(value),
                Err(json_err) => {
                    let preview = if text.len() > BODY_PREVIEW_LEN {
                        format!("{}...", &text[..BODY_PREVIEW_LEN])
                    } else {
                        text.clone()
                    };
                    if body_attempt < MAX_BODY_RETRIES {
                        let delay = Duration::from_secs(1u64 << (body_attempt + 1));
                        log::warn!(
                            "JSON parse failed (body retry {}/{MAX_BODY_RETRIES}), \
                             re-fetching in {delay:?}...\n  \
                             url: {url}\n  \
                             status: {status}\n  \
                             content-length: {content_length:?}\n  \
                             content-type: {content_type:?}\n  \
                             content-encoding: {content_encoding:?}\n  \
                             received: {} bytes\n  \
                             parse error: {json_err}\n  \
                             body preview: {preview}",
                            body_attempt + 1,
                            text.len(),
                        );
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    log::error!(
                        "JSON parse failed after {MAX_BODY_RETRIES} retries, giving up.\n  \
                         url: {url}\n  \
                         status: {status}\n  \
                         content-length: {content_length:?}\n  \
                         content-type: {content_type:?}\n  \
                         content-encoding: {content_encoding:?}\n  \
                         received: {} bytes\n  \
                         parse error: {json_err}\n  \
                         body preview: {preview}",
                        text.len(),
                    );
                    return Err(SourceError::Normalization {
                        message: format!(
                            "JSON parse failed: {json_err} (status={status}, \
                             received {} bytes, content-type={content_type:?})",
                            text.len()
                        ),
                    });
                }
            },
            Err(e) => {
                if body_attempt < MAX_BODY_RETRIES {
                    let delay = Duration::from_secs(1u64 << (body_attempt + 1));
                    log::warn!(
                        "Response body read failed (body retry {}/{MAX_BODY_RETRIES}), \
                         re-fetching in {delay:?}...\n  \
                         url: {url}\n  \
                         status: {status}\n  \
                         content-length: {content_length:?}\n  \
                         content-type: {content_type:?}\n  \
                         content-encoding: {content_encoding:?}\n  \
                         error: {e}",
                        body_attempt + 1,
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                log::error!(
                    "Response body read failed after {MAX_BODY_RETRIES} retries, giving up.\n  \
                     url: {url}\n  \
                     status: {status}\n  \
                     content-length: {content_length:?}\n  \
                     content-type: {content_type:?}\n  \
                     content-encoding: {content_encoding:?}\n  \
                     error: {e}",
                );
                return Err(SourceError::Http(e));
            }
        }
    }

    // Unreachable — the loop always returns via Ok or Err.
    unreachable!("send_json body-decode retry loop exited without returning")
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
/// body cannot be read as text after all body-decode retries.
#[allow(clippy::future_not_send)]
pub async fn send_text<F>(build_request: F) -> Result<String, SourceError>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    for body_attempt in 0..=MAX_BODY_RETRIES {
        let response = send_inner(&build_request, MAX_RETRIES).await?;

        let url = response.url().to_string();
        let status = response.status();
        let content_length = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let content_encoding = response
            .headers()
            .get(reqwest::header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        match response.text().await {
            Ok(text) => return Ok(text),
            Err(e) => {
                if body_attempt < MAX_BODY_RETRIES {
                    let delay = Duration::from_secs(1u64 << (body_attempt + 1));
                    log::warn!(
                        "Text body read failed (body retry {}/{MAX_BODY_RETRIES}), \
                         re-fetching in {delay:?}...\n  \
                         url: {url}\n  \
                         status: {status}\n  \
                         content-length: {content_length:?}\n  \
                         content-encoding: {content_encoding:?}\n  \
                         error: {e}",
                        body_attempt + 1,
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
                log::error!(
                    "Text body read failed after {MAX_BODY_RETRIES} retries, giving up.\n  \
                     url: {url}\n  \
                     status: {status}\n  \
                     content-length: {content_length:?}\n  \
                     content-encoding: {content_encoding:?}\n  \
                     error: {e}",
                );
                return Err(SourceError::Http(e));
            }
        }
    }

    unreachable!("send_text body-decode retry loop exited without returning")
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
