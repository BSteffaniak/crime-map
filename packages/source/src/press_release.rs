//! Press-release / news-bulletin scraper for police department websites.
//!
//! Designed for agencies that publish structured crime incident reports as
//! individual web pages (e.g., Anne Arundel County PD publishes daily press
//! releases on a Drupal site with paginated listing pages).
//!
//! The scraper:
//! 1. Crawls paginated index pages to collect press release URLs
//! 2. Fetches each individual press release page
//! 3. Parses structured incident data from the HTML (district, crime type,
//!    case number, narrative with date/time/location)
//! 4. Emits one JSON record per incident, suitable for field mapping

use std::sync::Arc;

use regex::Regex;
use scraper::{Html, Selector};
use tokio::sync::mpsc;

use crate::progress::ProgressCallback;
use crate::{FetchOptions, SourceError};

/// Maximum number of listing pages to crawl when no `since` filter.
const MAX_LISTING_PAGES: u32 = 70;

/// Delay between HTTP requests to avoid hammering the server (ms).
const REQUEST_DELAY_MS: u64 = 200;

/// Configuration for a press-release scraper.
pub struct PressReleaseConfig<'a> {
    /// Base URL of the press release listing page (e.g.,
    /// `https://www.aacounty.org/police-department/about-us/news`).
    pub listing_url: &'a str,
    /// Base domain for resolving relative URLs (e.g.,
    /// `https://www.aacounty.org`).
    pub base_url: &'a str,
    /// CSS selector for links to individual press release pages on the
    /// listing page.
    pub link_selector: &'a str,
    /// URL substring filter — only follow links containing this string.
    pub link_filter: &'a str,
    /// CSS selector for the article body content on individual pages.
    pub article_selector: &'a str,
    /// Pagination query parameter name (default `"page"`).
    pub page_param: &'a str,
    /// Maximum listing pages to crawl.
    pub max_pages: u32,
    /// Parse mode: `"structured"` (default, Anne Arundel style) or
    /// `"drupal_narrative"` (Howard County style, single-incident prose).
    pub parse_mode: &'a str,
    /// Label for log messages.
    pub label: &'a str,
}

/// A single parsed incident from a press release page.
#[derive(Debug)]
struct ParsedIncident {
    /// Police district (e.g., "Southern District").
    district: String,
    /// Crime type headline (e.g., "Commercial Armed Robbery - Brooklyn Park").
    crime_type: String,
    /// Case number (e.g., "26-705422").
    case_number: String,
    /// Full narrative text.
    narrative: String,
    /// Date extracted from the narrative (ISO-8601 or free text).
    date: Option<String>,
    /// Address extracted from the narrative.
    address: Option<String>,
    /// City/neighborhood extracted from the narrative.
    city: Option<String>,
}

/// Extract a date from a narrative paragraph. Looks for patterns like:
/// "On February 20, 2026, at 2:05 a.m."
/// "On February 20, 2026, at approximately 1:10 a.m."
/// "at approximately 3:30 a.m. on Dec. 19"
/// "on Dec. 19, 2025"
fn extract_date_from_narrative(narrative: &str) -> Option<String> {
    // Full month names and abbreviations
    let month_pattern = r"(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan\.?|Feb\.?|Mar\.?|Apr\.?|May\.?|Jun\.?|Jul\.?|Aug\.?|Sep\.?|Sept\.?|Oct\.?|Nov\.?|Dec\.?)";

    // Pattern 1: "On MONTH DAY, YEAR, at TIME am/pm"
    let re1 = Regex::new(
        &format!(r"(?i)On\s+({month_pattern})\s+(\d{{1,2}}),\s+(\d{{4}}),?\s+at\s+(?:approximately\s+)?(\d{{1,2}}:\d{{2}})\s*(a\.m\.|p\.m\.|am|pm)")
    ).ok()?;

    if let Some(caps) = re1.captures(narrative) {
        let month = parse_month_name(&caps[1])?;
        let day: u32 = caps[2].parse().ok()?;
        let year: u32 = caps[3].parse().ok()?;
        let (hour, minute) = parse_time_ampm(&caps[4], &caps[5])?;
        return Some(format!(
            "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:00"
        ));
    }

    // Pattern 2: "at TIME am/pm on MONTH DAY" (reversed, common in HC press releases)
    let re2 = Regex::new(
        &format!(r"(?i)at\s+(?:approximately\s+)?(\d{{1,2}}:\d{{2}})\s*(a\.m\.|p\.m\.|am|pm)\s+on\s+({month_pattern})\s+(\d{{1,2}})(?:,\s*(\d{{4}}))?")
    ).ok()?;

    if let Some(caps) = re2.captures(narrative) {
        let month = parse_month_name(&caps[3])?;
        let day: u32 = caps[4].parse().ok()?;
        // Year may be absent; try to extract from elsewhere in the text
        let year: u32 = caps
            .get(5)
            .and_then(|m| m.as_str().parse().ok())
            .or_else(|| extract_year_from_text(narrative))?;
        let (hour, minute) = parse_time_ampm(&caps[1], &caps[2])?;
        return Some(format!(
            "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:00"
        ));
    }

    // Pattern 3: "on MONTH DAY, YEAR" (date only, no time)
    let re3 = Regex::new(&format!(
        r"(?i)on\s+({month_pattern})\s+(\d{{1,2}}),?\s+(\d{{4}})"
    ))
    .ok()?;

    if let Some(caps) = re3.captures(narrative) {
        let month = parse_month_name(&caps[1])?;
        let day: u32 = caps[2].parse().ok()?;
        let year: u32 = caps[3].parse().ok()?;
        return Some(format!("{year:04}-{month:02}-{day:02}T00:00:00"));
    }

    None
}

/// Parse a month name (full or abbreviated) to its numeric value.
fn parse_month_name(s: &str) -> Option<u32> {
    let normalized = s.to_lowercase().replace('.', "");
    match normalized.as_str() {
        "january" | "jan" => Some(1),
        "february" | "feb" => Some(2),
        "march" | "mar" => Some(3),
        "april" | "apr" => Some(4),
        "may" => Some(5),
        "june" | "jun" => Some(6),
        "july" | "jul" => Some(7),
        "august" | "aug" => Some(8),
        "september" | "sep" | "sept" => Some(9),
        "october" | "oct" => Some(10),
        "november" | "nov" => Some(11),
        "december" | "dec" => Some(12),
        _ => None,
    }
}

/// Parse a time string like "3:30" with an AM/PM indicator.
fn parse_time_ampm(time_str: &str, ampm_str: &str) -> Option<(u32, u32)> {
    let parts: Vec<&str> = time_str.split(':').collect();
    let mut hour: u32 = parts.first()?.parse().ok()?;
    let minute: u32 = parts.get(1)?.parse().ok()?;
    let ampm = ampm_str.to_lowercase().replace('.', "");

    if ampm == "pm" && hour != 12 {
        hour += 12;
    } else if ampm == "am" && hour == 12 {
        hour = 0;
    }
    Some((hour, minute))
}

/// Extract a four-digit year from narrative text as a fallback.
fn extract_year_from_text(text: &str) -> Option<u32> {
    let re = Regex::new(r"\b(20\d{2})\b").ok()?;
    re.captures(text).and_then(|caps| caps[1].parse().ok())
}

/// Extract an address from a narrative. Looks for patterns like:
/// "the 4000 block of Ritchie Highway in Brooklyn Park"
/// "in the 600 block of Admiral Drive in Annapolis"
/// "the 3400 block of Huntsmans Run for a report"
/// "on Solomons Island Road near Poplar Point Road"
fn extract_address_from_narrative(narrative: &str) -> (Option<String>, Option<String>) {
    // Pattern 1: "the N block of STREET in CITY" (with city)
    let block_city_re = Regex::new(
        r"(?i)(?:in\s+)?the\s+(\d+)\s+block\s+of\s+([A-Z][A-Za-z'\s]+?)\s+in\s+([A-Z][A-Za-z\s]+?)(?:\s+for|\s+when|\s+and|\s+where|\s*[,.])"
    ).ok();

    if let Some(re) = &block_city_re
        && let Some(caps) = re.captures(narrative)
    {
        let block = &caps[1];
        let street = caps[2].trim();
        let city = caps[3].trim();
        return (
            Some(format!("{block} block of {street}")),
            Some(city.to_string()),
        );
    }

    // Pattern 2: "the N block of STREET" (without city — stop at prepositions,
    // punctuation, or common narrative words)
    let block_re = Regex::new(
        r"(?i)(?:in\s+)?the\s+(\d+)\s+block\s+of\s+([A-Z][A-Za-z'\s]+?)(?:\s+(?:for|when|and|where|after|that|who|which|he|she|they|police|a\s|an\s|the\s)|\s*[,.;])"
    ).ok();

    if let Some(re) = &block_re
        && let Some(caps) = re.captures(narrative)
    {
        let block = &caps[1];
        let street = caps[2].trim();
        // Only accept if the street name looks valid (at least 2 words or ends
        // with a known suffix, or is a proper name like "Huntsmans Run")
        if street.len() >= 3 {
            return (Some(format!("{block} block of {street}")), None);
        }
    }

    // Pattern 3: "on STREET near STREET" or "on STREET in CITY"
    let on_re = Regex::new(
        r"(?i)on\s+([A-Z][A-Za-z'\s]+?(?:Road|Street|Highway|Boulevard|Avenue|Drive|Lane|Way|Place|Court|Circle|Pike|Terrace|Parkway|Trail|Run|Path|Point))\s+(?:near|in)\s+([A-Z][A-Za-z\s]+?)(?:\s+for|\s+when|\s+and|\s*[,.])"
    ).ok();

    if let Some(re) = &on_re
        && let Some(caps) = re.captures(narrative)
    {
        let street = caps[1].trim();
        let location = caps[2].trim();
        return (Some(street.to_string()), Some(location.to_string()));
    }

    // Pattern 4: "a residence/business/location in CITY" (common in HC press
    // releases that don't give a specific address)
    let location_in_re = Regex::new(
        r"(?i)(?:a\s+)?(?:residence|home|business|apartment|hotel|motel|parking\s+lot|location)\s+in\s+([A-Z][A-Za-z\s]+?)(?:\s+for|\s+when|\s+and|\s+where|\s*[,.])"
    ).ok();

    if let Some(re) = &location_in_re
        && let Some(caps) = re.captures(narrative)
    {
        let city = caps[1].trim();
        return (None, Some(city.to_string()));
    }

    (None, None)
}

/// Parse a Drupal narrative press release page (Howard County style) into
/// zero or one incidents.
///
/// These pages contain a single incident described in prose, with a title,
/// a `<time>` element, and body paragraphs.
fn parse_drupal_narrative(html: &str) -> Vec<ParsedIncident> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let document = Html::parse_document(html);

    // ── Title ────────────────────────────────────────────────────────
    let title = {
        let hero_title_sel = Selector::parse("h1.hero__title").ok();
        let h1_sel = Selector::parse("h1").ok();

        hero_title_sel
            .as_ref()
            .and_then(|sel| document.select(sel).next())
            .or_else(|| h1_sel.as_ref().and_then(|sel| document.select(sel).next()))
            .map(|el| el.text().collect::<String>().trim().to_string())
            .unwrap_or_default()
    };

    if title.is_empty() {
        return Vec::new();
    }

    // ── Date from <time datetime="..."> ──────────────────────────────
    let date_str = {
        let time_sel = Selector::parse("time[datetime]").ok();
        time_sel
            .as_ref()
            .and_then(|sel| document.select(sel).next())
            .and_then(|el| el.value().attr("datetime").map(String::from))
    };

    // ── Body text ────────────────────────────────────────────────────
    let body_text = {
        let content_sel = Selector::parse(".field--name-field-content p").ok();
        let fallback_sel = Selector::parse(".paragraph--rich-text p").ok();

        let paragraphs: Vec<String> = content_sel
            .as_ref()
            .map(|sel| {
                document
                    .select(sel)
                    .map(|p| p.text().collect::<String>().trim().replace('\u{a0}', " "))
                    .filter(|t| !t.is_empty())
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .or_else(|| {
                fallback_sel.as_ref().map(|sel| {
                    document
                        .select(sel)
                        .map(|p| p.text().collect::<String>().trim().replace('\u{a0}', " "))
                        .filter(|t| !t.is_empty())
                        .collect::<Vec<_>>()
                })
            })
            .unwrap_or_default();

        paragraphs.join("\n")
    };

    if body_text.is_empty() {
        return Vec::new();
    }

    // ── Synthetic case number from hash of title + date ──────────────
    let case_number = {
        let mut hasher = DefaultHasher::new();
        title.hash(&mut hasher);
        if let Some(ref d) = date_str {
            d.hash(&mut hasher);
        }
        let hash_val = hasher.finish();
        format!("HC-{hash_val:016x}")[..11].to_string()
    };

    // ── Crime type from title ────────────────────────────────────────
    let crime_type = title.clone();

    // ── Address / city from narrative ─────────────────────────────────
    let (address, city) = extract_address_from_narrative(&body_text);

    // Also try to find the city in the title if not found in narrative
    let city = city.or_else(|| {
        // Look for "in CityName" at end of title — use greedy .* to match
        // the LAST "in" before the city name (e.g., "Suspect in custody in
        // domestic fatal stabbing in Ellicott City" -> "Ellicott City")
        let re = Regex::new(r"(?i).*\bin\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*)\s*$").ok()?;
        re.captures(&title).map(|caps| caps[1].trim().to_string())
    });

    // ── Date: prefer <time> attribute, fall back to narrative ─────────
    let date = date_str.or_else(|| extract_date_from_narrative(&body_text));

    vec![ParsedIncident {
        district: String::new(),
        crime_type,
        case_number,
        narrative: body_text,
        date,
        address,
        city,
    }]
}

/// Parse a single press release page into zero or more incidents.
///
/// The expected HTML structure (Anne Arundel County PD):
/// ```text
/// <article>
///   <h2>District Name</h2>
///   <h3>Crime Type - Location</h3>
///   <h3>Case Number</h3>
///   <p>Narrative paragraph(s)...</p>
///   <hr>
///   (next incident...)
/// </article>
/// ```
fn parse_press_release(html: &str, article_selector_str: &str) -> Vec<ParsedIncident> {
    let document = Html::parse_document(html);
    let mut incidents = Vec::new();

    let Ok(article_sel) = Selector::parse(article_selector_str) else {
        return incidents;
    };

    let h2_sel = Selector::parse("h2").unwrap_or_else(|_| unreachable!());
    let h3_sel = Selector::parse("h3").unwrap_or_else(|_| unreachable!());
    let p_sel = Selector::parse("p").unwrap_or_else(|_| unreachable!());
    let case_re = Regex::new(r"^\d{2}-\d{5,7}(?:-\d+)?$").unwrap_or_else(|_| unreachable!());

    for article in document.select(&article_sel) {
        let article_html = article.inner_html();

        // Collect all elements in order to parse the structure
        let mut current_district = String::new();
        let mut current_crime_type: Option<String> = None;
        let mut current_case: Option<String> = None;
        let mut current_narrative = String::new();

        // We need to walk the DOM in order. Get the root element's children.
        // Strategy: split the article HTML by <hr> to get incident blocks,
        // then parse each block.
        let blocks: Vec<&str> = article_html.split("<hr").collect();

        for block in &blocks {
            let block_html = format!("<div>{block}</div>");
            let block_doc = Html::parse_fragment(&block_html);

            let mut block_district = String::new();
            let mut block_crime_type: Option<String> = None;
            let mut block_case: Option<String> = None;
            let mut block_paragraphs: Vec<String> = Vec::new();

            // Extract h2 (district)
            for h2 in block_doc.select(&h2_sel) {
                let text = h2.text().collect::<String>().trim().to_string();
                if !text.is_empty() {
                    block_district = text;
                }
            }

            // Extract h3 elements (alternating: crime type, case number)
            let h3_texts: Vec<String> = block_doc
                .select(&h3_sel)
                .map(|h3| h3.text().collect::<String>().trim().to_string())
                .filter(|t| !t.is_empty())
                .collect();

            for h3_text in &h3_texts {
                if case_re.is_match(h3_text) {
                    block_case = Some(h3_text.clone());
                } else {
                    block_crime_type = Some(h3_text.clone());
                }
            }

            // Extract paragraphs
            for p in block_doc.select(&p_sel) {
                let text = p.text().collect::<String>().trim().replace('\u{a0}', " ");
                if !text.is_empty() {
                    block_paragraphs.push(text);
                }
            }

            // Use the district from this block, or carry over from previous
            if !block_district.is_empty() {
                current_district = block_district;
            }

            // If we have a case number, emit an incident
            if let Some(case) = block_case {
                let narrative = block_paragraphs.join("\n");
                let crime = block_crime_type
                    .clone()
                    .unwrap_or_else(|| "Unknown".to_string());

                let date = extract_date_from_narrative(&narrative);
                let (address, city) = extract_address_from_narrative(&narrative);

                if !case.is_empty() {
                    incidents.push(ParsedIncident {
                        district: current_district.clone(),
                        crime_type: crime,
                        case_number: case,
                        narrative,
                        date,
                        address,
                        city,
                    });
                }
            } else if block_crime_type.is_some() {
                // Store for potential next block
                current_crime_type = block_crime_type;
                current_case = None;
                current_narrative = block_paragraphs.join("\n");
            }
        }

        // Handle case where crime type and case number are separate h3s
        // within the same block (no <hr> separator before first incident)
        let _ = (current_crime_type, current_case, current_narrative);
    }

    incidents
}

/// Convert a [`ParsedIncident`] into a flat JSON object suitable for field
/// mapping.
fn incident_to_json(incident: &ParsedIncident) -> serde_json::Value {
    let mut map = serde_json::Map::new();

    map.insert(
        "case_number".to_string(),
        serde_json::Value::String(incident.case_number.clone()),
    );
    map.insert(
        "district".to_string(),
        serde_json::Value::String(incident.district.clone()),
    );
    map.insert(
        "crime_type".to_string(),
        serde_json::Value::String(incident.crime_type.clone()),
    );
    map.insert(
        "narrative".to_string(),
        serde_json::Value::String(incident.narrative.clone()),
    );

    if let Some(ref date) = incident.date {
        map.insert("date".to_string(), serde_json::Value::String(date.clone()));
    }
    if let Some(ref addr) = incident.address {
        map.insert(
            "block_address".to_string(),
            serde_json::Value::String(addr.clone()),
        );
    }
    if let Some(ref city) = incident.city {
        map.insert("city".to_string(), serde_json::Value::String(city.clone()));
    }

    // Build a combined description
    let desc = if incident.district.is_empty() {
        incident.crime_type.clone()
    } else {
        format!("{} — {}", incident.crime_type, incident.district)
    };
    map.insert("description".to_string(), serde_json::Value::String(desc));

    serde_json::Value::Object(map)
}

/// Fetches press releases from a paginated listing page, parses each
/// individual release, and sends incident records through the channel.
///
/// Returns the total number of incident records fetched.
///
/// # Errors
///
/// Returns [`SourceError`] if HTTP requests or HTML parsing fails.
#[allow(clippy::too_many_lines)]
pub async fn fetch_press_release(
    config: &PressReleaseConfig<'_>,
    options: &FetchOptions,
    tx: &mpsc::Sender<Vec<serde_json::Value>>,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<u64, SourceError> {
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        .build()?;

    let fetch_limit = options.limit.unwrap_or(u64::MAX);
    let max_pages = if config.max_pages == 0 {
        MAX_LISTING_PAGES
    } else {
        config.max_pages
    };

    let link_sel =
        Selector::parse(config.link_selector).map_err(|e| SourceError::Normalization {
            message: format!("invalid link CSS selector: {e}"),
        })?;

    let mut total_fetched: u64 = 0;
    let mut seen_urls: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    log::info!(
        "{}: starting press release crawl from {}",
        config.label,
        config.listing_url,
    );

    // ── Crawl listing pages ──────────────────────────────────────────
    for page_num in 0..max_pages {
        if total_fetched >= fetch_limit {
            break;
        }

        let listing_url = if page_num == 0 {
            config.listing_url.to_string()
        } else if config.listing_url.contains('?') {
            format!("{}&{}={page_num}", config.listing_url, config.page_param)
        } else {
            format!("{}?{}={page_num}", config.listing_url, config.page_param)
        };

        log::info!(
            "{}: fetching listing page {page_num} ({listing_url})",
            config.label,
        );

        let resp = client.get(&listing_url).send().await?;
        if !resp.status().is_success() {
            log::warn!(
                "{}: listing page {page_num} returned {}",
                config.label,
                resp.status(),
            );
            break;
        }

        let html = resp.text().await?;

        // Parse the listing page in a block so the non-Send `Html` is
        // dropped before the next `.await`.
        let release_urls = {
            let document = Html::parse_document(&html);
            let mut urls: Vec<String> = Vec::new();
            for link in document.select(&link_sel) {
                if let Some(href) = link.value().attr("href") {
                    if !config.link_filter.is_empty() && !href.contains(config.link_filter) {
                        continue;
                    }
                    let full_url = if href.starts_with("http") {
                        href.to_string()
                    } else {
                        format!("{}{href}", config.base_url)
                    };
                    if seen_urls.insert(full_url.clone()) {
                        urls.push(full_url);
                    }
                }
            }
            urls
        };

        if release_urls.is_empty() {
            log::info!(
                "{}: no more press releases found on page {page_num}, stopping",
                config.label,
            );
            break;
        }

        log::info!(
            "{}: found {} press release URLs on page {page_num}",
            config.label,
            release_urls.len(),
        );

        // ── Fetch and parse each press release ───────────────────────
        for release_url in &release_urls {
            if total_fetched >= fetch_limit {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(REQUEST_DELAY_MS)).await;

            let resp = match client.get(release_url).send().await {
                Ok(r) => r,
                Err(e) => {
                    log::warn!("{}: failed to fetch {release_url}: {e}", config.label);
                    continue;
                }
            };

            if !resp.status().is_success() {
                log::warn!("{}: {release_url} returned {}", config.label, resp.status(),);
                continue;
            }

            let release_html = resp.text().await?;
            let incidents = if config.parse_mode == "drupal_narrative" {
                parse_drupal_narrative(&release_html)
            } else {
                parse_press_release(&release_html, config.article_selector)
            };

            if incidents.is_empty() {
                continue;
            }

            let page: Vec<serde_json::Value> = incidents.iter().map(incident_to_json).collect();

            let count = page.len() as u64;
            total_fetched += count;
            progress.inc(count);

            tx.send(page)
                .await
                .map_err(|e| SourceError::Normalization {
                    message: format!("channel send failed: {e}"),
                })?;
        }
    }

    log::info!(
        "{}: press release crawl complete — {total_fetched} incidents",
        config.label,
    );
    progress.finish(format!(
        "{}: press release crawl complete -- {total_fetched} incidents",
        config.label,
    ));
    Ok(total_fetched)
}
