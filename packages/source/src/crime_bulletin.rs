//! Daily crime-bulletin scraper for police department websites.
//!
//! Designed for agencies that publish structured daily crime bulletins on a
//! single page using accordion/expandable sections (e.g., Howard County PD
//! publishes daily bulletins at `howardcountymd.gov/police/newsroom`).
//!
//! The scraper:
//! 1. Fetches the single bulletin page
//! 2. Extracts accordion item content blocks
//! 3. Parses structured incident data from each bulletin (crime type,
//!    city/zip, block address, date range, items stolen, vehicle info)
//! 4. Emits one JSON record per incident entry, suitable for field mapping
//!
//! ## Bulletin entry format
//!
//! Each `<p>` in a bulletin can be:
//! - A **crime type header**: `<strong><u>Robbery</u>` (bold + underlined)
//! - A **city entry** (first for a crime type): `<strong>City, ZIP:</strong> address, date, items`
//! - A **continuation entry** (same city, same crime type): `address, date, items`
//! - An **arrest line**: `ARRESTED: Name, age, charges`

use std::sync::Arc;

use regex::Regex;
use scraper::{Html, Selector};
use tokio::sync::mpsc;

use crate::progress::ProgressCallback;
use crate::{FetchOptions, SourceError};

/// Configuration for a crime-bulletin scraper.
pub struct CrimeBulletinConfig<'a> {
    /// URL of the page containing the bulletins.
    pub url: &'a str,
    /// CSS selector for accordion content containers.
    pub content_selector: &'a str,
    /// Label for log messages.
    pub label: &'a str,
}

/// A single parsed incident entry from a crime bulletin.
#[derive(Debug)]
struct BulletinEntry {
    /// Crime type (e.g., "Robbery", "Vehicle theft").
    crime_type: String,
    /// City name (e.g., "Columbia").
    city: String,
    /// ZIP code (e.g., "21044").
    zip: String,
    /// Block address or location (e.g., "6000 block of Cedar Wood Drive").
    address: String,
    /// Date range text (e.g., "Feb. 19-20 overnight").
    date_text: String,
    /// Items stolen or description (e.g., "tools", "purse").
    description: String,
    /// Full raw text of the entry.
    raw_text: String,
    /// Bulletin date from the accordion header (e.g., "February 20, 2026").
    bulletin_date: String,
    /// Arrest info if present.
    arrest_info: Option<String>,
}

/// Clean HTML tags and entities from a string.
fn clean_html(s: &str) -> String {
    let re = Regex::new(r"<[^>]+>").unwrap_or_else(|_| unreachable!());
    let cleaned = re.replace_all(s, "");
    cleaned
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace('\u{a0}', " ")
        .trim()
        .to_string()
}

/// Parse a year from a bulletin date header. Falls back to current year.
fn extract_year_from_header(header: &str) -> &str {
    let re = Regex::new(r"20\d{2}").unwrap_or_else(|_| unreachable!());
    re.find(header).map_or("2026", |m| m.as_str())
}

/// Parse a single bulletin accordion content block into incident entries.
fn parse_bulletin_content(html_content: &str, bulletin_date: &str) -> Vec<BulletinEntry> {
    let mut entries = Vec::new();

    // The content is a series of <p> tags. Parse them.
    let fragment = Html::parse_fragment(html_content);
    let p_sel = Selector::parse("p").unwrap_or_else(|_| unreachable!());

    // Regex for detecting crime type headers (bold + underlined text)
    let crime_type_re =
        Regex::new(r"(?i)<strong>\s*<u>([^<]+)</u>").unwrap_or_else(|_| unreachable!());

    // Regex for detecting city/zip entries: "City, ZIPCODE:" pattern
    let city_zip_re =
        Regex::new(r"(?:^|<strong>)\s*([A-Z][a-zA-Z\s]+),\s*(\d{5}):\s*</strong>\s*(.*)")
            .unwrap_or_else(|_| unreachable!());

    // Regex for continuation entries (same city, different address)
    let continuation_re = Regex::new(r"<strong>([A-Z][a-zA-Z\s]+),\s*(\d{5}):\s*</strong>\s*(.*)")
        .unwrap_or_else(|_| unreachable!());

    let mut current_crime_type = String::new();
    let mut current_city = String::new();
    let mut current_zip = String::new();

    for p_elem in fragment.select(&p_sel) {
        let inner_html = p_elem.inner_html();
        let text = clean_html(&inner_html);

        // Skip empty paragraphs and the bulletin title
        if text.is_empty() || text.starts_with("HCPD Daily Crime Bulletin") {
            continue;
        }

        // Check for ARRESTED line
        if text.contains("ARRESTED:") || text.contains("𝐀𝐫𝐫𝐞𝐬𝐭𝐞𝐝") {
            // Attach arrest info to the most recent entry
            if let Some(last) = entries.last_mut() {
                let entry: &mut BulletinEntry = last;
                entry.arrest_info = Some(text.clone());
                entry.description = format!("{} [{}]", entry.description, text);
            }
            continue;
        }

        // Check for crime type header (bold + underlined)
        if let Some(caps) = crime_type_re.captures(&inner_html) {
            current_crime_type = clean_html(&caps[1]);

            // The crime type line may also contain the first city entry
            // e.g., "<strong><u>Robbery</u><br />\nColumbia, 21045:</strong> address..."
            let after_header = &inner_html[caps.get(0).unwrap().end()..];
            if let Some(city_caps) = continuation_re.captures(after_header) {
                current_city = clean_html(&city_caps[1]);
                current_zip = city_caps[2].to_string();
                let rest = clean_html(&city_caps[3]);
                if let Some(entry) = parse_entry_text(
                    &rest,
                    &current_crime_type,
                    &current_city,
                    &current_zip,
                    bulletin_date,
                ) {
                    entries.push(entry);
                }
            }
            continue;
        }

        // Check for city/zip entry (bold city with colon)
        if let Some(caps) = city_zip_re.captures(&inner_html) {
            current_city = clean_html(&caps[1]);
            current_zip = caps[2].to_string();
            let rest = clean_html(&caps[3]);

            // This may contain multiple entries separated by <br> tags
            let sub_entries: Vec<&str> = rest.split('\n').collect();
            for sub in sub_entries {
                let trimmed = sub.trim();
                if !trimmed.is_empty()
                    && let Some(entry) = parse_entry_text(
                        trimmed,
                        &current_crime_type,
                        &current_city,
                        &current_zip,
                        bulletin_date,
                    )
                {
                    entries.push(entry);
                }
            }
            continue;
        }

        // Otherwise, it may be a continuation entry or a description line
        // for the previous entry (e.g., vehicle description for vehicle theft)
        if !text.is_empty() && !current_crime_type.is_empty() {
            // Check if this looks like a new address entry (starts with a number
            // for block address, or is a vehicle description line)
            let is_address_line = text.chars().next().is_some_and(|c| c.is_ascii_digit());

            if is_address_line && !current_city.is_empty() {
                if let Some(entry) = parse_entry_text(
                    &text,
                    &current_crime_type,
                    &current_city,
                    &current_zip,
                    bulletin_date,
                ) {
                    entries.push(entry);
                }
            } else if let Some(last) = entries.last_mut() {
                // Append to previous entry's description (e.g., vehicle info)
                let entry: &mut BulletinEntry = last;
                if entry.description.is_empty() {
                    entry.description.clone_from(&text);
                } else {
                    entry.description = format!("{} | {text}", entry.description);
                }
                entry.raw_text = format!("{}\n{text}", entry.raw_text);
            }
        }
    }

    entries
}

/// Parse a single entry text line into a `BulletinEntry`.
///
/// Expected format: "9200 block of All Saints Road, Feb. 18 8:40 a.m."
/// or: "6000 block of Cedar Wood Drive, Feb. 19-20 overnight, tools"
fn parse_entry_text(
    text: &str,
    crime_type: &str,
    city: &str,
    zip: &str,
    bulletin_date: &str,
) -> Option<BulletinEntry> {
    if text.is_empty() {
        return None;
    }

    // Try to split into address and date/description parts
    // Pattern: optional business name + address, date info, items
    let address_re = Regex::new(
        r"(?:([A-Za-z][A-Za-z\s&'\.]+),\s+)?(\d+\s+block\s+of\s+[^,]+|[A-Z][a-zA-Z\s]+(?:Road|Street|Highway|Boulevard|Avenue|Drive|Lane|Way|Place|Court|Circle|Pike|Parkway|Trail|Run)[^,]*),\s*(.*)"
    ).ok()?;

    // Also handle intersection format: "Street & Street, date"
    let intersection_re = Regex::new(
        r"([A-Z][a-zA-Z\s]+(?:Road|Street|Highway|Boulevard|Avenue|Drive|Lane|Way|Place|Court|Circle|Pike|Parkway|Trail|Run)\s*&\s*[A-Z][a-zA-Z\s]+(?:Road|Street|Highway|Boulevard|Avenue|Drive|Lane|Way|Place|Court|Circle|Pike|Parkway|Trail|Run)[^,]*),\s*(.*)"
    ).ok()?;

    #[allow(clippy::option_if_let_else)]
    let (address, date_and_desc) = if let Some(caps) = address_re.captures(text) {
        let business = caps.get(1).map(|m| m.as_str().trim());
        let addr = caps[2].trim().to_string();
        let rest = caps[3].to_string();
        let full_addr = if let Some(biz) = business {
            format!("{biz}, {addr}")
        } else {
            addr
        };
        (full_addr, rest)
    } else if let Some(caps) = intersection_re.captures(text) {
        (caps[1].trim().to_string(), caps[2].to_string())
    } else {
        // Fallback: use the whole text
        (String::new(), text.to_string())
    };

    // Split date_and_desc into date part and description part
    // Date patterns: "Feb. 19-20 overnight", "Feb. 18 8:40 a.m.",
    // "Jan. 28-29 overnight"
    let date_split_re = Regex::new(
        r"^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d{1,2}(?:-\d{1,2})?\s*(?:overnight|(?:\d{1,2}(?::\d{2})?\s*(?:a\.m\.|p\.m\.|am|pm)?\s*(?:-\s*\d{1,2}(?::\d{2})?\s*(?:a\.m\.|p\.m\.|am|pm)?)?)?(?:\s*overnight)?)?)(?:,\s*(.*))?$"
    ).ok();

    let (date_text, description) = if let Some(re) = &date_split_re
        && let Some(caps) = re.captures(&date_and_desc)
    {
        let date = caps[1].trim().to_string();
        let desc = caps
            .get(2)
            .map_or(String::new(), |m| m.as_str().trim().to_string());
        (date, desc)
    } else {
        (date_and_desc, String::new())
    };

    Some(BulletinEntry {
        crime_type: crime_type.to_string(),
        city: city.to_string(),
        zip: zip.to_string(),
        address,
        date_text,
        description,
        raw_text: text.to_string(),
        bulletin_date: bulletin_date.to_string(),
        arrest_info: None,
    })
}

/// Generate a synthetic incident ID from a bulletin entry (no case numbers
/// in this format).
fn generate_incident_id(entry: &BulletinEntry) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    entry.bulletin_date.hash(&mut hasher);
    entry.crime_type.hash(&mut hasher);
    entry.city.hash(&mut hasher);
    entry.address.hash(&mut hasher);
    entry.date_text.hash(&mut hasher);
    format!("HCPD-{:016X}", hasher.finish())
}

/// Convert a [`BulletinEntry`] to a flat JSON object for field mapping.
fn entry_to_json(entry: &BulletinEntry, year: &str) -> serde_json::Value {
    let mut map = serde_json::Map::new();

    let incident_id = generate_incident_id(entry);
    map.insert(
        "incident_id".to_string(),
        serde_json::Value::String(incident_id),
    );
    map.insert(
        "crime_type".to_string(),
        serde_json::Value::String(entry.crime_type.clone()),
    );
    map.insert(
        "city".to_string(),
        serde_json::Value::String(entry.city.clone()),
    );
    map.insert(
        "zip".to_string(),
        serde_json::Value::String(entry.zip.clone()),
    );
    map.insert(
        "block_address".to_string(),
        serde_json::Value::String(if entry.address.is_empty() {
            entry.raw_text.clone()
        } else {
            format!("{}, {}, MD {}", entry.address, entry.city, entry.zip)
        }),
    );
    map.insert(
        "date_text".to_string(),
        serde_json::Value::String(entry.date_text.clone()),
    );

    // Try to convert the date_text into an ISO date
    if let Some(iso_date) = parse_bulletin_date(&entry.date_text, year) {
        map.insert("date".to_string(), serde_json::Value::String(iso_date));
    } else {
        // Fall back to the bulletin date
        map.insert(
            "date".to_string(),
            serde_json::Value::String(entry.bulletin_date.clone()),
        );
    }

    let desc = if entry.description.is_empty() {
        format!("{} — {}", entry.crime_type, entry.raw_text)
    } else {
        format!("{} — {}", entry.crime_type, entry.description)
    };
    map.insert("description".to_string(), serde_json::Value::String(desc));

    if let Some(ref arrest) = entry.arrest_info {
        map.insert(
            "arrest_info".to_string(),
            serde_json::Value::String(arrest.clone()),
        );
    }

    serde_json::Value::Object(map)
}

/// Try to convert bulletin date text (e.g., "Feb. 19-20 overnight") into
/// an ISO-8601 date string.
fn parse_bulletin_date(date_text: &str, year: &str) -> Option<String> {
    let re =
        Regex::new(r"(?i)(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+(\d{1,2})").ok()?;
    let caps = re.captures(date_text)?;
    let month_str = &caps[1];
    let day: u32 = caps[2].parse().ok()?;

    let month = match month_str.to_lowercase().as_str() {
        "jan" => 1,
        "feb" => 2,
        "mar" => 3,
        "apr" => 4,
        "may" => 5,
        "jun" => 6,
        "jul" => 7,
        "aug" => 8,
        "sep" => 9,
        "oct" => 10,
        "nov" => 11,
        "dec" => 12,
        _ => return None,
    };

    let yr: u32 = year.parse().ok()?;
    Some(format!("{yr:04}-{month:02}-{day:02}T00:00:00"))
}

/// Fetches the crime bulletin page, parses all accordion items, and sends
/// incident records through the channel.
///
/// Returns the total number of incident records fetched.
///
/// # Errors
///
/// Returns [`SourceError`] if HTTP requests or HTML parsing fails.
#[allow(clippy::too_many_lines)]
pub async fn fetch_crime_bulletin(
    config: &CrimeBulletinConfig<'_>,
    options: &FetchOptions,
    tx: &mpsc::Sender<Vec<serde_json::Value>>,
    progress: &Arc<dyn ProgressCallback>,
) -> Result<u64, SourceError> {
    let client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        .timeout(std::time::Duration::from_secs(120))
        .connect_timeout(std::time::Duration::from_secs(30))
        .build()?;

    let fetch_limit = options.limit.unwrap_or(u64::MAX);

    log::info!(
        "{}: fetching crime bulletin page at {}",
        config.label,
        config.url,
    );

    let html = crate::retry::send_text(|| client.get(config.url)).await?;

    // Parse accordion items in a non-async block (Html is not Send)
    let all_entries = {
        let document = Html::parse_document(&html);

        let content_sel =
            Selector::parse(config.content_selector).map_err(|e| SourceError::Normalization {
                message: format!("invalid content CSS selector: {e}"),
            })?;

        // Also grab the accordion titles for bulletin dates
        let title_sel = Selector::parse(".accordion__title h3, .accordion h3")
            .unwrap_or_else(|_| unreachable!());

        // Collect accordion titles (dates)
        let titles: Vec<String> = document
            .select(&title_sel)
            .map(|el| el.text().collect::<String>().trim().to_string())
            .filter(|t| !t.is_empty())
            .collect();

        // Collect accordion content blocks
        let contents: Vec<String> = document
            .select(&content_sel)
            .map(|el| el.inner_html())
            .collect();

        log::info!(
            "{}: found {} accordion titles, {} content blocks",
            config.label,
            titles.len(),
            contents.len(),
        );

        let mut entries: Vec<BulletinEntry> = Vec::new();

        for (i, content) in contents.iter().enumerate() {
            let bulletin_date = titles.get(i).map_or("Unknown", String::as_str);
            let year = extract_year_from_header(bulletin_date);
            let mut block_entries = parse_bulletin_content(content, bulletin_date);

            // Fill in year for date parsing
            for entry in &mut block_entries {
                if let Some(iso) = parse_bulletin_date(&entry.date_text, year) {
                    entry.date_text = iso;
                }
            }

            entries.append(&mut block_entries);
        }

        entries
    };

    let total = all_entries.len() as u64;
    let to_send = total.min(fetch_limit);

    log::info!(
        "{}: parsed {} total entries from bulletin, sending {}",
        config.label,
        total,
        to_send,
    );

    // Send entries in batches
    let batch_size = 50;
    let mut sent: u64 = 0;

    #[allow(clippy::cast_possible_truncation)]
    let take_count = to_send as usize;

    for chunk in all_entries
        .iter()
        .take(take_count)
        .collect::<Vec<_>>()
        .chunks(batch_size)
    {
        let year = chunk.first().map_or("2026", |first| {
            extract_year_from_header(&first.bulletin_date)
        });

        let page: Vec<serde_json::Value> = chunk.iter().map(|e| entry_to_json(e, year)).collect();
        let count = page.len() as u64;
        sent += count;
        progress.inc(count);

        tx.send(page)
            .await
            .map_err(|e| SourceError::Normalization {
                message: format!("channel send failed: {e}"),
            })?;
    }

    log::info!(
        "{}: bulletin scrape complete — {sent} incidents",
        config.label,
    );
    progress.finish(format!(
        "{}: bulletin scrape complete -- {sent} incidents",
        config.label,
    ));
    Ok(sent)
}
