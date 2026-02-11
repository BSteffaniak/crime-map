---
name: source-discovery
description: Aggressive discovery and integration of crime data from every possible source — APIs, scraping, PDFs, hidden endpoints, third-party platforms
---

# Source Discovery Skill

## Core Philosophy: THE DATA EXISTS. FIND IT.

Every city with a police department has crime data. The question is never "does the data exist?" — it is only "how is it published?" Our job is to exhaust every possible avenue to find and ingest that data.

**Principles:**

- **"No results" means try harder.** One failed search strategy means try 10 more. A dead API means look for scrapers, PDFs, hidden endpoints, third-party aggregators, FOIA portals, and archived snapshots.
- **Scraping is a first-class strategy.** If there is no API, scrape it. If the PDF exists, extract it. If the table is on a webpage, parse it. We have `html_table`, `csv_download`, `json_paginated`, and `pdf_extract` fetcher types — use them.
- **Reverse-engineer everything.** Open the browser network tab. Inspect XHR calls behind crime map dashboards. Read `robots.txt` for hidden paths. Decode obfuscated JavaScript for API keys. The hidden API is almost always there.
- **Cross-pollinate.** When you find a new API pattern (e.g., CrimeMapping.com uses a specific JSON endpoint), immediately check if other cities use the same platform.
- **Document licensing, always.** Every source needs a `[license]` section in its TOML. This is non-negotiable. But "restricted" data is still configured — it's just opt-in at ingest time.
- **Never mark a city as "no data."** Mark it as "not yet found." Come back with new strategies.

## Overview

The crime-map project maintains a discovery system for finding, evaluating, and tracking public crime data APIs across the United States. All discovery state is persisted in a SQLite database at `data/discovery.db`, managed through the CLI tool `cargo discover`.

The workflow is:

1. **Search** for crime data using EVERY strategy available
2. **Evaluate** each lead for data quality, coordinate availability, freshness, and licensing
3. **Record** all findings and search activity in the discovery database
4. **Integrate** viable sources into the ingest pipeline (including scraper-based sources)

Every search performed and every lead found must be recorded. This ensures no work is duplicated across sessions and provides a clear audit trail of what has been explored.

## Starting a Session

Always begin a discovery session with these steps:

1. **Check current state:**

   ```
   cargo discover status
   ```

   This shows total leads by status, recent activity, and coverage summary.

2. **Review past searches:**

   ```
   cargo discover search-log list
   ```

   This shows what strategies have been tried, what queries were used, and what regions have been covered. Do not repeat searches that have already been logged.

3. **Identify gaps in coverage:**

   Compare the list of known sources (from `cargo discover leads list`) against major US cities and states. Prioritize regions with no coverage.

4. **Check for "not yet found" cities:**

   Any city previously marked as a dead end should be re-evaluated with fresh eyes and new strategies. The discovery database tracks what was tried — try something different.

## Discovery Strategies

### Strategy Matrix

Use ALL of these strategies, in roughly this priority order. For each city/jurisdiction, work through the list until you find data. DO NOT STOP at strategy 1 or 2.

| # | Strategy | When to use | Success rate |
|---|----------|-------------|--------------|
| 1 | Socrata catalog API | First pass for any city | High for large cities |
| 2 | ArcGIS Hub search | Second pass | High for mid-size cities |
| 3 | ArcGIS Online search | Cities with known ESRI infrastructure | Medium |
| 4 | ArcGIS Server direct enumeration | When you know the GIS domain | High when domain known |
| 5 | CKAN portal search | Known CKAN portals (data.gov, etc.) | Medium |
| 6 | Carto API search | Cities with Carto history | Low |
| 7 | Google search | When portal discovery fails | High — often finds hidden portals |
| 8 | CrimeMapping.com | Always check — very common platform | Medium-High |
| 9 | RAIDS Online | Always check | Medium |
| 10 | LexisNexis Community Crime Map | Common for mid-size cities | Medium |
| 11 | SpotCrime.com | Aggregates from multiple PD sources | Low (aggregated) |
| 12 | City police department website | Browse for data/stats sections | Medium |
| 13 | State UCR/NIBRS portal | Covers entire state at once | High for state data |
| 14 | FBI Crime Data Explorer | Federal aggregate data | High for national data |
| 15 | Data.gov (CKAN catalog) | Federal cross-reference | Low |
| 16 | OpenDataSoft portals | Some cities use this platform | Low |
| 17 | Tyler Technologies / data.world | Alternative data platforms | Low |
| 18 | Wayback Machine | Check archived versions of dead portals | Last resort |
| 19 | Network tab inspection | Uncover hidden APIs behind maps/dashboards | High when applicable |
| 20 | `robots.txt` review | Find hidden paths on city websites | Low but cheap |

### A. Socrata Portal Discovery

Socrata is the most common open data platform used by US cities and counties.

**Finding portals:**

- Common domain patterns:
  - `data.{city}.gov` (e.g., `data.seattle.gov`, `data.chicago.gov`)
  - `data.{county}.gov` (e.g., `data.lacounty.gov`)
  - `opendata.{state}.gov` or `data.{state}.gov`
  - `{city}opendata.socrata.com` (older style)
- Verify a domain is Socrata by fetching: `https://{domain}/api/views.json?limit=1`

**Searching for crime datasets:**

- Catalog search: `https://{domain}/api/catalog/v1?q=crime&limit=20`
- Also search: `police`, `incident`, `offense`, `arrest`, `ucr`, `nibrs`, `public safety`
- List all datasets: `https://{domain}/api/views.json?limit=100`

**Verifying data quality:**

- Sample record: `https://{domain}/resource/{id}.json?$limit=1`
- Record count: `https://{domain}/resource/{id}.json?$select=count(*) as count`
- Freshness: `https://{domain}/resource/{id}.json?$order={date_field} DESC&$limit=1`

**Recording the search:**

```
cargo discover search-log add --search-type socrata_portal --query "{domain}" --geographic-scope "{city/state}" --results-summary "{description of findings}"
```

### B. ArcGIS Hub/Server Discovery

Many cities publish crime data through ArcGIS Online, ArcGIS Hub, or self-hosted ArcGIS Server instances.

**ArcGIS Hub search:**

```
https://hub.arcgis.com/api/v3/datasets?q=crime+{city}&per_page=20
```

**ArcGIS Online search:**

```
https://www.arcgis.com/sharing/rest/search?q=crime {city}&f=json&num=20
```

Look for items of type `Feature Service` or `Map Service`.

**Direct server enumeration:**

If you know a city's ArcGIS server domain:

```
https://{host}/arcgis/rest/services?f=json
```

Then enumerate each folder:

```
https://{host}/arcgis/rest/services/{folder}?f=json
```

Look for services with names like `Police`, `Crime`, `PublicSafety`, `Incidents`.

**Checking layers:**

```
https://{host}/arcgis/rest/services/{path}/FeatureServer?f=json
```

Check the `copyrightText` field for licensing information.

**Verifying geometry and getting counts:**

```
https://{host}/arcgis/rest/services/{path}/FeatureServer/{layer}/query?where=1=1&returnCountOnly=true&f=json
```

Sample:

```
https://{host}/arcgis/rest/services/{path}/FeatureServer/{layer}/query?where=1=1&outFields=*&resultRecordCount=1&f=json
```

**Recording the search:**

```
cargo discover search-log add --search-type arcgis --query "{search terms}" --geographic-scope "{city/state}" --results-summary "{description of findings}"
```

### C. CKAN Portal Discovery

CKAN is used by some governments (e.g., `catalog.data.gov`, `data.boston.gov`).

**Detecting a CKAN portal:**

```
https://{domain}/api/3/action/status_show
```

**Searching:**

```
https://{domain}/api/3/action/package_search?q=crime&rows=20
```

**Recording the search:**

```
cargo discover search-log add --search-type ckan --query "{domain} crime" --geographic-scope "{region}" --results-summary "{description of findings}"
```

### D. Google Search (DO NOT SKIP THIS)

When formal portal discovery fails, Google is your best friend. Use targeted searches:

- `site:{city}.gov crime data`
- `site:{city}.gov open data police`
- `"{city} police department" crime data API`
- `"{city} police department" crime statistics download`
- `"{city}" crime data arcgis`
- `"{city}" crime data socrata`
- `"{city}" crime incidents CSV`
- `"{city}" crime map API`
- `"{state}" statewide crime data`

Often this will reveal:
- Hidden open data portals not on the standard domains
- Direct links to ArcGIS feature services
- PDF crime bulletin archives
- CSV download pages on police department websites
- Third-party platforms hosting the data

**Recording the search:**

```
cargo discover search-log add --search-type google --query "{search query}" --geographic-scope "{city/state}" --results-summary "{description of findings}"
```

### E. Third-Party Crime Mapping Platforms

Many police departments outsource their crime mapping to commercial platforms. These platforms almost always have hidden APIs behind their map interfaces.

**CrimeMapping.com:**

- Check: `https://www.crimemapping.com/map/agency/{agency_id}`
- The API behind CrimeMapping uses a JSON endpoint. Open the network tab while the map loads and look for XHR requests.
- Common API pattern: POST to a search endpoint with date ranges and agency IDs.

**RAIDS Online:**

- Check: `https://rfraidsonline.com/api/incidents?agency={id}`
- Similar to CrimeMapping — JSON API behind the map.

**LexisNexis Community Crime Map:**

- Check: `https://communitycrimemap.com/?address={city},{state}`
- Has a REST API behind the map interface.

**SpotCrime:**

- Check: `https://spotcrime.com/map/{city}-{state}`
- Aggregates data from various sources. Use as a lead — find the original source.

**Recording:**

```
cargo discover search-log add --search-type third_party --query "{platform} {city}" --geographic-scope "{city/state}" --results-summary "{description of findings}"
```

### F. Network Tab Inspection (Hidden API Discovery)

This is one of the MOST EFFECTIVE strategies for cities that appear to have no API.

**How to do it:**

1. Go to the city's crime map page in a browser
2. Open Developer Tools → Network tab
3. Filter by XHR/Fetch requests
4. Interact with the map (zoom, filter, search)
5. Look for JSON API calls — these are your hidden endpoints

**What to look for:**

- POST/GET requests returning JSON arrays of incident data
- Requests to `/api/`, `/services/`, `/query`, `/search` endpoints
- Authentication tokens in headers (often passed as cookies or URL params)
- Pagination parameters in the request

**Common hidden API patterns:**

- Tableau: `https://{host}/views/{workbook}?:embed=y` → look for `tabula-data` XHR calls
- Power BI: `https://wabi-{region}.analysis.windows.net/public/reports/{id}` → look for data query calls
- Custom map dashboards: Almost always call a REST API for data

### G. Statewide and Federal Data Sources

Some states publish statewide crime datasets that cover ALL jurisdictions:

**State portals to check:**

- `opendata.{state}.gov`
- `data.{state}.gov`
- `{state}ucr.com` or `{state}nibrs.com`
- `{state}.gov/crime-statistics`
- State Bureau of Investigation websites

**Federal sources:**

- FBI Crime Data Explorer: `https://cde.ucr.cjis.gov/LATEST/webapp/#/pages/explorer`
- Bureau of Justice Statistics: `https://bjs.ojp.gov/`
- Data.gov: `https://catalog.data.gov/dataset?q=crime`

**Key advantage:** A single statewide source can cover dozens of jurisdictions at once.

### H. Scraping Evaluation

When a city publishes data on its website without a formal API, evaluate for scraping.

**What to look for on police department websites:**

- Crime maps (often CrimeMapping.com, RAIDS Online, LexisNexis)
- Crime statistics pages with HTML tables
- Downloadable CSV/Excel files (sometimes behind a form)
- Press release / crime bulletin PDFs
- Crime blotter / activity log pages with tabular data
- Embedded iframe maps pointing to third-party services

**Assessing scrapability:**

1. Check `robots.txt` for disallowed paths
2. Look for anti-bot protections: Cloudflare, CAPTCHA, JavaScript-rendered content
3. Assess pagination: how many pages? Is data date-ranged?
4. Check if data is server-rendered (view source) vs client-rendered (JavaScript)
5. **Always look for hidden API calls in the network tab first** — the "scrape" target often has a JSON API

**Our fetcher types for scraping:**

| Fetcher | TOML type | Use case |
|---------|-----------|----------|
| `html_table` | `type = "html_table"` | Police websites with tabular crime data |
| `csv_download` | `type = "csv_download"` | Downloadable CSV files (periodic or static) |
| `json_paginated` | `type = "json_paginated"` | Hidden JSON APIs behind dashboards/maps |
| `pdf_extract` | `type = "pdf_extract"` | PDF crime bulletins and reports |

**Recording scrape targets:**

```
cargo discover scrape add --lead-id {id} --url "{url}" --strategy "{html_table|json_paginated|csv_download|pdf_extract}" --estimated-effort "{easy|medium|hard}"
```

### I. Geocoding Assessment

Some datasets contain addresses but no coordinates. These are STILL VIABLE.

**When to consider geocoding:**

- Dataset has address fields (street, city, state, zip) but no lat/lng
- Dataset is otherwise high quality (good date fields, crime types, record count)
- The jurisdiction is not already covered by a geocoded source

**Evaluating address quality:**

- Fetch 5-10 sample records
- Check for: full street address, city, state, zip
- Block-format addresses ("100 block of Main St") geocode reasonably well
- Intersection format ("1st & Main") also works

**Test geocoding success rate:**

```
https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address={addr}&benchmark=Public_AR_Current&format=json
```

If >80% of addresses geocode successfully, the source is viable. Sources without coordinates use `lat`/`lng` fields omitted from their TOML — the `cargo ingest geocode` command handles batch geocoding later.

## "Second Chance" Protocol

Before accepting that a city has no data, you MUST complete this checklist:

- [ ] Searched Socrata catalog with queries: `crime`, `police`, `incident`, `offense`
- [ ] Searched ArcGIS Hub with city name
- [ ] Searched ArcGIS Online with city name
- [ ] Googled: `site:{city}.gov crime data`
- [ ] Googled: `"{city} police department" crime data`
- [ ] Googled: `"{city}" crime incidents CSV OR API OR download`
- [ ] Checked CrimeMapping.com for the city
- [ ] Checked RAIDS Online for the city
- [ ] Checked LexisNexis Community Crime Map for the city
- [ ] Visited the police department website and checked for data/statistics sections
- [ ] Checked if a statewide dataset covers the city
- [ ] Checked if a county-level dataset includes the city
- [ ] Checked neighboring cities for shared regional data platforms
- [ ] Opened any available crime map in the browser and inspected network tab for APIs

Only after completing ALL of these can you mark a city as `rejected` (and even then, note what was tried so future sessions don't repeat).

## Evaluating a Lead

When you find a promising dataset, perform a thorough evaluation before recording it:

1. **Fetch 1-2 sample records** to understand the schema
2. **Check for coordinate fields:**
   - Direct: `latitude`, `longitude`, `lat`, `lng`, `x`, `y`
   - Nested: `location.latitude`, `geometry.coordinates`
   - Socrata: `location` object, `geocoded_column`
   - ArcGIS: `geometry` field on features
3. **Check for date fields:**
   - Look for: `date`, `occurred_date`, `report_date`, `incident_date`, `datetime`
   - Verify the format (ISO 8601, Unix timestamp, custom)
4. **Check for required fields:**
   - Incident ID or case number (for deduplication)
   - Crime type / offense description / UCR code
   - Address (even if coordinates exist, useful for validation)
5. **Get total record count** to assess dataset size
6. **Determine data freshness:**
   - Sort by date descending, fetch the most recent record
   - If the most recent record is older than 30 days, note this as a concern
7. **Record or update the lead:**

   ```
   cargo discover leads add \
     --jurisdiction "{City, ST}" \
     --name "{Human-readable source name}" \
     --api-type "{socrata|arcgis|ckan|carto|odata|scrape|unknown}" \
     --url "{api_url}" \
     --priority "{high|medium|low}" \
     --likelihood {0.0-1.0} \
     --notes "{evaluation notes}"
   ```

   Or update an existing lead:

   ```
   cargo discover leads update <id> \
     --status {verified_good|needs_geocoding|verified_no_coords|verified_no_data|rejected} \
     --has-coordinates {true|false} \
     --record-count {count} \
     --notes "{updated notes}"
   ```

   To view full details about a lead:

   ```
   cargo discover leads investigate <id>
   ```

## Legal Evaluation

Before integrating a source, assess its legal status. Every source MUST have a `[license]` section in its TOML — this is non-negotiable.

### License Discovery Methods (in order of reliability)

| # | Method | How | When it works |
|---|--------|-----|---------------|
| 1 | **Socrata metadata API** | `https://{domain}/api/views/{4x4}.json` → check `licenseId` field | ~50% of Socrata portals have explicit `licenseId` |
| 2 | **Portal TOS page** | Try `https://{domain}/terms`, `/tos`, `/site/terms.html`, `/pages/terms-of-use` | Most portals have a TOS link |
| 3 | **Dataset page** | `https://{domain}/d/{4x4}` → look for "License" section | Socrata dataset pages |
| 4 | **Portal footer/about** | `https://{domain}/` → look for "Terms", "Legal", "License" links | Universal |
| 5 | **ArcGIS service metadata** | `https://{host}/arcgis/rest/services/{path}/FeatureServer?f=json` → check `copyrightText` | ArcGIS services |
| 6 | **Google search** | `site:{domain} terms of use` or `"{city} open data" license terms` | When portal navigation fails |
| 7 | **City/state open data policy** | Search for `{city} open data executive order` or `{city} open data policy` | Many cities have formal policies |

### Common License Types

| License | TOML value | Usability |
|---------|------------|-----------|
| Public Domain / PDDL | `"public_domain"` | Fully open, no restrictions |
| Creative Commons Zero | `"cc_zero"` | Fully open, no restrictions |
| Open Data / Open Government License | `"open_data"` | Open with attribution typically requested |
| CC-BY / ODC-BY | `"cc_by"` | Open with attribution required |
| CC-BY-SA | `"cc_by_sa"` | Open with attribution + share-alike |
| Custom TOS | `"tos_restricted"` | Read carefully — usually permissive for government data |
| Proprietary platform | `"proprietary"` | Requires opt-in to ingest (`restricted = true`) |
| Unknown | `"unknown"` | Research more, but do not block integration |

### Socrata `licenseId` Mapping

The Socrata metadata API (`/api/views/{id}.json`) returns a `licenseId` field with these common values:

| `licenseId` value | TOML `license_type` |
|-------------------|---------------------|
| `PUBLIC_DOMAIN` | `"public_domain"` |
| `PDDL` | `"public_domain"` |
| `CC0_10` | `"cc_zero"` |
| `ODC_BY` | `"cc_by"` |
| `CC_30_BY` | `"cc_by"` |
| `SEE_TERMS_OF_USE` | `"tos_restricted"` |
| (absent/null) | `"open_data"` (default for government Socrata portals) |

### Licensing Rules

- **EVERY source MUST have a `[license]` section.** No exceptions.
- **Scraping sources MUST document `robots.txt` status** and TOS position on automated access.
- **Sources with `restricted = true`** are still configured and tracked — they just require `--include-restricted` to ingest.
- **Attribution text must be captured verbatim** when required.
- **Default assumption for US government open data:** If a city/county government publishes data on a public portal, it is generally open for public use. When in doubt, use `license_type = "open_data"` with `attribution_required = true`.

**Recording legal assessment:**

```
cargo discover legal add \
  --lead-id {lead_id} \
  --license-type "{open_data|cc_by|cc_by_sa|cc_zero|public_domain|proprietary|tos_restricted|unknown}" \
  --tos-url "{url}" \
  --allows-api-access true \
  --allows-bulk-download true \
  --allows-redistribution true \
  --notes "{assessment notes}"
```

## Integrating a Viable Source

After a lead reaches `verified_good` status and passes legal review, integrate it into the ingest pipeline.

### Step 1: Generate a skeleton TOML config

```
cargo discover integrate <lead_id>
```

This command:
- Reads the lead's metadata (jurisdiction, API type, URL)
- Generates a skeleton TOML config file at `packages/source/sources/{name}.toml`
- Adds the `include_str!` entry to `packages/source/src/registry.rs`
- Increments the `EXPECTED_SOURCE_COUNT` in the registry test
- Marks the lead status as `integrated`

The generated TOML will have `TODO` placeholders for field mappings and the `[license]` section that must be filled in.

### Step 2: Fill in the field mappings and license

The skeleton TOML needs its `[fields]` and `[license]` sections completed.

**Required `[license]` section (MUST be fully explicit, no defaults):**

```toml
[license]
license_type = "open_data"       # REQUIRED: public_domain|cc_zero|cc_by|cc_by_sa|open_data|tos_restricted|proprietary|unknown
tos_url = "https://..."          # Optional: URL to terms of service
attribution_required = true      # REQUIRED: whether attribution is needed
attribution_text = "City of ..." # Optional: verbatim attribution text
allows_redistribution = true     # REQUIRED: whether redistribution is allowed
allows_scraping = true           # Optional: whether scraping is explicitly allowed
restricted = false               # REQUIRED: whether source requires opt-in to ingest
notes = "..."                    # Optional: free-form notes
```

**Required fields:**
- `incident_id` — List of field names for the incident/case ID (tried in order as fallback chain)
- `crime_type` — List of field names for crime type/category (first non-empty wins)
- `occurred_at` — Date extractor configuration (see options below)
- `lat` / `lng` — Coordinate fields (OMIT entirely for sources requiring geocoding)
- `description` — How to build the description string

**Date extractor types:**
| Type | Use when | TOML example |
|------|----------|--------------|
| `simple` | Single ISO-8601 datetime field | `type = "simple"` / `field = "date"` |
| `date_plus_hhmm` | Separate date + 4-digit time (e.g., "1430") | `type = "date_plus_hhmm"` / `date_field = "date_occ"` / `time_field = "time_occ"` |
| `date_plus_hhmmss` | Separate date + "HH:MM:SS" time | `type = "date_plus_hhmmss"` / `date_field = "date"` / `time_field = "time"` |
| `epoch_ms` | Epoch milliseconds (float) | `type = "epoch_ms"` / `field = "report_dat"` |
| `mdy_date` | "MM/DD/YYYY" text date (no time) | `type = "mdy_date"` / `field = "date"` |

**Coordinate types:**
| Type | Use when | TOML example |
|------|----------|--------------|
| `string` | Coordinate is a JSON string (e.g., `"47.6"`) | `field = "latitude"` / `type = "string"` |
| `f64` | Coordinate is a JSON number | `field = "geo_lat"` / `type = "f64"` |
| `point_lat` | Latitude from a GeoJSON Point or Socrata location | `field = "location"` / `type = "point_lat"` |
| `point_lng` | Longitude from a GeoJSON Point or Socrata location | `field = "location"` / `type = "point_lng"` |

**Description extractor types:**
| Type | Use when | TOML example |
|------|----------|--------------|
| `single` | One field contains the full description | `type = "single"` / `field = "offense_desc"` |
| `combine` | Combine multiple fields with a separator | `type = "combine"` / `fields = ["type", "detail"]` / `separator = ": "` |
| `fallback_chain` | Try fields in order, use first non-empty | `type = "fallback_chain"` / `fields = ["pd_desc", "ofns_desc"]` |

**Arrest extractor types:**
| Type | Use when | TOML example |
|------|----------|--------------|
| `none` | No arrest information available | `type = "none"` |
| `direct_bool` | Boolean field | `type = "direct_bool"` / `field = "arrest"` |
| `string_contains` | String checked for substring | `type = "string_contains"` / `field = "resolution"` / `contains = "arrest"` |

**Optional fields:**
- `reported_at` — String field name for report date
- `block_address` — String field name for block/street address
- `location_type` — String field name for location description
- `domestic` — String field name for domestic violence boolean flag

### Step 3: Test the configuration

```bash
# Verify the TOML parses correctly
cargo test -p crime_map_source

# Run a small test ingest
cargo ingest sync --source {source_id} --limit 100
```

### Step 4: Record the source

After successful testing:

```
cargo discover sources add \
  --source-id "{source_id}" \
  --jurisdiction "{City, ST}" \
  --api-type "{socrata|arcgis|ckan|carto|odata|scrape}" \
  --url "{api_url}"
```

### Source TOML Templates

#### Standard API Source (Socrata/ArcGIS/CKAN/Carto/OData)

```toml
id = "{city_state}_pd"
name = "{City} Police Department"
city = "{City}"
state = "{ST}"
output_filename = "{city}_crimes.json"

[license]
license_type = "open_data"
attribution_required = true
attribution_text = "City of {City}"
allows_redistribution = true
restricted = false

[fetcher]
# Choose ONE fetcher type:
type = "socrata"
api_url = "https://data.{city}.gov/resource/{4x4-id}.json"
date_column = "date_field_name"
page_size = 50000

[fields]
incident_id = ["case_number", "incident_id"]
crime_type = ["offense_type", "category"]

[fields.occurred_at]
type = "simple"
field = "occurred_date"

[fields.lat]
field = "latitude"
type = "string"

[fields.lng]
field = "longitude"
type = "string"

[fields.description]
type = "single"
field = "offense_description"

[fields.arrest]
type = "none"
```

#### HTML Table Scraper Source

```toml
id = "{city_state}_pd"
name = "{City} Police Department"
city = "{City}"
state = "{ST}"
output_filename = "{city}_crimes.json"

[license]
license_type = "open_data"
attribution_required = true
attribution_text = "City of {City}"
allows_redistribution = true
allows_scraping = true
restricted = false

[fetcher]
type = "html_table"
url = "https://police.{city}.gov/crime-stats"
table_selector = "table.crime-data"
# header_selector = "thead tr th"      # optional
# row_selector = "tbody tr"            # optional
# cell_selector = "td"                 # optional
# delay_ms = 1000                      # optional rate limiting

[fields]
# ... same field mapping as standard sources
```

#### CSV Download Source

```toml
[fetcher]
type = "csv_download"
urls = [
  "https://police.{city}.gov/data/crimes_2024.csv",
  "https://police.{city}.gov/data/crimes_2023.csv",
]
# delimiter = ","       # optional, default comma
# compressed = "gzip"   # optional
# max_records = 100000  # optional
```

#### Hidden JSON API Source

```toml
[fetcher]
type = "json_paginated"
api_url = "https://crime-api.{city}.gov/incidents"
pagination = "offset"           # offset | page | cursor
page_size = 1000
# response_format = "wrapped"  # bare_array (default) | wrapped
# records_path = "data.items"  # for wrapped responses
# page_param = "start"         # override default param name
# size_param = "count"         # override default size param
# delay_ms = 500               # rate limiting
```

#### PDF Extraction Source

```toml
[fetcher]
type = "pdf_extract"
urls = [
  "https://police.{city}.gov/bulletins/2024-01.pdf",
  "https://police.{city}.gov/bulletins/2024-02.pdf",
]
extraction_strategy = "regex_rows"
row_pattern = '(?P<date>\d{2}/\d{2}/\d{4})\s+(?P<type>\w+)\s+(?P<address>.+)'
# For text_table strategy:
# extraction_strategy = "text_table"
# column_boundaries = [0, 12, 30, 55]
# column_names = ["date", "type", "location", "description"]
# skip_header_lines = 2
```

## Recording Findings

Every action during a discovery session must be persisted:

- **Every search** gets logged via `cargo discover search-log add`
- **Every promising dataset** becomes a lead via `cargo discover leads add`
- **Every evaluation** updates the lead via `cargo discover leads update`
- **Every legal assessment** is recorded via `cargo discover legal add`

Do not rely on memory or conversation history. The discovery database is the single source of truth. If it is not recorded in the database, it did not happen.

## Lead Status Lifecycle

Leads progress through these statuses:

```
new -> investigating -> verified_good -> integrated
                     -> needs_geocoding -> integrated
                     -> needs_scraper -> integrated
                     -> verified_no_coords
                     -> verified_no_data
                     -> verified_aggregate_only
                     -> verified_proprietary -> integrated (restricted)
                     -> rejected (ONLY after Second Chance Protocol)
```

| Status | Meaning |
|--------|---------|
| `new` | Just discovered, needs evaluation |
| `investigating` | Currently being evaluated |
| `verified_good` | Has coordinates, dates, incident-level data — ready for integration |
| `needs_geocoding` | Good data but no coordinates — requires geocoding pipeline |
| `needs_scraper` | Data exists but requires web scraping to access |
| `verified_no_coords` | Evaluated, confirmed no coordinate data available |
| `verified_no_data` | Portal exists but no usable crime incident data |
| `verified_aggregate_only` | Only aggregate/summary stats, not incident-level |
| `verified_proprietary` | Behind proprietary platform — can still be integrated with `restricted = true` |
| `integrated` | TOML config created and registered in the ingest pipeline |
| `rejected` | Evaluated via Second Chance Protocol and determined unsuitable |

## Session Workflow

A standard discovery session follows this pattern:

1. **Check status** — `cargo discover status` to understand current state
2. **Review search log** — `cargo discover search-log list` to avoid duplicating effort
3. **Pick a strategy** based on priority:
   - **Highest**: Investigate existing leads with status `new` (they need evaluation)
   - **High**: Re-check previously "dead end" cities with new strategies
   - **High**: Explore regions with no coverage using the full strategy matrix
   - **Medium**: Try new search strategies on partially-covered regions
   - **Low**: Re-check previously stale sources for updates
4. **Execute searches aggressively:**
   - For each city: work through the strategy matrix until you find data
   - For each search: perform the query, examine results, record the search
   - For each promising result: evaluate the dataset, add as lead
   - For each existing lead: investigate, update status
5. **Integrate viable leads:**
   - For leads at `verified_good`, `needs_geocoding`, or `needs_scraper`: generate TOML
   - Fill in field mappings AND `[license]` section by examining sample records
   - Run `cargo test -p crime_map_source` to verify
6. **Summarize the session:**
   - What searches were performed
   - What new leads were found
   - What leads were evaluated and their outcomes
   - What sources were integrated
   - What new API patterns were discovered (cross-pollinate for other cities!)
   - Suggested next steps for the following session

## CLI Reference

### Status
```
cargo discover status                    # Dashboard of leads, sources, searches
```

### Leads
```
cargo discover leads list                # List all leads
cargo discover leads list --status new   # Filter by status
cargo discover leads list --api-type socrata  # Filter by API type
cargo discover leads add --jurisdiction "City, ST" --name "Name" --api-type socrata --url "..." --priority high --likelihood 0.9 --notes "..."
cargo discover leads update <id> --status verified_good --record-count 50000 --has-coordinates true --notes "..."
cargo discover leads investigate <id>    # Show full details + legal info
```

### Search Log
```
cargo discover search-log list           # Show recent searches (default 20)
cargo discover search-log list --limit 50
cargo discover search-log add --search-type socrata_portal --query "data.example.gov" --geographic-scope "city:Example,ST" --results-summary "Found dataset xyz with 100K records"
```

### Sources
```
cargo discover sources list              # List all integrated sources
cargo discover sources list --status active
cargo discover sources add --source-id "example_pd" --jurisdiction "Example, ST" --api-type socrata --url "..."
```

### Legal
```
cargo discover legal list                # List all legal records
cargo discover legal add --lead-id 5 --license-type open_data --allows-api-access true --allows-bulk-download true --notes "..."
cargo discover legal show <id>           # Detailed view
```

### Scrape Targets
```
cargo discover scrape list               # List all scrape targets
cargo discover scrape add --lead-id 5 --url "https://..." --strategy html_table --estimated-effort medium
```

### Integration
```
cargo discover integrate <lead_id>       # Generate TOML + register in pipeline
cargo discover integrate <lead_id> --source-id custom_id  # Override the generated source ID
cargo discover integrate <lead_id> --dry-run              # Preview without writing files
```

### Other
```
cargo discover seed                      # Populate DB with existing knowledge
```
