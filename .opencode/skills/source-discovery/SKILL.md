---
name: source-discovery
description: Systematic discovery and evaluation of public crime data APIs with persistent state tracking
---

# Source Discovery Skill

## Overview

The crime-map project maintains a discovery system for finding, evaluating, and tracking public crime data APIs across the United States. All discovery state is persisted in a SQLite database at `data/discovery.db`, managed through the CLI tool `cargo discover`.

The workflow is:

1. **Search** for crime data portals and datasets using known strategies
2. **Evaluate** each lead for data quality, coordinate availability, freshness, and licensing
3. **Record** all findings and search activity in the discovery database
4. **Integrate** viable sources into the ingest pipeline

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

   Compare the list of known sources (from `cargo discover leads list`) against major US cities and states. Prioritize regions with no coverage. The status output may include a coverage summary to help with this.

## Discovery Strategies

### A. Socrata Portal Discovery

Socrata is the most common open data platform used by US cities and counties. Most Socrata portals follow predictable URL patterns and expose a consistent API.

**Finding portals:**

- Common domain patterns:
  - `data.{city}.gov` (e.g., `data.seattle.gov`, `data.chicago.gov`)
  - `data.{county}.gov` (e.g., `data.lacounty.gov`)
  - `opendata.{state}.gov` or `data.{state}.gov`
  - `{city}opendata.socrata.com` (older style)
- Verify a domain is Socrata by fetching: `https://{domain}/api/views.json?limit=1`
  - A valid Socrata portal returns a JSON array of dataset metadata

**Searching for crime datasets:**

- List all datasets: `https://{domain}/api/views.json?limit=100`
- Filter by name: look for datasets with names containing: `crime`, `police`, `incident`, `offense`, `arrest`, `call`, `dispatch`, `ucr`, `nibrs`
- Use the catalog search: `https://{domain}/api/catalog/v1?q=crime&limit=20`
- Each dataset has a 4x4 identifier (e.g., `ijzp-q8t2`) used in the SODA API

**Verifying coordinates exist:**

- Fetch 1 sample record: `https://{domain}/resource/{id}.json?$limit=1`
- Check for fields like: `latitude`, `longitude`, `lat`, `lng`, `location` (often a Socrata location object with `latitude`/`longitude` sub-fields), `geocoded_column`, `the_geom`
- A Socrata location object looks like: `{"latitude": "47.6", "longitude": "-122.3", "human_address": "..."}`

**Getting record counts:**

```
https://{domain}/resource/{id}.json?$select=count(*) as count
```

**Checking data freshness:**

```
https://{domain}/resource/{id}.json?$order=date_field DESC&$limit=1
```

Replace `date_field` with the actual date column name from the sample record.

**Recording the search:**

```
cargo discover search-log add --type socrata_portal --query "{domain}" --scope "{city/state}" --results-found {count}
```

### B. ArcGIS Hub/Server Discovery

Many cities and counties publish crime data through ArcGIS Online, ArcGIS Hub, or self-hosted ArcGIS Server instances. These expose REST APIs for feature services.

**ArcGIS Hub search:**

```
https://hub.arcgis.com/api/v3/datasets?q=crime+{city}&per_page=20
```

Response includes dataset metadata with URLs to the underlying feature service.

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

This lists all service folders. Then enumerate each folder:

```
https://{host}/arcgis/rest/services/{folder}?f=json
```

Look for services with names like `Police`, `Crime`, `PublicSafety`, `Incidents`.

**Checking layers:**

Once you find a feature service:

```
https://{host}/arcgis/rest/services/{path}/FeatureServer?f=json
```

This lists layers. Check each layer's name for crime-related keywords.

**Verifying geometry and getting counts:**

```
https://{host}/arcgis/rest/services/{path}/FeatureServer/{layer}/query?where=1=1&returnCountOnly=true&f=json
```

Fetch a sample:

```
https://{host}/arcgis/rest/services/{path}/FeatureServer/{layer}/query?where=1=1&outFields=*&resultRecordCount=1&f=json
```

Check that features have geometry (usually point geometry with x/y coordinates in the `geometry` field).

**Recording the search:**

```
cargo discover search-log add --type arcgis --query "{search terms}" --scope "{city/state}" --results-found {count}
```

### C. CKAN Portal Discovery

CKAN is an open-source data portal platform used by some governments (e.g., `catalog.data.gov`).

**Detecting a CKAN portal:**

```
https://{domain}/api/3/action/status_show
```

A valid CKAN instance returns `{"success": true, ...}`.

**Searching for crime data:**

```
https://{domain}/api/3/action/package_search?q=crime&rows=20
```

Response includes dataset metadata with resource URLs (often CSV or API endpoints).

**Listing all packages:**

```
https://{domain}/api/3/action/package_list
```

**Checking resources:**

Each package has resources. Look for resources with format `CSV`, `JSON`, `GeoJSON`, or `API`. Fetch a sample to check for coordinate fields.

**Recording the search:**

```
cargo discover search-log add --type ckan --query "{domain} crime" --scope "{region}" --results-found {count}
```

### D. Web Scraping Evaluation

Some police departments publish crime data on their websites without a formal API. These require scraping.

**What to look for on a police department website:**

- Crime maps (often powered by CrimeMapping.com, RAIDS Online, LexisNexis Community Crime Map, or SpotCrime)
- Crime statistics pages with HTML tables
- Downloadable CSV/Excel files (sometimes behind a form)
- Press release / crime bulletin PDFs
- Embedded iframe maps pointing to third-party services

**Assessing scrapability:**

1. Check `robots.txt` for disallowed paths
2. Look for anti-bot protections: Cloudflare, CAPTCHA, JavaScript-rendered content
3. Assess pagination: does the data list paginate? How many pages?
4. Check if data is rendered server-side (view source) or client-side (JavaScript)
5. Look for underlying API calls in browser network tab (many "scrape" targets actually have a hidden JSON API)

**Recording scrape targets:**

```
cargo discover scrape add --url "{url}" --city "{city}" --state "{state}" --method "{html_table|csv_download|api_hidden}" --difficulty "{easy|medium|hard}"
```

### E. Geocoding Assessment

Some datasets contain street addresses but no coordinates. These can still be viable if geocoding is feasible.

**When to consider geocoding:**

- Dataset has address fields (street, city, state, zip) but no lat/lng
- Dataset is otherwise high quality (good date fields, crime types, record count)
- The jurisdiction is not already covered by a geocoded source

**Evaluating address quality:**

- Fetch 5-10 sample records
- Check for: full street address, city, state, zip code
- Look for: intersection format ("1st & Main"), block format ("100 block of Main St"), or incomplete addresses
- Block-format addresses are common in crime data (for privacy) and geocode reasonably well

**Geocoding services:**

- **US Census Bureau Geocoder** (free, batch capable): `https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address={addr}&benchmark=Public_AR_Current&format=json`
- **Nominatim** (free, rate-limited): `https://nominatim.openstreetmap.org/search?q={addr}&format=json`
- **Photon** (free, based on OSM): `https://photon.komoot.io/api/?q={addr}`

Test a few sample addresses to gauge geocoding success rate. If >80% of addresses geocode successfully, the source is viable.

**Recording geocoding notes:**

Include geocoding requirements when adding or updating the lead:

```
cargo discover leads update <id> --notes "Requires geocoding. Address quality: good. Census geocoder test: 9/10 success."
```

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
   - Check if the dataset appears to be actively maintained (metadata update date)
7. **Record or update the lead:**

   ```
   cargo discover leads add \
     --city "{city}" \
     --state "{state}" \
     --source-type "{socrata|arcgis|ckan|scrape|other}" \
     --url "{api_url}" \
     --has-coordinates {true|false} \
     --record-count {count} \
     --freshness "{daily|weekly|monthly|stale|unknown}" \
     --notes "{evaluation notes}"
   ```

   Or update an existing lead:

   ```
   cargo discover leads update <id> \
     --status {investigated|viable|not_viable|integrated} \
     --has-coordinates {true|false} \
     --record-count {count} \
     --notes "{updated notes}"
   ```

## Legal Evaluation

Before marking a source as viable, assess its legal status:

**Where to find license information:**

- On the dataset page itself (look for "License", "Terms", "About" sections)
- On the portal's terms of service page (usually linked in the footer)
- In API metadata responses (Socrata includes license info in dataset metadata)

**Common license types:**

| License | Usability |
|---------|-----------|
| Public Domain / CC0 | Fully open, no restrictions |
| Open Data / Open Government License | Open with attribution |
| CC-BY | Open with attribution required |
| CC-BY-SA | Open with attribution + share-alike |
| CC-BY-NC | Non-commercial only -- check if project qualifies |
| Custom TOS | Read carefully, assess case by case |

**General rules:**

- Socrata open data portals: data is generally published as open data. Verify on the specific dataset.
- ArcGIS public feature services: if publicly accessible without authentication, generally intended for public use. Check the portal's TOS.
- Scraping: always check `robots.txt` and the site's TOS for scraping clauses. Some explicitly prohibit automated access.

**Recording legal assessment:**

```
cargo discover legal add <lead_id> \
  --license-type "{open_data|cc_by|cc0|public_domain|custom|unknown}" \
  --tos-url "{url}" \
  --notes "{assessment notes}"
```

## Recording Findings

Every action during a discovery session must be persisted:

- **Every search** gets logged via `cargo discover search-log add`
- **Every promising dataset** becomes a lead via `cargo discover leads add`
- **Every evaluation** updates the lead via `cargo discover leads update`
- **Every legal assessment** is recorded via `cargo discover legal add`

Do not rely on memory or conversation history. The discovery database is the single source of truth. If it is not recorded in the database, it did not happen.

## Session Workflow

A standard discovery session follows this pattern:

1. **Check status** -- `cargo discover status` to understand current state
2. **Review search log** -- `cargo discover search-log list` to avoid duplicating effort
3. **Pick a strategy** based on priority:
   - **Highest**: Investigate existing leads with status `new` (they need evaluation)
   - **High**: Explore regions with no coverage
   - **Medium**: Try new search strategies on partially-covered regions
   - **Low**: Re-check previously stale sources for updates
4. **Execute 3-5 searches or investigations:**
   - For each search: perform the query, examine results, record the search
   - For each promising result: evaluate the dataset, add as lead
   - For each existing lead: investigate, update status
5. **Summarize the session:**
   - What searches were performed
   - What new leads were found
   - What leads were evaluated and their outcomes
   - Suggested next steps for the following session
