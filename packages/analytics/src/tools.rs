//! Tool execution functions for the AI agent.
//!
//! Each function implements one of the analytical tools that the agent
//! can invoke. They run optimized SQL against `PostGIS` and return typed
//! results.

use crime_map_analytics_models::{
    CityInfo, ComparePeriodParams, ComparePeriodResult, CountIncidentsParams, CountIncidentsResult,
    ListCitiesParams, ListCitiesResult, PlaceInfo, RankAreaParams, RankAreaResult,
    SearchLocationsParams, SearchLocationsResult, TimeGranularity, TopCrimeTypesParams,
    TopCrimeTypesResult, TrendParams, TrendResult,
};
use crime_map_geography_models::{AreaStats, CategoryCount, PeriodComparison, TimeSeriesPoint};
use moosicbox_json_utils::database::ToValue as _;
use switchy_database::{Database, DatabaseValue};

use crate::AnalyticsError;

/// Parses a date string like `"2024-01-01"` into a `NaiveDateTime` at midnight.
///
/// `switchy_database` sends parameters in binary format, so date strings
/// must be converted to `DatabaseValue::DateTime` rather than passed as
/// `DatabaseValue::String` — Postgres cannot decode raw UTF-8 bytes as
/// a binary `timestamptz`.
fn parse_date(s: &str) -> Result<chrono::NaiveDateTime, AnalyticsError> {
    // Try full datetime first, then date-only
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Ok(dt);
    }
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map(|d| d.and_hms_opt(0, 0, 0).unwrap_or_default())
        .map_err(|e| AnalyticsError::Query {
            message: format!("Invalid date '{s}': {e}. Expected format: YYYY-MM-DD"),
        })
}

/// Builds a WHERE clause fragment and parameter list for the common
/// city/state/geoid/date/category/severity filters.
///
/// Returns `(where_fragments, params, next_param_index)`.
///
/// # Errors
///
/// Returns [`AnalyticsError`] if a date string cannot be parsed.
#[allow(clippy::too_many_arguments)]
fn build_common_filters(
    city: Option<&str>,
    state: Option<&str>,
    geoid: Option<&str>,
    place_geoid: Option<&str>,
    date_from: Option<&str>,
    date_to: Option<&str>,
    category: Option<&str>,
    subcategory: Option<&str>,
    severity_min: Option<u8>,
    start_idx: u32,
) -> Result<(Vec<String>, Vec<DatabaseValue>, u32), AnalyticsError> {
    let mut frags = Vec::new();
    let mut params: Vec<DatabaseValue> = Vec::new();
    let mut idx = start_idx;

    if let Some(city) = city {
        frags.push(format!("i.city ILIKE ${idx}"));
        params.push(DatabaseValue::String(city.to_string()));
        idx += 1;
    }

    if let Some(state) = state {
        frags.push(format!("i.state = ${idx}"));
        params.push(DatabaseValue::String(state.to_uppercase()));
        idx += 1;
    }

    if let Some(geoid) = geoid {
        frags.push(format!("i.census_tract_geoid = ${idx}"));
        params.push(DatabaseValue::String(geoid.to_string()));
        idx += 1;
    }

    if let Some(place_geoid) = place_geoid {
        frags.push(format!("i.census_place_geoid = ${idx}"));
        params.push(DatabaseValue::String(place_geoid.to_string()));
        idx += 1;
    }

    if let Some(from) = date_from {
        let dt = parse_date(from)?;
        frags.push(format!("i.occurred_at >= ${idx}"));
        params.push(DatabaseValue::DateTime(dt));
        idx += 1;
    }

    if let Some(to) = date_to {
        let dt = parse_date(to)?;
        frags.push(format!("i.occurred_at <= ${idx}"));
        params.push(DatabaseValue::DateTime(dt));
        idx += 1;
    }

    if let Some(cat) = category {
        frags.push(format!(
            "i.parent_category_id = (SELECT id FROM crime_categories WHERE name = ${idx} AND parent_id IS NULL)"
        ));
        params.push(DatabaseValue::String(cat.to_uppercase()));
        idx += 1;
    }

    if let Some(sub) = subcategory {
        frags.push(format!("c.name = ${idx}"));
        params.push(DatabaseValue::String(sub.to_uppercase()));
        idx += 1;
    }

    if let Some(sev) = severity_min
        && sev > 1
    {
        frags.push(format!("c.severity >= ${idx}"));
        params.push(DatabaseValue::Int32(i32::from(sev)));
        idx += 1;
    }

    Ok((frags, params, idx))
}

/// Returns `true` if any filter is active that requires a JOIN to
/// `crime_categories`. Since `parent_category_id` is denormalized onto
/// `crime_incidents`, the top-level category filter no longer needs
/// the join — only subcategory and severity filters still reference
/// columns on the `crime_categories` table.
fn needs_category_join(subcategory: Option<&str>, severity_min: Option<u8>) -> bool {
    subcategory.is_some() || severity_min.is_some_and(|s| s > 1)
}

fn where_clause(frags: &[String]) -> String {
    if frags.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", frags.join(" AND "))
    }
}

#[allow(clippy::option_if_let_else)]
fn describe_area(
    city: Option<&str>,
    state: Option<&str>,
    geoid: Option<&str>,
    place_geoid: Option<&str>,
) -> String {
    if let Some(place_geoid) = place_geoid {
        format!("Census place {place_geoid}")
    } else if let Some(geoid) = geoid {
        format!("census tract {geoid}")
    } else if let Some(city) = city {
        if let Some(state) = state {
            format!("{city}, {state}")
        } else {
            city.to_string()
        }
    } else if let Some(state) = state {
        format!("state {state}")
    } else {
        "all areas".to_string()
    }
}

fn describe_date_range(from: Option<&str>, to: Option<&str>) -> String {
    match (from, to) {
        (Some(f), Some(t)) => format!("{f} to {t}"),
        (Some(f), None) => format!("since {f}"),
        (None, Some(t)) => format!("through {t}"),
        (None, None) => "all time".to_string(),
    }
}

/// Counts incidents matching the given filters.
///
/// # Errors
///
/// Returns [`AnalyticsError`] if the database query fails.
pub async fn count_incidents(
    db: &dyn Database,
    params: &CountIncidentsParams,
) -> Result<CountIncidentsResult, AnalyticsError> {
    let (frags, db_params, _) = build_common_filters(
        params.city.as_deref(),
        params.state.as_deref(),
        params.geoid.as_deref(),
        params.place_geoid.as_deref(),
        params.date_from.as_deref(),
        params.date_to.as_deref(),
        params.category.as_deref(),
        params.subcategory.as_deref(),
        params.severity_min,
        1,
    )?;

    let wc = where_clause(&frags);
    let has_cat_filter = needs_category_join(params.subcategory.as_deref(), params.severity_min);

    // Total count — skip the category join when no category filters are
    // active, which lets Postgres use a much faster index-only scan.
    let count_sql = if has_cat_filter {
        format!(
            "SELECT COUNT(*) as total
             FROM crime_incidents i
             JOIN crime_categories c ON i.category_id = c.id
             {wc}"
        )
    } else {
        format!(
            "SELECT COUNT(*) as total
             FROM crime_incidents i
             {wc}"
        )
    };

    let rows = db.query_raw_params(&count_sql, &db_params).await?;
    let total: i64 = rows.first().map_or(0, |r| r.to_value("total").unwrap_or(0));

    // Category breakdown — join only to resolve parent category name
    let cat_sql = format!(
        "SELECT pc.name as category, COUNT(*) as cnt
         FROM crime_incidents i
         JOIN crime_categories pc ON i.parent_category_id = pc.id
         {wc}
         GROUP BY pc.name
         ORDER BY cnt DESC"
    );

    let cat_rows = db.query_raw_params(&cat_sql, &db_params).await?;
    let by_category: Vec<CategoryCount> = cat_rows
        .iter()
        .map(|row| {
            let cat: String = row.to_value("category").unwrap_or_default();
            let cnt: i64 = row.to_value("cnt").unwrap_or(0);
            CategoryCount {
                category: cat,
                #[allow(clippy::cast_sign_loss)]
                count: cnt as u64,
            }
        })
        .collect();

    #[allow(clippy::cast_sign_loss)]
    Ok(CountIncidentsResult {
        total: total as u64,
        by_category,
        area_description: describe_area(
            params.city.as_deref(),
            params.state.as_deref(),
            params.geoid.as_deref(),
            params.place_geoid.as_deref(),
        ),
        date_range: describe_date_range(params.date_from.as_deref(), params.date_to.as_deref()),
    })
}

/// Ranks census tracts within a city by incident count.
///
/// # Errors
///
/// Returns [`AnalyticsError`] if the database query fails.
#[allow(clippy::too_many_lines)]
pub async fn rank_areas(
    db: &dyn Database,
    params: &RankAreaParams,
) -> Result<RankAreaResult, AnalyticsError> {
    let limit = params.limit.unwrap_or(10);
    let safest_first = params.safest_first.unwrap_or(true);

    let mut frags = Vec::new();
    let mut db_params: Vec<DatabaseValue> = Vec::new();
    let mut idx = 1u32;

    // Either city or placeGeoid is required to scope the ranking
    if let Some(ref place_geoid) = params.place_geoid {
        frags.push(format!("i.census_place_geoid = ${idx}"));
        db_params.push(DatabaseValue::String(place_geoid.clone()));
        idx += 1;
    } else if let Some(ref city) = params.city {
        frags.push(format!("i.city ILIKE ${idx}"));
        db_params.push(DatabaseValue::String(city.clone()));
        idx += 1;
    } else {
        return Err(AnalyticsError::Query {
            message: "Either 'city' or 'placeGeoid' is required for rank_areas".to_string(),
        });
    }

    if let Some(ref state) = params.state {
        frags.push(format!("i.state = ${idx}"));
        db_params.push(DatabaseValue::String(state.to_uppercase()));
        idx += 1;
    }

    if let Some(ref from) = params.date_from {
        let dt = parse_date(from)?;
        frags.push(format!("i.occurred_at >= ${idx}"));
        db_params.push(DatabaseValue::DateTime(dt));
        idx += 1;
    }

    if let Some(ref to) = params.date_to {
        let dt = parse_date(to)?;
        frags.push(format!("i.occurred_at <= ${idx}"));
        db_params.push(DatabaseValue::DateTime(dt));
        idx += 1;
    }

    if let Some(ref cat) = params.category {
        frags.push(format!(
            "i.parent_category_id = (SELECT id FROM crime_categories WHERE name = ${idx} AND parent_id IS NULL)"
        ));
        db_params.push(DatabaseValue::String(cat.to_uppercase()));
        idx += 1;
    }

    let _ = idx; // suppress unused-after-increment; kept for safety if filters are added
    let wc = where_clause(&frags);

    // Fast pre-check: bail out immediately if no matching incidents have
    // census tract attribution.  Without this, the main query scans
    // millions of rows (e.g. 8.4 M for Chicago) only to join against
    // census_tracts and return zero results.
    let check_sql = format!(
        "SELECT EXISTS(SELECT 1 FROM crime_incidents i {wc} AND i.census_tract_geoid IS NOT NULL LIMIT 1)"
    );
    let check_rows = db.query_raw_params(&check_sql, &db_params).await?;
    let has_tract_data: bool = check_rows
        .first()
        .and_then(|r| r.to_value("exists").ok())
        .unwrap_or(false);

    if !has_tract_data {
        let area_name = params
            .city
            .as_deref()
            .or(params.place_geoid.as_deref())
            .unwrap_or("this area");
        return Ok(RankAreaResult {
            description: format!(
                "No census tract attribution data available for {area_name}. \
                 Tract-level ranking requires geocoded incidents with census tract \
                 assignments. Use count_incidents or trend instead for city-level analysis."
            ),
            areas: vec![],
        });
    }

    let sql = format!(
        "SELECT ct.geoid,
                COALESCE(n.name, ct.geoid) as area_id,
                COALESCE(n.name, ct.name) as area_name,
                ct.population, ct.land_area_sq_mi,
                pc.name as category, COUNT(*) as cat_cnt
         FROM crime_incidents i
         JOIN crime_categories pc ON i.parent_category_id = pc.id
         JOIN census_tracts ct ON ct.geoid = i.census_tract_geoid
         LEFT JOIN tract_neighborhoods tn ON ct.geoid = tn.geoid
         LEFT JOIN neighborhoods n ON tn.neighborhood_id = n.id
         {wc}
         AND i.census_tract_geoid IS NOT NULL
         GROUP BY ct.geoid, COALESCE(n.name, ct.geoid), COALESCE(n.name, ct.name),
                  ct.population, ct.land_area_sq_mi, pc.name"
    );

    let rows = db.query_raw_params(&sql, &db_params).await?;

    // Intermediate struct for aggregation: tracks per-area totals while
    // accumulating results from multiple tracts and category rows.
    #[allow(clippy::items_after_statements)]
    struct AreaAccum {
        area_name: String,
        total_incidents: u64,
        total_population: i64,
        total_land_area: f64,
        by_category: std::collections::BTreeMap<String, u64>,
        seen_geoids: std::collections::BTreeSet<String>,
    }

    // Aggregate rows by area (neighborhood name or geoid). Multiple rows
    // exist per area due to category grouping AND because one neighborhood
    // may span multiple census tracts.
    let mut area_map: std::collections::BTreeMap<String, AreaAccum> =
        std::collections::BTreeMap::new();

    for row in &rows {
        let geoid: String = row.to_value("geoid").unwrap_or_default();
        let area_id: String = row.to_value("area_id").unwrap_or_default();
        let area_name: String = row.to_value("area_name").unwrap_or_default();
        let population: Option<i32> = row.to_value("population").unwrap_or(None);
        let land_area: Option<f64> = row.to_value("land_area_sq_mi").unwrap_or(None);
        let cat: String = row.to_value("category").unwrap_or_default();
        let cat_cnt: i64 = row.to_value("cat_cnt").unwrap_or(0);

        let entry = area_map
            .entry(area_id.clone())
            .or_insert_with(|| AreaAccum {
                area_name,
                total_incidents: 0,
                total_population: 0,
                total_land_area: 0.0,
                by_category: std::collections::BTreeMap::new(),
                seen_geoids: std::collections::BTreeSet::new(),
            });

        #[allow(clippy::cast_sign_loss)]
        {
            entry.total_incidents += cat_cnt as u64;
        }

        #[allow(clippy::cast_sign_loss)]
        {
            *entry.by_category.entry(cat).or_insert(0) += cat_cnt as u64;
        }

        // Only count population and land area once per tract, even if
        // the tract appears in multiple category rows.
        if entry.seen_geoids.insert(geoid) {
            if let Some(pop) = population {
                entry.total_population += i64::from(pop);
            }
            if let Some(area) = land_area {
                entry.total_land_area += area;
            }
        }
    }

    // Convert accumulated data into AreaStats with computed rates
    let mut areas: Vec<AreaStats> = area_map
        .into_iter()
        .map(|(area_id, acc)| {
            let incidents_per_1k = if acc.total_population > 0 {
                #[allow(clippy::cast_precision_loss)]
                Some((acc.total_incidents as f64 / acc.total_population as f64) * 1000.0)
            } else {
                None
            };

            let incidents_per_sq_mi = if acc.total_land_area > 0.0 {
                #[allow(clippy::cast_precision_loss)]
                Some(acc.total_incidents as f64 / acc.total_land_area)
            } else {
                None
            };

            let land_area_sq_mi = if acc.total_land_area > 0.0 {
                Some(acc.total_land_area)
            } else {
                None
            };

            AreaStats {
                area_id,
                area_name: acc.area_name,
                total_incidents: acc.total_incidents,
                incidents_per_1k,
                land_area_sq_mi,
                incidents_per_sq_mi,
                by_category: acc
                    .by_category
                    .into_iter()
                    .map(|(category, count)| CategoryCount { category, count })
                    .collect(),
            }
        })
        .collect();

    // Sort by per-capita rate when available, falling back to absolute count
    areas.sort_by(|a, b| {
        let cmp = match (a.incidents_per_1k, b.incidents_per_1k) {
            (Some(a_rate), Some(b_rate)) => a_rate
                .partial_cmp(&b_rate)
                .unwrap_or(std::cmp::Ordering::Equal),
            // Tracts with known rate sort before those without
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => a.total_incidents.cmp(&b.total_incidents),
        };
        if safest_first { cmp } else { cmp.reverse() }
    });
    areas.truncate(limit as usize);

    let label = if safest_first {
        "safest"
    } else {
        "most dangerous"
    };

    let area_name = params
        .city
        .as_deref()
        .or(params.place_geoid.as_deref())
        .unwrap_or("unknown area");

    Ok(RankAreaResult {
        description: format!("Top {} {label} areas in {area_name}", areas.len()),
        areas,
    })
}

/// Compares crime between two time periods.
///
/// Uses conditional aggregation (`COUNT FILTER`) to compare both periods
/// in a single table scan instead of running separate queries per period.
///
/// # Errors
///
/// Returns [`AnalyticsError`] if the database query fails.
#[allow(clippy::too_many_lines)]
pub async fn compare_periods(
    db: &dyn Database,
    params: &ComparePeriodParams,
) -> Result<ComparePeriodResult, AnalyticsError> {
    // Parse all four date boundaries
    let a_from = parse_date(&params.period_a_from)?;
    let a_to = parse_date(&params.period_a_to)?;
    let b_from = parse_date(&params.period_b_from)?;
    let b_to = parse_date(&params.period_b_to)?;

    // Build location + category filters (shared across both periods)
    let mut frags = Vec::new();
    let mut db_params: Vec<DatabaseValue> = Vec::new();
    let mut idx = 1u32;

    if let Some(ref city) = params.city {
        frags.push(format!("i.city ILIKE ${idx}"));
        db_params.push(DatabaseValue::String(city.clone()));
        idx += 1;
    }
    if let Some(ref state) = params.state {
        frags.push(format!("i.state = ${idx}"));
        db_params.push(DatabaseValue::String(state.to_uppercase()));
        idx += 1;
    }
    if let Some(ref geoid) = params.geoid {
        frags.push(format!("i.census_tract_geoid = ${idx}"));
        db_params.push(DatabaseValue::String(geoid.clone()));
        idx += 1;
    }
    if let Some(ref place_geoid) = params.place_geoid {
        frags.push(format!("i.census_place_geoid = ${idx}"));
        db_params.push(DatabaseValue::String(place_geoid.clone()));
        idx += 1;
    }
    if let Some(ref cat) = params.category {
        frags.push(format!(
            "i.parent_category_id = (SELECT id FROM crime_categories WHERE name = ${idx} AND parent_id IS NULL)"
        ));
        db_params.push(DatabaseValue::String(cat.to_uppercase()));
        idx += 1;
    }

    // Date range covering both periods so the planner can use
    // the composite index on (place_geoid, occurred_at, ...).
    let date_lo_idx = idx;
    let date_hi_idx = idx + 1;
    frags.push(format!("i.occurred_at >= ${date_lo_idx}"));
    frags.push(format!("i.occurred_at <= ${date_hi_idx}"));
    let date_lo = a_from.min(b_from);
    let date_hi = a_to.max(b_to);
    db_params.push(DatabaseValue::DateTime(date_lo));
    db_params.push(DatabaseValue::DateTime(date_hi));
    idx += 2;

    // Period boundary params for FILTER clauses
    let a_from_idx = idx;
    let a_to_idx = idx + 1;
    let b_from_idx = idx + 2;
    let b_to_idx = idx + 3;
    db_params.push(DatabaseValue::DateTime(a_from));
    db_params.push(DatabaseValue::DateTime(a_to));
    db_params.push(DatabaseValue::DateTime(b_from));
    db_params.push(DatabaseValue::DateTime(b_to));

    let wc = where_clause(&frags);

    // ── Overall totals (single scan) ──────────────────────────────────
    // No category join needed — parent_category_id filter is on the
    // incidents table directly.
    let totals_sql = format!(
        "SELECT
           COUNT(*) FILTER (WHERE i.occurred_at >= ${a_from_idx} AND i.occurred_at <= ${a_to_idx}) as a_total,
           COUNT(*) FILTER (WHERE i.occurred_at >= ${b_from_idx} AND i.occurred_at <= ${b_to_idx}) as b_total
         FROM crime_incidents i
         {wc}"
    );

    let totals_rows = db.query_raw_params(&totals_sql, &db_params).await?;
    let (a_total, b_total): (i64, i64) = totals_rows.first().map_or((0, 0), |r| {
        (
            r.to_value("a_total").unwrap_or(0),
            r.to_value("b_total").unwrap_or(0),
        )
    });

    // ── Per-category breakdown (single scan) ──────────────────────────
    let cat_sql = format!(
        "SELECT pc.name as category,
                COUNT(*) FILTER (WHERE i.occurred_at >= ${a_from_idx} AND i.occurred_at <= ${a_to_idx}) as a_cnt,
                COUNT(*) FILTER (WHERE i.occurred_at >= ${b_from_idx} AND i.occurred_at <= ${b_to_idx}) as b_cnt
         FROM crime_incidents i
         JOIN crime_categories pc ON i.parent_category_id = pc.id
         {wc}
         GROUP BY pc.name
         ORDER BY a_cnt DESC"
    );

    let cat_rows = db.query_raw_params(&cat_sql, &db_params).await?;

    let area_desc = describe_area(
        params.city.as_deref(),
        params.state.as_deref(),
        params.geoid.as_deref(),
        params.place_geoid.as_deref(),
    );

    #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]
    let percent_change = if a_total > 0 {
        ((b_total as f64 - a_total as f64) / a_total as f64) * 100.0
    } else if b_total > 0 {
        100.0
    } else {
        0.0
    };

    #[allow(clippy::cast_sign_loss)]
    let overall = PeriodComparison {
        area_id: area_desc.clone(),
        area_name: area_desc.clone(),
        period_a_count: a_total as u64,
        period_b_count: b_total as u64,
        percent_change,
    };

    let mut by_category = Vec::new();
    for row in &cat_rows {
        let cat_name: String = row.to_value("category").unwrap_or_default();
        let a_cnt: i64 = row.to_value("a_cnt").unwrap_or(0);
        let b_cnt: i64 = row.to_value("b_cnt").unwrap_or(0);

        #[allow(clippy::cast_precision_loss)]
        let pct = if a_cnt > 0 {
            ((b_cnt as f64 - a_cnt as f64) / a_cnt as f64) * 100.0
        } else if b_cnt > 0 {
            100.0
        } else {
            0.0
        };

        #[allow(clippy::cast_sign_loss)]
        by_category.push(PeriodComparison {
            area_id: cat_name.clone(),
            area_name: cat_name,
            period_a_count: a_cnt as u64,
            period_b_count: b_cnt as u64,
            percent_change: pct,
        });
    }

    Ok(ComparePeriodResult {
        overall,
        by_category,
        description: format!(
            "Comparison for {area_desc}: {} to {} vs {} to {}",
            params.period_a_from, params.period_a_to, params.period_b_from, params.period_b_to,
        ),
    })
}

/// Gets crime count trends over time.
///
/// # Errors
///
/// Returns [`AnalyticsError`] if the database query fails.
pub async fn get_trend(
    db: &dyn Database,
    params: &TrendParams,
) -> Result<TrendResult, AnalyticsError> {
    let trunc = match params.granularity {
        TimeGranularity::Daily => "day",
        TimeGranularity::Weekly => "week",
        TimeGranularity::Monthly => "month",
        TimeGranularity::Yearly => "year",
    };

    let (frags, db_params, _) = build_common_filters(
        params.city.as_deref(),
        params.state.as_deref(),
        params.geoid.as_deref(),
        params.place_geoid.as_deref(),
        params.date_from.as_deref(),
        params.date_to.as_deref(),
        params.category.as_deref(),
        None,
        None,
        1,
    )?;

    let wc = where_clause(&frags);
    let cat_join = if needs_category_join(None, None) {
        "JOIN crime_categories c ON i.category_id = c.id"
    } else {
        ""
    };

    let sql = format!(
        "SELECT date_trunc('{trunc}', i.occurred_at)::date::text as period, COUNT(*) as cnt
         FROM crime_incidents i
         {cat_join}
         {wc}
         GROUP BY period
         ORDER BY period"
    );

    let rows = db.query_raw_params(&sql, &db_params).await?;

    let data: Vec<TimeSeriesPoint> = rows
        .iter()
        .map(|row| {
            let period: String = row.to_value("period").unwrap_or_default();
            let cnt: i64 = row.to_value("cnt").unwrap_or(0);
            TimeSeriesPoint {
                period,
                #[allow(clippy::cast_sign_loss)]
                count: cnt as u64,
            }
        })
        .collect();

    let area = describe_area(
        params.city.as_deref(),
        params.state.as_deref(),
        params.geoid.as_deref(),
        params.place_geoid.as_deref(),
    );

    Ok(TrendResult {
        description: format!("{} crime trend for {area}", params.granularity,),
        data,
    })
}

/// Finds the most common crime types in an area.
///
/// # Errors
///
/// Returns [`AnalyticsError`] if the database query fails.
pub async fn top_crime_types(
    db: &dyn Database,
    params: &TopCrimeTypesParams,
) -> Result<TopCrimeTypesResult, AnalyticsError> {
    let limit = params.limit.unwrap_or(10);

    let (frags, db_params, next_idx) = build_common_filters(
        params.city.as_deref(),
        params.state.as_deref(),
        params.geoid.as_deref(),
        params.place_geoid.as_deref(),
        params.date_from.as_deref(),
        params.date_to.as_deref(),
        None,
        None,
        None,
        1,
    )?;

    let wc = where_clause(&frags);

    // By subcategory
    let sub_sql = format!(
        "SELECT c.name as subcategory, COUNT(*) as cnt
         FROM crime_incidents i
         JOIN crime_categories c ON i.category_id = c.id
         {wc}
         GROUP BY c.name
         ORDER BY cnt DESC
         LIMIT ${next_idx}"
    );

    let mut sub_params = db_params.clone();
    sub_params.push(DatabaseValue::Int64(i64::from(limit)));

    let sub_rows = db.query_raw_params(&sub_sql, &sub_params).await?;
    let subcategories: Vec<CategoryCount> = sub_rows
        .iter()
        .map(|row| {
            let name: String = row.to_value("subcategory").unwrap_or_default();
            let cnt: i64 = row.to_value("cnt").unwrap_or(0);
            CategoryCount {
                category: name,
                #[allow(clippy::cast_sign_loss)]
                count: cnt as u64,
            }
        })
        .collect();

    // By category — use denormalized parent_category_id
    let cat_sql = format!(
        "SELECT pc.name as category, COUNT(*) as cnt
         FROM crime_incidents i
         JOIN crime_categories pc ON i.parent_category_id = pc.id
         {wc}
         GROUP BY pc.name
         ORDER BY cnt DESC"
    );

    let cat_rows = db.query_raw_params(&cat_sql, &db_params).await?;
    let categories: Vec<CategoryCount> = cat_rows
        .iter()
        .map(|row| {
            let name: String = row.to_value("category").unwrap_or_default();
            let cnt: i64 = row.to_value("cnt").unwrap_or(0);
            CategoryCount {
                category: name,
                #[allow(clippy::cast_sign_loss)]
                count: cnt as u64,
            }
        })
        .collect();

    // Total — no category filter in the WHERE clause, so skip the join
    let total_sql = format!(
        "SELECT COUNT(*) as total FROM crime_incidents i
         {wc}"
    );
    let total_rows = db.query_raw_params(&total_sql, &db_params).await?;
    let total: i64 = total_rows
        .first()
        .map_or(0, |r| r.to_value("total").unwrap_or(0));

    let area = describe_area(
        params.city.as_deref(),
        params.state.as_deref(),
        params.geoid.as_deref(),
        params.place_geoid.as_deref(),
    );

    #[allow(clippy::cast_sign_loss)]
    Ok(TopCrimeTypesResult {
        subcategories,
        categories,
        total: total as u64,
        description: format!("Top crime types in {area}"),
    })
}

/// Lists available cities in the dataset.
///
/// # Errors
///
/// Returns [`AnalyticsError`] if the database query fails.
#[allow(clippy::option_if_let_else)]
pub async fn list_cities(
    db: &dyn Database,
    params: &ListCitiesParams,
) -> Result<ListCitiesResult, AnalyticsError> {
    let (sql, db_params) = if let Some(ref state) = params.state {
        (
            "SELECT city, state, COUNT(*) as cnt
             FROM crime_incidents
             WHERE city IS NOT NULL AND city != '' AND state = $1
             GROUP BY city, state
             ORDER BY cnt DESC"
                .to_string(),
            vec![DatabaseValue::String(state.to_uppercase())],
        )
    } else {
        (
            "SELECT city, state, COUNT(*) as cnt
             FROM crime_incidents
             WHERE city IS NOT NULL AND city != ''
             GROUP BY city, state
             ORDER BY cnt DESC"
                .to_string(),
            vec![],
        )
    };

    let rows = db.query_raw_params(&sql, &db_params).await?;

    let cities: Vec<CityInfo> = rows
        .iter()
        .map(|row| {
            let city: String = row.to_value("city").unwrap_or_default();
            let state: String = row.to_value("state").unwrap_or_default();
            let cnt: i64 = row.to_value("cnt").unwrap_or(0);
            CityInfo {
                city,
                state,
                #[allow(clippy::cast_sign_loss)]
                incident_count: Some(cnt as u64),
            }
        })
        .collect();

    Ok(ListCitiesResult { cities })
}

/// Searches for available locations matching a query string.
///
/// Performs a case-insensitive partial match against city/county names
/// in the dataset. Returns matching locations with incident counts,
/// ordered by relevance (exact matches first, then prefix matches,
/// then substring matches).
///
/// # Errors
///
/// Returns [`AnalyticsError`] if the database query fails.
#[allow(clippy::too_many_lines)]
pub async fn search_locations(
    db: &dyn Database,
    params: &SearchLocationsParams,
) -> Result<SearchLocationsResult, AnalyticsError> {
    let query = params.query.trim();

    if query.is_empty() {
        return Ok(SearchLocationsResult {
            matches: Vec::new(),
            places: Vec::new(),
            description: "Empty search query".to_string(),
        });
    }

    // Build query with optional state filter.
    // Use ILIKE for case-insensitive matching. Rank results by match quality:
    //   1. Exact match (city ILIKE query)
    //   2. Prefix match (city ILIKE query%)
    //   3. Substring match (city ILIKE %query%)
    let mut idx = 1u32;
    let mut frags = Vec::new();
    let mut db_params: Vec<DatabaseValue> = Vec::new();

    // The query value is used in the ILIKE pattern
    let like_pattern = format!("%{query}%");
    frags.push(format!("city ILIKE ${idx}"));
    db_params.push(DatabaseValue::String(like_pattern));
    idx += 1;

    if let Some(ref state) = params.state {
        frags.push(format!("state = ${idx}"));
        db_params.push(DatabaseValue::String(state.to_uppercase()));
        idx += 1;
    }

    let _ = idx;
    let where_clause = frags.join(" AND ");

    // Use a CASE expression to sort by match quality
    let exact_pattern = query.to_string();
    let prefix_pattern = format!("{query}%");
    db_params.push(DatabaseValue::String(exact_pattern));
    db_params.push(DatabaseValue::String(prefix_pattern));

    let exact_idx = idx;
    let prefix_idx = idx + 1;

    let sql = format!(
        "SELECT city, state, COUNT(*) as cnt,
                CASE
                    WHEN city ILIKE ${exact_idx} THEN 0
                    WHEN city ILIKE ${prefix_idx} THEN 1
                    ELSE 2
                END as match_rank
         FROM crime_incidents
         WHERE city IS NOT NULL AND city != '' AND {where_clause}
         GROUP BY city, state
         ORDER BY match_rank, cnt DESC
         LIMIT 10"
    );

    let rows = db.query_raw_params(&sql, &db_params).await?;

    let matches: Vec<CityInfo> = rows
        .iter()
        .map(|row| {
            let city: String = row.to_value("city").unwrap_or_default();
            let state: String = row.to_value("state").unwrap_or_default();
            let cnt: i64 = row.to_value("cnt").unwrap_or(0);
            CityInfo {
                city,
                state,
                #[allow(clippy::cast_sign_loss)]
                incident_count: Some(cnt as u64),
            }
        })
        .collect();

    let description = if matches.is_empty() {
        format!("No locations found matching \"{query}\"")
    } else {
        format!("Found {} location(s) matching \"{query}\"", matches.len())
    };

    // Also search census_places for matching incorporated cities, towns, CDPs
    let mut place_params: Vec<DatabaseValue> = Vec::new();
    let place_like = format!("%{query}%");
    place_params.push(DatabaseValue::String(place_like));

    #[allow(clippy::option_if_let_else)]
    let place_state_filter = if let Some(ref state) = params.state {
        place_params.push(DatabaseValue::String(state.to_uppercase()));
        "AND state_abbr = $2"
    } else {
        ""
    };

    let place_sql = format!(
        "SELECT geoid, name, full_name, state_abbr, place_type, population, land_area_sq_mi
         FROM census_places
         WHERE name ILIKE $1 {place_state_filter}
         ORDER BY population DESC NULLS LAST
         LIMIT 10"
    );

    let place_rows = db.query_raw_params(&place_sql, &place_params).await?;

    let places: Vec<PlaceInfo> = place_rows
        .iter()
        .map(|row| {
            let geoid: String = row.to_value("geoid").unwrap_or_default();
            let name: String = row.to_value("name").unwrap_or_default();
            let full_name: String = row.to_value("full_name").unwrap_or_default();
            let state: String = row.to_value("state_abbr").unwrap_or_default();
            let place_type: String = row.to_value("place_type").unwrap_or_default();
            let population: Option<i32> = row.to_value("population").unwrap_or(None);
            let land_area_sq_mi: Option<f64> = row.to_value("land_area_sq_mi").unwrap_or(None);
            PlaceInfo {
                geoid,
                name,
                full_name,
                state,
                place_type,
                population: population.map(i64::from),
                land_area_sq_mi,
            }
        })
        .collect();

    let full_description = if places.is_empty() {
        description
    } else {
        format!(
            "{description}. Also found {} Census place(s) with boundaries",
            places.len()
        )
    };

    Ok(SearchLocationsResult {
        matches,
        places,
        description: full_description,
    })
}
