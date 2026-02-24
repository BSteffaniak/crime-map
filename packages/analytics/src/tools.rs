//! Tool execution functions for the AI agent.
//!
//! Each function implements one of the analytical tools that the agent
//! can invoke. They run optimized SQL against the pre-generated
//! `analytics.duckdb` database and return typed results.

use crime_map_analytics_models::{
    CityInfo, ComparePeriodParams, ComparePeriodResult, CountIncidentsParams, CountIncidentsResult,
    ListCitiesParams, ListCitiesResult, PlaceInfo, RankAreaParams, RankAreaResult,
    SearchLocationsParams, SearchLocationsResult, TimeGranularity, TopCrimeTypesParams,
    TopCrimeTypesResult, TrendParams, TrendResult,
};
use crime_map_geography_models::{AreaStats, CategoryCount, PeriodComparison, TimeSeriesPoint};

use crate::AnalyticsError;

/// A bind value for `DuckDB` queries.
///
/// `duckdb::Connection` uses `?` positional placeholders and requires
/// `dyn duckdb::ToSql` references.
enum DuckValue {
    Str(String),
    Int(i32),
    Timestamp(String),
}

fn duck_value_to_boxed(v: DuckValue) -> Box<dyn duckdb::ToSql> {
    match v {
        DuckValue::Str(s) | DuckValue::Timestamp(s) => Box::new(s),
        DuckValue::Int(i) => Box::new(i),
    }
}

/// Parses a date string like `"2024-01-01"` into a timestamp string
/// suitable for `DuckDB` comparison.
fn parse_date(s: &str) -> Result<String, AnalyticsError> {
    // Validate format, then return as-is for DuckDB CAST
    if chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").is_ok()
        || chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok()
    {
        Ok(s.to_string())
    } else {
        Err(AnalyticsError::Query {
            message: format!("Invalid date '{s}': Expected format: YYYY-MM-DD"),
        })
    }
}

/// Builds a WHERE clause fragment and parameter list for common filters.
///
/// Returns `(where_fragments, params)`.
///
/// In the analytics `DuckDB`, incidents have pre-resolved text columns:
/// - `city`, `state` (text, for ILIKE / = filtering)
/// - `category`, `subcategory` (text names)
/// - `census_tract_geoid`, `census_place_geoid` (GEOIDs)
/// - `occurred_at` (TIMESTAMP)
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
) -> Result<(Vec<String>, Vec<DuckValue>), AnalyticsError> {
    let mut frags = Vec::new();
    let mut params: Vec<DuckValue> = Vec::new();

    if let Some(city) = city {
        frags.push("i.city ILIKE ?".to_string());
        params.push(DuckValue::Str(city.to_string()));
    }

    if let Some(state) = state {
        frags.push("i.state = ?".to_string());
        params.push(DuckValue::Str(state.to_uppercase()));
    }

    if let Some(geoid) = geoid {
        frags.push("i.census_tract_geoid = ?".to_string());
        params.push(DuckValue::Str(geoid.to_string()));
    }

    if let Some(place_geoid) = place_geoid {
        frags.push("i.census_place_geoid = ?".to_string());
        params.push(DuckValue::Str(place_geoid.to_string()));
    }

    if let Some(from) = date_from {
        let ts = parse_date(from)?;
        frags.push("i.occurred_at >= CAST(? AS TIMESTAMP)".to_string());
        params.push(DuckValue::Timestamp(ts));
    }

    if let Some(to) = date_to {
        let ts = parse_date(to)?;
        frags.push("i.occurred_at <= CAST(? AS TIMESTAMP)".to_string());
        params.push(DuckValue::Timestamp(ts));
    }

    if let Some(cat) = category {
        frags.push("i.category = ?".to_string());
        params.push(DuckValue::Str(cat.to_uppercase()));
    }

    if let Some(sub) = subcategory {
        frags.push("i.subcategory = ?".to_string());
        params.push(DuckValue::Str(sub.to_uppercase()));
    }

    if let Some(sev) = severity_min
        && sev > 1
    {
        frags.push("i.severity >= ?".to_string());
        params.push(DuckValue::Int(i32::from(sev)));
    }

    Ok((frags, params))
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

/// Helper: convert `Vec<DuckValue>` into the ref-slice `DuckDB` expects.
fn prepare_params(params: &[Box<dyn duckdb::ToSql>]) -> Vec<&dyn duckdb::ToSql> {
    params.iter().map(AsRef::as_ref).collect()
}

/// Counts incidents matching the given filters.
///
/// # Errors
///
/// Returns [`AnalyticsError`] if the database query fails.
pub fn count_incidents(
    db: &duckdb::Connection,
    params: &CountIncidentsParams,
) -> Result<CountIncidentsResult, AnalyticsError> {
    let (frags, db_params) = build_common_filters(
        params.city.as_deref(),
        params.state.as_deref(),
        params.geoid.as_deref(),
        params.place_geoid.as_deref(),
        params.date_from.as_deref(),
        params.date_to.as_deref(),
        params.category.as_deref(),
        params.subcategory.as_deref(),
        params.severity_min,
    )?;

    let wc = where_clause(&frags);
    let boxed: Vec<Box<dyn duckdb::ToSql>> =
        db_params.into_iter().map(duck_value_to_boxed).collect();
    let refs = prepare_params(&boxed);

    // Total count
    let count_sql = format!("SELECT COUNT(*) as total FROM incidents i{wc}");
    let total: i64 = db
        .prepare(&count_sql)?
        .query_row(refs.as_slice(), |row| row.get(0))?;

    // Category breakdown
    let cat_sql = format!(
        "SELECT i.category, COUNT(*) as cnt
         FROM incidents i{wc}
         GROUP BY i.category
         ORDER BY cnt DESC"
    );
    let mut cat_stmt = db.prepare(&cat_sql)?;
    let cat_rows = cat_stmt.query_map(refs.as_slice(), |row| {
        let cat: String = row.get(0)?;
        let cnt: i64 = row.get(1)?;
        Ok((cat, cnt))
    })?;

    let by_category: Vec<CategoryCount> = cat_rows
        .filter_map(Result::ok)
        .map(|(category, cnt)| CategoryCount {
            category,
            #[allow(clippy::cast_sign_loss)]
            count: cnt as u64,
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
pub fn rank_areas(
    db: &duckdb::Connection,
    params: &RankAreaParams,
) -> Result<RankAreaResult, AnalyticsError> {
    let limit = params.limit.unwrap_or(10);
    let safest_first = params.safest_first.unwrap_or(true);

    let mut frags = Vec::new();
    let mut db_params: Vec<DuckValue> = Vec::new();

    // Either city or placeGeoid is required to scope the ranking
    if let Some(ref place_geoid) = params.place_geoid {
        frags.push("i.census_place_geoid = ?".to_string());
        db_params.push(DuckValue::Str(place_geoid.clone()));
    } else if let Some(ref city) = params.city {
        frags.push("i.city ILIKE ?".to_string());
        db_params.push(DuckValue::Str(city.clone()));
    } else {
        return Err(AnalyticsError::Query {
            message: "Either 'city' or 'placeGeoid' is required for rank_areas".to_string(),
        });
    }

    if let Some(ref state) = params.state {
        frags.push("i.state = ?".to_string());
        db_params.push(DuckValue::Str(state.to_uppercase()));
    }

    if let Some(ref from) = params.date_from {
        let ts = parse_date(from)?;
        frags.push("i.occurred_at >= CAST(? AS TIMESTAMP)".to_string());
        db_params.push(DuckValue::Timestamp(ts));
    }

    if let Some(ref to) = params.date_to {
        let ts = parse_date(to)?;
        frags.push("i.occurred_at <= CAST(? AS TIMESTAMP)".to_string());
        db_params.push(DuckValue::Timestamp(ts));
    }

    if let Some(ref cat) = params.category {
        frags.push("i.category = ?".to_string());
        db_params.push(DuckValue::Str(cat.to_uppercase()));
    }

    let wc = where_clause(&frags);
    let boxed: Vec<Box<dyn duckdb::ToSql>> =
        db_params.into_iter().map(duck_value_to_boxed).collect();
    let refs = prepare_params(&boxed);

    // Pre-check: bail out if no matching incidents have tract data
    let check_sql = format!(
        "SELECT COUNT(*) > 0 FROM incidents i{wc} AND i.census_tract_geoid IS NOT NULL LIMIT 1"
    );
    // If wc is empty, we need WHERE instead of AND
    let check_sql = if wc.is_empty() {
        "SELECT COUNT(*) > 0 FROM incidents i WHERE i.census_tract_geoid IS NOT NULL LIMIT 1"
            .to_string()
    } else {
        check_sql
    };
    let has_tract_data: bool = db
        .prepare(&check_sql)?
        .query_row(refs.as_slice(), |row| row.get(0))?;

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

    // Main query: join incidents with census_tracts and neighborhoods
    let tract_not_null = if wc.is_empty() {
        " WHERE i.census_tract_geoid IS NOT NULL"
    } else {
        " AND i.census_tract_geoid IS NOT NULL"
    };

    let sql = format!(
        "SELECT ct.geoid,
                COALESCE(n.name, ct.geoid) as area_id,
                COALESCE(n.name, ct.name) as area_name,
                ct.population, ct.land_area_sq_mi,
                i.category, COUNT(*) as cat_cnt
         FROM incidents i
         JOIN census_tracts ct ON ct.geoid = i.census_tract_geoid
         LEFT JOIN tract_neighborhoods tn ON ct.geoid = tn.geoid
         LEFT JOIN neighborhoods n ON tn.neighborhood_id = n.id
         {wc}{tract_not_null}
         GROUP BY ct.geoid, COALESCE(n.name, ct.geoid), COALESCE(n.name, ct.name),
                  ct.population, ct.land_area_sq_mi, i.category"
    );

    let mut stmt = db.prepare(&sql)?;
    let rows = stmt.query_map(refs.as_slice(), |row| {
        let geoid: String = row.get(0)?;
        let area_id: String = row.get(1)?;
        let area_name: String = row.get(2)?;
        let population: Option<i32> = row.get(3)?;
        let land_area: Option<f64> = row.get(4)?;
        let cat: String = row.get(5)?;
        let cat_cnt: i64 = row.get(6)?;
        Ok((
            geoid, area_id, area_name, population, land_area, cat, cat_cnt,
        ))
    })?;

    // Aggregate rows by area
    #[allow(clippy::items_after_statements)]
    struct AreaAccum {
        area_name: String,
        total_incidents: u64,
        total_population: i64,
        total_land_area: f64,
        by_category: std::collections::BTreeMap<String, u64>,
        seen_geoids: std::collections::BTreeSet<String>,
    }

    let mut area_map: std::collections::BTreeMap<String, AreaAccum> =
        std::collections::BTreeMap::new();

    for row_result in rows {
        let (geoid, area_id, area_name, population, land_area, cat, cat_cnt) = row_result?;

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

        if entry.seen_geoids.insert(geoid) {
            if let Some(pop) = population {
                entry.total_population += i64::from(pop);
            }
            if let Some(area) = land_area {
                entry.total_land_area += area;
            }
        }
    }

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

    areas.sort_by(|a, b| {
        let cmp = match (a.incidents_per_1k, b.incidents_per_1k) {
            (Some(a_rate), Some(b_rate)) => a_rate
                .partial_cmp(&b_rate)
                .unwrap_or(std::cmp::Ordering::Equal),
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
/// Uses conditional aggregation (`COUNT(*) FILTER`) to compare both
/// periods in a single table scan.
///
/// # Errors
///
/// Returns [`AnalyticsError`] if the database query fails.
#[allow(clippy::too_many_lines)]
pub fn compare_periods(
    db: &duckdb::Connection,
    params: &ComparePeriodParams,
) -> Result<ComparePeriodResult, AnalyticsError> {
    let a_from = parse_date(&params.period_a_from)?;
    let a_to = parse_date(&params.period_a_to)?;
    let b_from = parse_date(&params.period_b_from)?;
    let b_to = parse_date(&params.period_b_to)?;

    // Build base location/category filters
    let mut base_frags = Vec::new();
    let mut base_params: Vec<DuckValue> = Vec::new();

    if let Some(ref city) = params.city {
        base_frags.push("i.city ILIKE ?".to_string());
        base_params.push(DuckValue::Str(city.clone()));
    }
    if let Some(ref state) = params.state {
        base_frags.push("i.state = ?".to_string());
        base_params.push(DuckValue::Str(state.to_uppercase()));
    }
    if let Some(ref geoid) = params.geoid {
        base_frags.push("i.census_tract_geoid = ?".to_string());
        base_params.push(DuckValue::Str(geoid.clone()));
    }
    if let Some(ref place_geoid) = params.place_geoid {
        base_frags.push("i.census_place_geoid = ?".to_string());
        base_params.push(DuckValue::Str(place_geoid.clone()));
    }
    if let Some(ref cat) = params.category {
        base_frags.push("i.category = ?".to_string());
        base_params.push(DuckValue::Str(cat.to_uppercase()));
    }

    // Outer date range covering both periods
    let date_lo = if a_from <= b_from {
        a_from.clone()
    } else {
        b_from.clone()
    };
    let date_hi = if a_to >= b_to {
        a_to.clone()
    } else {
        b_to.clone()
    };

    base_frags.push("i.occurred_at >= CAST(? AS TIMESTAMP)".to_string());
    base_frags.push("i.occurred_at <= CAST(? AS TIMESTAMP)".to_string());
    base_params.push(DuckValue::Timestamp(date_lo));
    base_params.push(DuckValue::Timestamp(date_hi));

    let wc = where_clause(&base_frags);

    // Helper to build params in SQL text order.
    // SQL has FILTER(a_from, a_to, b_from, b_to) then WHERE(base_params).
    // `DuckDB` binds ? left-to-right in SQL text order.
    let build_params = |base: &[DuckValue]| -> Vec<Box<dyn duckdb::ToSql>> {
        let mut p: Vec<Box<dyn duckdb::ToSql>> = vec![
            duck_value_to_boxed(DuckValue::Timestamp(a_from.clone())),
            duck_value_to_boxed(DuckValue::Timestamp(a_to.clone())),
            duck_value_to_boxed(DuckValue::Timestamp(b_from.clone())),
            duck_value_to_boxed(DuckValue::Timestamp(b_to.clone())),
        ];
        for v in base {
            p.push(duck_value_to_boxed(match v {
                DuckValue::Str(s) => DuckValue::Str(s.clone()),
                DuckValue::Int(i) => DuckValue::Int(*i),
                DuckValue::Timestamp(s) => DuckValue::Timestamp(s.clone()),
            }));
        }
        p
    };

    // Overall totals
    let totals_sql = format!(
        "SELECT
           COUNT(*) FILTER (WHERE i.occurred_at >= CAST(? AS TIMESTAMP) AND i.occurred_at <= CAST(? AS TIMESTAMP)) as a_total,
           COUNT(*) FILTER (WHERE i.occurred_at >= CAST(? AS TIMESTAMP) AND i.occurred_at <= CAST(? AS TIMESTAMP)) as b_total
         FROM incidents i{wc}"
    );

    let totals_boxed = build_params(&base_params);
    let totals_refs = prepare_params(&totals_boxed);

    let (a_total, b_total): (i64, i64) = db
        .prepare(&totals_sql)?
        .query_row(totals_refs.as_slice(), |row| Ok((row.get(0)?, row.get(1)?)))?;

    // Per-category breakdown
    let cat_sql = format!(
        "SELECT i.category,
                COUNT(*) FILTER (WHERE i.occurred_at >= CAST(? AS TIMESTAMP) AND i.occurred_at <= CAST(? AS TIMESTAMP)) as a_cnt,
                COUNT(*) FILTER (WHERE i.occurred_at >= CAST(? AS TIMESTAMP) AND i.occurred_at <= CAST(? AS TIMESTAMP)) as b_cnt
         FROM incidents i{wc}
         GROUP BY i.category
         ORDER BY a_cnt DESC"
    );

    let cat_boxed = build_params(&base_params);
    let cat_refs = prepare_params(&cat_boxed);

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
    let mut cat_stmt = db.prepare(&cat_sql)?;
    let cat_rows = cat_stmt.query_map(cat_refs.as_slice(), |row| {
        let cat_name: String = row.get(0)?;
        let a_cnt: i64 = row.get(1)?;
        let b_cnt: i64 = row.get(2)?;
        Ok((cat_name, a_cnt, b_cnt))
    })?;

    for row_result in cat_rows {
        let (cat_name, a_cnt, b_cnt) = row_result?;

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
pub fn get_trend(
    db: &duckdb::Connection,
    params: &TrendParams,
) -> Result<TrendResult, AnalyticsError> {
    let trunc = match params.granularity {
        TimeGranularity::Daily => "day",
        TimeGranularity::Weekly => "week",
        TimeGranularity::Monthly => "month",
        TimeGranularity::Yearly => "year",
    };

    let (frags, db_params) = build_common_filters(
        params.city.as_deref(),
        params.state.as_deref(),
        params.geoid.as_deref(),
        params.place_geoid.as_deref(),
        params.date_from.as_deref(),
        params.date_to.as_deref(),
        params.category.as_deref(),
        None,
        None,
    )?;

    let wc = where_clause(&frags);
    let and_or_where = if wc.is_empty() { " WHERE" } else { " AND" };

    let sql = format!(
        "SELECT CAST(date_trunc('{trunc}', i.occurred_at) AS VARCHAR) as period, COUNT(*) as cnt
         FROM incidents i
         {wc}{and_or_where} i.occurred_at IS NOT NULL
         GROUP BY period
         ORDER BY period"
    );

    let boxed: Vec<Box<dyn duckdb::ToSql>> =
        db_params.into_iter().map(duck_value_to_boxed).collect();
    let refs = prepare_params(&boxed);

    let mut stmt = db.prepare(&sql)?;
    let rows = stmt.query_map(refs.as_slice(), |row| {
        let period: String = row.get(0)?;
        let cnt: i64 = row.get(1)?;
        Ok((period, cnt))
    })?;

    let data: Vec<TimeSeriesPoint> = rows
        .filter_map(Result::ok)
        .map(|(period, cnt)| {
            // Trim the time component if present (e.g., "2024-01-01 00:00:00" -> "2024-01-01")
            let period = period
                .split_whitespace()
                .next()
                .unwrap_or(&period)
                .to_string();
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
        description: format!("{} crime trend for {area}", params.granularity),
        data,
    })
}

/// Finds the most common crime types in an area.
///
/// # Errors
///
/// Returns [`AnalyticsError`] if the database query fails.
#[allow(clippy::too_many_lines)]
pub fn top_crime_types(
    db: &duckdb::Connection,
    params: &TopCrimeTypesParams,
) -> Result<TopCrimeTypesResult, AnalyticsError> {
    let limit = params.limit.unwrap_or(10);

    let (frags, _) = build_common_filters(
        params.city.as_deref(),
        params.state.as_deref(),
        params.geoid.as_deref(),
        params.place_geoid.as_deref(),
        params.date_from.as_deref(),
        params.date_to.as_deref(),
        None,
        None,
        None,
    )?;

    let wc = where_clause(&frags);

    // By subcategory (need limit param appended)
    let sub_sql = format!(
        "SELECT i.subcategory, COUNT(*) as cnt
         FROM incidents i{wc}
         GROUP BY i.subcategory
         ORDER BY cnt DESC
         LIMIT ?"
    );

    // Rebuild params with limit appended for subcategory query
    let (_, sub_db_params) = build_common_filters(
        params.city.as_deref(),
        params.state.as_deref(),
        params.geoid.as_deref(),
        params.place_geoid.as_deref(),
        params.date_from.as_deref(),
        params.date_to.as_deref(),
        None,
        None,
        None,
    )?;
    let mut sub_boxed: Vec<Box<dyn duckdb::ToSql>> =
        sub_db_params.into_iter().map(duck_value_to_boxed).collect();
    sub_boxed.push(Box::new(i64::from(limit)));
    let sub_refs = prepare_params(&sub_boxed);

    let mut sub_stmt = db.prepare(&sub_sql)?;
    let sub_rows = sub_stmt.query_map(sub_refs.as_slice(), |row| {
        let name: String = row.get(0)?;
        let cnt: i64 = row.get(1)?;
        Ok((name, cnt))
    })?;

    let subcategories: Vec<CategoryCount> = sub_rows
        .filter_map(Result::ok)
        .map(|(category, cnt)| CategoryCount {
            category,
            #[allow(clippy::cast_sign_loss)]
            count: cnt as u64,
        })
        .collect();

    // By category
    let cat_sql = format!(
        "SELECT i.category, COUNT(*) as cnt
         FROM incidents i{wc}
         GROUP BY i.category
         ORDER BY cnt DESC"
    );

    let (_, cat_db_params) = build_common_filters(
        params.city.as_deref(),
        params.state.as_deref(),
        params.geoid.as_deref(),
        params.place_geoid.as_deref(),
        params.date_from.as_deref(),
        params.date_to.as_deref(),
        None,
        None,
        None,
    )?;
    let cat_boxed: Vec<Box<dyn duckdb::ToSql>> =
        cat_db_params.into_iter().map(duck_value_to_boxed).collect();
    let cat_refs = prepare_params(&cat_boxed);

    let mut cat_stmt = db.prepare(&cat_sql)?;
    let cat_rows = cat_stmt.query_map(cat_refs.as_slice(), |row| {
        let name: String = row.get(0)?;
        let cnt: i64 = row.get(1)?;
        Ok((name, cnt))
    })?;

    let categories: Vec<CategoryCount> = cat_rows
        .filter_map(Result::ok)
        .map(|(category, cnt)| CategoryCount {
            category,
            #[allow(clippy::cast_sign_loss)]
            count: cnt as u64,
        })
        .collect();

    // Total
    let total_sql = format!("SELECT COUNT(*) as total FROM incidents i{wc}");
    let (_, total_db_params) = build_common_filters(
        params.city.as_deref(),
        params.state.as_deref(),
        params.geoid.as_deref(),
        params.place_geoid.as_deref(),
        params.date_from.as_deref(),
        params.date_to.as_deref(),
        None,
        None,
        None,
    )?;
    let total_boxed: Vec<Box<dyn duckdb::ToSql>> = total_db_params
        .into_iter()
        .map(duck_value_to_boxed)
        .collect();
    let total_refs = prepare_params(&total_boxed);
    let total: i64 = db
        .prepare(&total_sql)?
        .query_row(total_refs.as_slice(), |row| row.get(0))?;

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
pub fn list_cities(
    db: &duckdb::Connection,
    params: &ListCitiesParams,
) -> Result<ListCitiesResult, AnalyticsError> {
    let (sql, bind_params): (String, Vec<Box<dyn duckdb::ToSql>>) =
        if let Some(ref state) = params.state {
            (
                "SELECT city, state, COUNT(*) as cnt
             FROM incidents
             WHERE city IS NOT NULL AND city != '' AND state = ?
             GROUP BY city, state
             ORDER BY cnt DESC"
                    .to_string(),
                vec![Box::new(state.to_uppercase()) as Box<dyn duckdb::ToSql>],
            )
        } else {
            (
                "SELECT city, state, COUNT(*) as cnt
             FROM incidents
             WHERE city IS NOT NULL AND city != ''
             GROUP BY city, state
             ORDER BY cnt DESC"
                    .to_string(),
                vec![],
            )
        };

    let refs: Vec<&dyn duckdb::ToSql> = bind_params.iter().map(AsRef::as_ref).collect();
    let mut stmt = db.prepare(&sql)?;
    let rows = stmt.query_map(refs.as_slice(), |row| {
        let city: String = row.get(0)?;
        let state: String = row.get(1)?;
        let cnt: i64 = row.get(2)?;
        Ok((city, state, cnt))
    })?;

    let cities: Vec<CityInfo> = rows
        .filter_map(Result::ok)
        .map(|(city, state, cnt)| CityInfo {
            city,
            state,
            #[allow(clippy::cast_sign_loss)]
            incident_count: Some(cnt as u64),
        })
        .collect();

    Ok(ListCitiesResult { cities })
}

/// Searches for available locations matching a query string.
///
/// # Errors
///
/// Returns [`AnalyticsError`] if the database query fails.
#[allow(clippy::too_many_lines, clippy::option_if_let_else)]
pub fn search_locations(
    db: &duckdb::Connection,
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

    let like_pattern = format!("%{query}%");
    let exact_pattern = query.to_string();
    let prefix_pattern = format!("{query}%");

    // Build params in SQL text order:
    // CASE (exact_pattern, prefix_pattern) then WHERE (like_pattern, [state])
    let mut bind_params: Vec<Box<dyn duckdb::ToSql>> = Vec::new();
    bind_params.push(Box::new(exact_pattern));
    bind_params.push(Box::new(prefix_pattern));
    bind_params.push(Box::new(like_pattern));

    let state_filter = if let Some(ref state) = params.state {
        bind_params.push(Box::new(state.to_uppercase()));
        "AND state = ?"
    } else {
        ""
    };

    let sql = format!(
        "SELECT city, state, COUNT(*) as cnt,
                CASE
                    WHEN city ILIKE ? THEN 0
                    WHEN city ILIKE ? THEN 1
                    ELSE 2
                END as match_rank
         FROM incidents
         WHERE city IS NOT NULL AND city != '' AND city ILIKE ? {state_filter}
         GROUP BY city, state
         ORDER BY match_rank, cnt DESC
         LIMIT 10"
    );

    let refs: Vec<&dyn duckdb::ToSql> = bind_params.iter().map(AsRef::as_ref).collect();
    let mut stmt = db.prepare(&sql)?;
    let rows = stmt.query_map(refs.as_slice(), |row| {
        let city: String = row.get(0)?;
        let state: String = row.get(1)?;
        let cnt: i64 = row.get(2)?;
        Ok((city, state, cnt))
    })?;

    let matches: Vec<CityInfo> = rows
        .filter_map(Result::ok)
        .map(|(city, state, cnt)| CityInfo {
            city,
            state,
            #[allow(clippy::cast_sign_loss)]
            incident_count: Some(cnt as u64),
        })
        .collect();

    let description = if matches.is_empty() {
        format!("No locations found matching \"{query}\"")
    } else {
        format!("Found {} location(s) matching \"{query}\"", matches.len())
    };

    // Also search census_places for matching places
    let place_like = format!("%{query}%");
    let mut place_params: Vec<Box<dyn duckdb::ToSql>> = Vec::new();
    place_params.push(Box::new(place_like));

    let place_state_filter = if let Some(ref state) = params.state {
        place_params.push(Box::new(state.to_uppercase()));
        "AND state_abbr = ?"
    } else {
        ""
    };

    let place_sql = format!(
        "SELECT geoid, name, full_name, state_abbr, place_type, population, land_area_sq_mi
         FROM census_places
         WHERE name ILIKE ? {place_state_filter}
         ORDER BY population DESC NULLS LAST
         LIMIT 10"
    );

    let place_refs: Vec<&dyn duckdb::ToSql> = place_params.iter().map(AsRef::as_ref).collect();
    let mut place_stmt = db.prepare(&place_sql)?;
    let place_rows = place_stmt.query_map(place_refs.as_slice(), |row| {
        let geoid: String = row.get(0)?;
        let name: String = row.get(1)?;
        let full_name: String = row.get(2)?;
        let state: String = row.get::<_, Option<String>>(3)?.unwrap_or_default();
        let place_type: String = row.get::<_, Option<String>>(4)?.unwrap_or_default();
        let population: Option<i32> = row.get(5)?;
        let land_area_sq_mi: Option<f64> = row.get(6)?;
        Ok(PlaceInfo {
            geoid,
            name,
            full_name,
            state,
            place_type,
            population: population.map(i64::from),
            land_area_sq_mi,
        })
    })?;

    let places: Vec<PlaceInfo> = place_rows.filter_map(Result::ok).collect();

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
