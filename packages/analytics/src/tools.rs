//! Tool execution functions for the AI agent.
//!
//! Each function implements one of the analytical tools that the agent
//! can invoke. They run optimized SQL against `PostGIS` and return typed
//! results.

use crime_map_analytics_models::{
    CityInfo, ComparePeriodParams, ComparePeriodResult, CountIncidentsParams, CountIncidentsResult,
    ListCitiesParams, ListCitiesResult, RankAreaParams, RankAreaResult, TimeGranularity,
    TopCrimeTypesParams, TopCrimeTypesResult, TrendParams, TrendResult,
};
use crime_map_geography_models::{AreaStats, CategoryCount, PeriodComparison, TimeSeriesPoint};
use moosicbox_json_utils::database::ToValue as _;
use switchy_database::{Database, DatabaseValue};

use crate::AnalyticsError;

/// Builds a WHERE clause fragment and parameter list for the common
/// city/state/geoid/date/category/severity filters.
///
/// Returns `(where_fragments, params, next_param_index)`.
#[allow(clippy::too_many_arguments)]
fn build_common_filters(
    city: Option<&str>,
    state: Option<&str>,
    geoid: Option<&str>,
    date_from: Option<&str>,
    date_to: Option<&str>,
    category: Option<&str>,
    subcategory: Option<&str>,
    severity_min: Option<u8>,
    start_idx: u32,
) -> (Vec<String>, Vec<DatabaseValue>, u32) {
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
        frags.push(format!(
            "ST_Covers((SELECT boundary FROM census_tracts WHERE geoid = ${idx}), i.location)"
        ));
        params.push(DatabaseValue::String(geoid.to_string()));
        idx += 1;
    }

    if let Some(from) = date_from {
        frags.push(format!("i.occurred_at >= ${idx}::timestamptz"));
        params.push(DatabaseValue::String(from.to_string()));
        idx += 1;
    }

    if let Some(to) = date_to {
        frags.push(format!("i.occurred_at <= ${idx}::timestamptz"));
        params.push(DatabaseValue::String(to.to_string()));
        idx += 1;
    }

    if let Some(cat) = category {
        frags.push(format!(
            "c.parent_id = (SELECT id FROM crime_categories WHERE name = ${idx} AND parent_id IS NULL)"
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

    (frags, params, idx)
}

fn where_clause(frags: &[String]) -> String {
    if frags.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", frags.join(" AND "))
    }
}

#[allow(clippy::option_if_let_else)]
fn describe_area(city: Option<&str>, state: Option<&str>, geoid: Option<&str>) -> String {
    if let Some(geoid) = geoid {
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
        params.date_from.as_deref(),
        params.date_to.as_deref(),
        params.category.as_deref(),
        params.subcategory.as_deref(),
        params.severity_min,
        1,
    );

    let wc = where_clause(&frags);

    // Total count
    let count_sql = format!(
        "SELECT COUNT(*) as total
         FROM crime_incidents i
         JOIN crime_categories c ON i.category_id = c.id
         {wc}"
    );

    let rows = db.query_raw_params(&count_sql, &db_params).await?;
    let total: i64 = rows.first().map_or(0, |r| r.to_value("total").unwrap_or(0));

    // Category breakdown
    let cat_sql = format!(
        "SELECT pc.name as category, COUNT(*) as cnt
         FROM crime_incidents i
         JOIN crime_categories c ON i.category_id = c.id
         LEFT JOIN crime_categories pc ON c.parent_id = pc.id
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

    // City filter
    frags.push(format!("i.city ILIKE ${idx}"));
    db_params.push(DatabaseValue::String(params.city.clone()));
    idx += 1;

    if let Some(ref state) = params.state {
        frags.push(format!("i.state = ${idx}"));
        db_params.push(DatabaseValue::String(state.to_uppercase()));
        idx += 1;
    }

    if let Some(ref from) = params.date_from {
        frags.push(format!("i.occurred_at >= ${idx}::timestamptz"));
        db_params.push(DatabaseValue::String(from.clone()));
        idx += 1;
    }

    if let Some(ref to) = params.date_to {
        frags.push(format!("i.occurred_at <= ${idx}::timestamptz"));
        db_params.push(DatabaseValue::String(to.clone()));
        idx += 1;
    }

    if let Some(ref cat) = params.category {
        frags.push(format!(
            "c.parent_id = (SELECT id FROM crime_categories WHERE name = ${idx} AND parent_id IS NULL)"
        ));
        db_params.push(DatabaseValue::String(cat.to_uppercase()));
        idx += 1;
    }

    let _ = idx; // suppress unused-after-increment; kept for safety if filters are added
    let wc = where_clause(&frags);

    let sql = format!(
        "SELECT ct.geoid,
                COALESCE(n.name, ct.geoid) as area_id,
                COALESCE(n.name, ct.name) as area_name,
                ct.population, ct.land_area_sq_mi,
                pc.name as category, COUNT(*) as cat_cnt
         FROM crime_incidents i
         JOIN crime_categories c ON i.category_id = c.id
         LEFT JOIN crime_categories pc ON c.parent_id = pc.id
         JOIN census_tracts ct ON ST_Covers(ct.boundary, i.location)
         LEFT JOIN tract_neighborhoods tn ON ct.geoid = tn.geoid
         LEFT JOIN neighborhoods n ON tn.neighborhood_id = n.id
         {wc}
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

    Ok(RankAreaResult {
        description: format!("Top {} {label} areas in {}", areas.len(), params.city,),
        areas,
    })
}

/// Compares crime between two time periods.
///
/// # Errors
///
/// Returns [`AnalyticsError`] if the database query fails.
pub async fn compare_periods(
    db: &dyn Database,
    params: &ComparePeriodParams,
) -> Result<ComparePeriodResult, AnalyticsError> {
    // Count for period A
    let count_a = count_incidents(
        db,
        &CountIncidentsParams {
            city: params.city.clone(),
            state: params.state.clone(),
            geoid: params.geoid.clone(),
            date_from: Some(params.period_a_from.clone()),
            date_to: Some(params.period_a_to.clone()),
            category: params.category.clone(),
            subcategory: None,
            severity_min: None,
        },
    )
    .await?;

    // Count for period B
    let count_b = count_incidents(
        db,
        &CountIncidentsParams {
            city: params.city.clone(),
            state: params.state.clone(),
            geoid: params.geoid.clone(),
            date_from: Some(params.period_b_from.clone()),
            date_to: Some(params.period_b_to.clone()),
            category: params.category.clone(),
            subcategory: None,
            severity_min: None,
        },
    )
    .await?;

    let area_desc = describe_area(
        params.city.as_deref(),
        params.state.as_deref(),
        params.geoid.as_deref(),
    );

    #[allow(clippy::cast_precision_loss)]
    let percent_change = if count_a.total > 0 {
        ((count_b.total as f64 - count_a.total as f64) / count_a.total as f64) * 100.0
    } else if count_b.total > 0 {
        100.0
    } else {
        0.0
    };

    let overall = PeriodComparison {
        area_id: area_desc.clone(),
        area_name: area_desc.clone(),
        period_a_count: count_a.total,
        period_b_count: count_b.total,
        percent_change,
    };

    // Per-category comparison
    let mut by_category = Vec::new();
    for cat_a in &count_a.by_category {
        let cat_b_count = count_b
            .by_category
            .iter()
            .find(|c| c.category == cat_a.category)
            .map_or(0, |c| c.count);

        #[allow(clippy::cast_precision_loss)]
        let pct = if cat_a.count > 0 {
            ((cat_b_count as f64 - cat_a.count as f64) / cat_a.count as f64) * 100.0
        } else if cat_b_count > 0 {
            100.0
        } else {
            0.0
        };

        by_category.push(PeriodComparison {
            area_id: cat_a.category.clone(),
            area_name: cat_a.category.clone(),
            period_a_count: cat_a.count,
            period_b_count: cat_b_count,
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
        params.date_from.as_deref(),
        params.date_to.as_deref(),
        params.category.as_deref(),
        None,
        None,
        1,
    );

    let wc = where_clause(&frags);

    let sql = format!(
        "SELECT date_trunc('{trunc}', i.occurred_at)::date::text as period, COUNT(*) as cnt
         FROM crime_incidents i
         JOIN crime_categories c ON i.category_id = c.id
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
        params.date_from.as_deref(),
        params.date_to.as_deref(),
        None,
        None,
        None,
        1,
    );

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

    // By category
    let cat_sql = format!(
        "SELECT pc.name as category, COUNT(*) as cnt
         FROM crime_incidents i
         JOIN crime_categories c ON i.category_id = c.id
         LEFT JOIN crime_categories pc ON c.parent_id = pc.id
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

    // Total
    let total_sql = format!(
        "SELECT COUNT(*) as total FROM crime_incidents i
         JOIN crime_categories c ON i.category_id = c.id
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
