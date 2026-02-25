#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions)]

//! In-memory spatial index for boundary attribution.
//!
//! Loads census tract and place polygons from `DuckDB` at startup, builds
//! R-tree spatial indexes, and provides fast point-in-polygon lookups.
//! Used by both the ingestion enrichment step and generation pipeline.

use std::collections::BTreeMap;

use geo::{Contains, MultiPolygon};
use geojson::GeoJson;
use rstar::{AABB, RTree, RTreeObject};

/// A boundary polygon stored in the R-tree with its metadata.
struct BoundaryEntry {
    geoid: String,
    area_sq_mi: f64,
    envelope: AABB<[f64; 2]>,
    polygon: MultiPolygon<f64>,
}

impl RTreeObject for BoundaryEntry {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

/// Pre-built spatial indexes for census tracts and places.
///
/// Constructed once and shared across all consumers. Provides fast
/// point-in-polygon lookups for boundary attribution.
pub struct SpatialIndex {
    tracts: RTree<BoundaryEntry>,
    places: RTree<BoundaryEntry>,
    /// tract GEOID -> neighborhood ID (e.g. "nbhd-42")
    neighborhood_crosswalk: BTreeMap<String, String>,
}

impl SpatialIndex {
    /// Loads polygons from the boundaries `DuckDB` and builds R-tree indexes.
    ///
    /// # Errors
    ///
    /// Returns an error if the database queries or `GeoJSON` parsing fail.
    pub fn load(conn: &duckdb::Connection) -> Result<Self, Box<dyn std::error::Error>> {
        let tracts = Self::load_boundaries(
            conn,
            "SELECT geoid, land_area_sq_mi, boundary_geojson as geojson \
             FROM census_tracts WHERE boundary_geojson IS NOT NULL",
        )?;
        log::info!("Loaded {} census tracts into spatial index", tracts.size());

        let places = Self::load_boundaries(
            conn,
            "SELECT geoid, land_area_sq_mi, boundary_geojson as geojson \
             FROM census_places WHERE boundary_geojson IS NOT NULL",
        )?;
        log::info!("Loaded {} census places into spatial index", places.size());

        let neighborhood_crosswalk = Self::load_neighborhood_crosswalk(conn)?;
        log::info!(
            "Loaded {} tract->neighborhood mappings",
            neighborhood_crosswalk.len()
        );

        Ok(Self {
            tracts,
            places,
            neighborhood_crosswalk,
        })
    }

    fn load_boundaries(
        conn: &duckdb::Connection,
        query: &str,
    ) -> Result<RTree<BoundaryEntry>, Box<dyn std::error::Error>> {
        let mut stmt = conn.prepare(query)?;
        let mut rows = stmt.query([])?;
        let mut entries = Vec::new();

        while let Some(row) = rows.next()? {
            let geoid: String = row.get(0)?;
            let area_sq_mi: f64 = row.get::<_, Option<f64>>(1)?.unwrap_or(f64::MAX);
            let geojson_str: String = row.get(2)?;

            if geoid.is_empty() || geojson_str.is_empty() {
                continue;
            }

            let Some(multi_polygon) = parse_geojson_to_multipolygon(&geojson_str) else {
                log::warn!("Failed to parse GeoJSON for boundary {geoid}");
                continue;
            };

            let envelope = compute_envelope(&multi_polygon);

            entries.push(BoundaryEntry {
                geoid,
                area_sq_mi,
                envelope,
                polygon: multi_polygon,
            });
        }

        Ok(RTree::bulk_load(entries))
    }

    fn load_neighborhood_crosswalk(
        conn: &duckdb::Connection,
    ) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
        let mut stmt = conn.prepare("SELECT geoid, neighborhood_id FROM tract_neighborhoods")?;
        let mut rows = stmt.query([])?;

        let mut map = BTreeMap::new();
        while let Some(row) = rows.next()? {
            let geoid: String = row.get(0)?;
            let nbhd_id: i32 = row.get(1)?;
            if !geoid.is_empty() && nbhd_id > 0 {
                map.insert(geoid, format!("nbhd-{nbhd_id}"));
            }
        }

        Ok(map)
    }

    /// Look up the census tract GEOID for a point.
    ///
    /// Tracts tile the US without overlap, so first match wins.
    #[must_use]
    pub fn lookup_tract(&self, lng: f64, lat: f64) -> Option<&str> {
        let point = geo::Point::new(lng, lat);
        let query_env = AABB::from_point([lng, lat]);

        for entry in self.tracts.locate_in_envelope_intersecting(&query_env) {
            if entry.polygon.contains(&point) {
                return Some(&entry.geoid);
            }
        }
        None
    }

    /// Look up the census place GEOID for a point.
    ///
    /// Places can overlap; the smallest area wins (matching the previous
    /// `PostGIS` `ORDER BY ST_Area(boundary)` behavior).
    #[must_use]
    pub fn lookup_place(&self, lng: f64, lat: f64) -> Option<&str> {
        let point = geo::Point::new(lng, lat);
        let query_env = AABB::from_point([lng, lat]);

        let mut best: Option<&BoundaryEntry> = None;

        for entry in self.places.locate_in_envelope_intersecting(&query_env) {
            if entry.polygon.contains(&point) {
                match best {
                    None => best = Some(entry),
                    Some(current) if entry.area_sq_mi < current.area_sq_mi => {
                        best = Some(entry);
                    }
                    _ => {}
                }
            }
        }

        best.map(|e| e.geoid.as_str())
    }

    /// Derive state FIPS from a tract GEOID (first 2 characters).
    #[must_use]
    pub fn derive_state_fips(tract_geoid: &str) -> Option<&str> {
        if tract_geoid.len() >= 2 {
            Some(&tract_geoid[..2])
        } else {
            None
        }
    }

    /// Derive county GEOID from a tract GEOID (first 5 characters).
    #[must_use]
    pub fn derive_county_geoid(tract_geoid: &str) -> Option<&str> {
        if tract_geoid.len() >= 5 {
            Some(&tract_geoid[..5])
        } else {
            None
        }
    }

    /// Look up neighborhood ID from a tract GEOID via the crosswalk.
    #[must_use]
    pub fn lookup_neighborhood(&self, tract_geoid: &str) -> Option<&str> {
        self.neighborhood_crosswalk
            .get(tract_geoid)
            .map(String::as_str)
    }
}

/// Parse a `GeoJSON` string into a [`MultiPolygon`].
/// Handles both `Polygon` and `MultiPolygon` geometry types.
fn parse_geojson_to_multipolygon(geojson_str: &str) -> Option<MultiPolygon<f64>> {
    let geojson: GeoJson = geojson_str.parse().ok()?;
    if let GeoJson::Geometry(geom) = geojson {
        let geo_geom: geo::Geometry<f64> = geom.try_into().ok()?;
        match geo_geom {
            geo::Geometry::MultiPolygon(mp) => Some(mp),
            geo::Geometry::Polygon(p) => Some(MultiPolygon(vec![p])),
            _ => None,
        }
    } else {
        None
    }
}

/// Compute the bounding box envelope for a [`MultiPolygon`].
fn compute_envelope(mp: &MultiPolygon<f64>) -> AABB<[f64; 2]> {
    use geo::BoundingRect;

    mp.bounding_rect().map_or_else(
        || AABB::from_point([0.0, 0.0]),
        |rect| AABB::from_corners([rect.min().x, rect.min().y], [rect.max().x, rect.max().y]),
    )
}
