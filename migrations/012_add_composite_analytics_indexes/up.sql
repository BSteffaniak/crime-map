-- Clean up stale indexes from the original version of migration 011
-- (which used lower(city) functional indexes before being rewritten
-- to use pg_trgm). The migration runner skipped the rewritten 011
-- since it was already marked as applied.
DROP INDEX IF EXISTS idx_incidents_state_city;
DROP INDEX IF EXISTS idx_incidents_city_state_category;

-- Composite indexes for analytics query patterns.
--
-- The AI agent's tools filter by location (place_geoid or city/state) and
-- date range on nearly every query. Single-column indexes force the planner
-- to bitmap-AND or heap-scan after the first filter. These composites let
-- it do tight range scans across both dimensions.

-- Primary analytics path: filter by census place + date range.
-- Includes category_id so count queries with a category filter can do
-- an index-only scan without touching the heap.
CREATE INDEX IF NOT EXISTS idx_incidents_place_date_cat
  ON crime_incidents (census_place_geoid, occurred_at, category_id)
  WHERE census_place_geoid IS NOT NULL;

-- rank_areas: filter by place, join on tract, narrow by date.
CREATE INDEX IF NOT EXISTS idx_incidents_place_tract_date
  ON crime_incidents (census_place_geoid, census_tract_geoid, occurred_at)
  WHERE census_place_geoid IS NOT NULL AND census_tract_geoid IS NOT NULL;

-- rank_areas via city path: filter on tract + date.
CREATE INDEX IF NOT EXISTS idx_incidents_tract_date
  ON crime_incidents (census_tract_geoid, occurred_at)
  WHERE census_tract_geoid IS NOT NULL;

-- list_cities / search_locations: GROUP BY (state, city) without full scan.
CREATE INDEX IF NOT EXISTS idx_incidents_state_city
  ON crime_incidents (state, city);
