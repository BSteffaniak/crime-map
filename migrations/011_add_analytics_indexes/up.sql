-- Indexes to speed up analytics queries that filter by city/state.
-- The AI agent frequently queries by city+state, and without these the
-- planner falls back to sequential scans on the full incidents table.

-- Composite index on (state, city) covers the most common filter pattern.
-- Using lower(city) so we can do exact-match comparisons instead of ILIKE.
CREATE INDEX idx_incidents_state_city ON crime_incidents (state, lower(city));

-- Composite covering index for the category join + city/state filter.
-- This lets Postgres do an index-only scan for count queries.
CREATE INDEX idx_incidents_city_state_category ON crime_incidents (state, lower(city), category_id);
