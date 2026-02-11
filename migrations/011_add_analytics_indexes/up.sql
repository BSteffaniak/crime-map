-- Enable trigram extension for ILIKE-backed index scans.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- GIN trigram indexes allow the planner to use index scans for ILIKE
-- patterns (both exact like 'Washington' and substring like '%capitol%').
-- This replaces sequential scans on the full incidents table for the
-- city filter used by every analytics tool.
CREATE INDEX idx_incidents_city_trgm ON crime_incidents USING gin (city gin_trgm_ops);

-- Plain btree on state for exact-match equality filters.
CREATE INDEX idx_incidents_state ON crime_incidents (state);

-- Trigram index on census place names for search_locations queries.
CREATE INDEX idx_places_name_trgm ON census_places USING gin (name gin_trgm_ops);
