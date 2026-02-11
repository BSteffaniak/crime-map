DROP INDEX IF EXISTS idx_places_name_trgm;
DROP INDEX IF EXISTS idx_incidents_state;
DROP INDEX IF EXISTS idx_incidents_city_trgm;

DROP EXTENSION IF EXISTS pg_trgm;
