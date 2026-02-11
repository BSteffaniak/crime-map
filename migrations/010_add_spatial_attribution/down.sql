DROP INDEX IF EXISTS idx_incidents_has_coordinates;
DROP INDEX IF EXISTS idx_incidents_tract_geoid;
DROP INDEX IF EXISTS idx_incidents_place_geoid;
ALTER TABLE crime_incidents DROP COLUMN IF EXISTS census_tract_geoid;
ALTER TABLE crime_incidents DROP COLUMN IF EXISTS census_place_geoid;
ALTER TABLE crime_incidents DROP COLUMN IF EXISTS geocoded;
ALTER TABLE crime_incidents DROP COLUMN IF EXISTS has_coordinates;
