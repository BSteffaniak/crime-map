-- Partial indexes that accelerate the spatial attribution UPDATE queries.
-- The existing indexes cover IS NOT NULL (for analytics), but attribution
-- needs to find rows WHERE geoid IS NULL AND has_coordinates = TRUE.

CREATE INDEX IF NOT EXISTS idx_incidents_unattr_place
    ON crime_incidents (id)
    WHERE census_place_geoid IS NULL AND has_coordinates = TRUE;

CREATE INDEX IF NOT EXISTS idx_incidents_unattr_tract
    ON crime_incidents (id)
    WHERE census_tract_geoid IS NULL AND has_coordinates = TRUE;
