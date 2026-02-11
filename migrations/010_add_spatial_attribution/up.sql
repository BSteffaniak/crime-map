-- Allow incidents without precise coordinates (for counting by area).
-- We keep the NOT NULL on location for now and use a sentinel (0,0) for
-- un-geocoded rows so spatial indexes still work. The new has_coordinates
-- flag distinguishes real vs sentinel points.
ALTER TABLE crime_incidents ADD COLUMN has_coordinates BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE crime_incidents ADD COLUMN geocoded BOOLEAN NOT NULL DEFAULT FALSE;

-- Pre-computed spatial attribution: which census place / tract does this
-- incident belong to. Populated at ingest time via ST_DWithin (with a
-- small buffer to handle source-data imprecision).
ALTER TABLE crime_incidents ADD COLUMN census_place_geoid TEXT;
ALTER TABLE crime_incidents ADD COLUMN census_tract_geoid TEXT;

-- Index for fast filtering by place / tract (the primary analytics path).
CREATE INDEX idx_incidents_place_geoid ON crime_incidents (census_place_geoid) WHERE census_place_geoid IS NOT NULL;
CREATE INDEX idx_incidents_tract_geoid ON crime_incidents (census_tract_geoid) WHERE census_tract_geoid IS NOT NULL;
CREATE INDEX idx_incidents_has_coordinates ON crime_incidents (has_coordinates) WHERE has_coordinates = FALSE;
