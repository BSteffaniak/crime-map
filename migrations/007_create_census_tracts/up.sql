CREATE TABLE census_tracts (
    geoid               TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    state_fips          TEXT NOT NULL,
    county_fips         TEXT NOT NULL,
    state_abbr          TEXT,
    county_name         TEXT,
    boundary            GEOGRAPHY(MultiPolygon, 4326),
    land_area_sq_mi     DOUBLE PRECISION,
    population          INTEGER,
    centroid_lon        DOUBLE PRECISION,
    centroid_lat        DOUBLE PRECISION
);

-- Spatial index for point-in-polygon lookups
CREATE INDEX idx_census_tracts_boundary ON census_tracts USING GIST (boundary);

-- Index for state-level filtering
CREATE INDEX idx_census_tracts_state ON census_tracts (state_fips);

-- Index for county-level filtering
CREATE INDEX idx_census_tracts_county ON census_tracts (state_fips, county_fips);
