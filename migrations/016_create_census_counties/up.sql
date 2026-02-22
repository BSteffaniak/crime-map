CREATE TABLE census_counties (
    geoid            TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    full_name        TEXT NOT NULL,
    state_fips       TEXT NOT NULL,
    county_fips      TEXT NOT NULL,
    state_abbr       TEXT,
    boundary         GEOGRAPHY(MultiPolygon, 4326),
    land_area_sq_mi  DOUBLE PRECISION,
    population       INTEGER,
    centroid_lon     DOUBLE PRECISION,
    centroid_lat     DOUBLE PRECISION
);

CREATE INDEX idx_census_counties_boundary ON census_counties USING GIST (boundary);
CREATE INDEX idx_census_counties_state ON census_counties (state_fips);
