CREATE TABLE census_places (
    geoid            TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    full_name        TEXT NOT NULL,
    state_fips       TEXT NOT NULL,
    state_abbr       TEXT,
    place_type       TEXT NOT NULL,
    boundary         GEOGRAPHY(MultiPolygon, 4326),
    land_area_sq_mi  DOUBLE PRECISION,
    population       INTEGER,
    centroid_lon     DOUBLE PRECISION,
    centroid_lat     DOUBLE PRECISION
);

CREATE INDEX idx_census_places_boundary ON census_places USING GIST (boundary);
CREATE INDEX idx_census_places_state ON census_places (state_fips);
CREATE INDEX idx_census_places_name ON census_places (name, state_abbr);
