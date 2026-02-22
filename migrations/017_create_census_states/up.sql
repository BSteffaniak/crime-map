CREATE TABLE census_states (
    fips             TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    abbr             TEXT NOT NULL,
    boundary         GEOGRAPHY(MultiPolygon, 4326),
    land_area_sq_mi  DOUBLE PRECISION,
    population       BIGINT,
    centroid_lon     DOUBLE PRECISION,
    centroid_lat     DOUBLE PRECISION
);

CREATE INDEX idx_census_states_boundary ON census_states USING GIST (boundary);
