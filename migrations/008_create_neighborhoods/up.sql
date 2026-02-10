CREATE TABLE neighborhoods (
    id              SERIAL PRIMARY KEY,
    source_id       TEXT NOT NULL,
    city            TEXT NOT NULL,
    state           TEXT NOT NULL,
    name            TEXT NOT NULL,
    boundary        GEOGRAPHY(MultiPolygon, 4326),
    UNIQUE (source_id, name)
);

-- Spatial index for point-in-polygon lookups
CREATE INDEX idx_neighborhoods_boundary ON neighborhoods USING GIST (boundary);

-- Index for city-level filtering
CREATE INDEX idx_neighborhoods_city ON neighborhoods (city, state);

-- Crosswalk: maps each census tract centroid to its containing neighborhood
CREATE TABLE tract_neighborhoods (
    geoid           TEXT NOT NULL REFERENCES census_tracts(geoid),
    neighborhood_id INTEGER NOT NULL REFERENCES neighborhoods(id),
    PRIMARY KEY (geoid, neighborhood_id)
);
