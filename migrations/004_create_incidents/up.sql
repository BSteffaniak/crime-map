CREATE TABLE crime_incidents (
    id                  BIGSERIAL PRIMARY KEY,
    source_id           INTEGER NOT NULL REFERENCES crime_sources(id),
    source_incident_id  TEXT NOT NULL,
    category_id         INTEGER NOT NULL REFERENCES crime_categories(id),
    location            GEOGRAPHY(Point, 4326) NOT NULL,
    occurred_at         TIMESTAMPTZ NOT NULL,
    reported_at         TIMESTAMPTZ,
    description         TEXT,
    block_address       TEXT,
    city                TEXT,
    state               TEXT,
    arrest_made         BOOLEAN,
    domestic            BOOLEAN,
    location_type       TEXT,

    CONSTRAINT uq_source_incident UNIQUE (source_id, source_incident_id)
);

-- Spatial index for bounding box and proximity queries
CREATE INDEX idx_incidents_location ON crime_incidents USING GIST (location);

-- Temporal index for date range filtering
CREATE INDEX idx_incidents_occurred_at ON crime_incidents (occurred_at);

-- Category index for type filtering
CREATE INDEX idx_incidents_category ON crime_incidents (category_id);

-- Source index for per-source queries
CREATE INDEX idx_incidents_source ON crime_incidents (source_id);
