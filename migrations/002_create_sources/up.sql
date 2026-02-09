CREATE TABLE crime_sources (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    source_type     TEXT NOT NULL,
    api_url         TEXT,
    last_synced_at  TIMESTAMPTZ,
    record_count    BIGINT NOT NULL DEFAULT 0,
    coverage_area   TEXT NOT NULL,

    CONSTRAINT uq_source_name UNIQUE (name)
);
