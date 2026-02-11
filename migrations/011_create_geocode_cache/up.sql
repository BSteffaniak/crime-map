-- Cache geocoding results (both hits and misses) per address per provider.
-- This prevents redundant API calls across batches and across runs.
CREATE TABLE geocode_cache (
    address_key TEXT NOT NULL,
    provider    TEXT NOT NULL,
    lat         FLOAT8,
    lng         FLOAT8,
    matched_address TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (address_key, provider)
);
