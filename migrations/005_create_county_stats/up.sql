CREATE TABLE crime_stats_county (
    id                      SERIAL PRIMARY KEY,
    fips_code               TEXT NOT NULL,
    state                   TEXT NOT NULL,
    county_name             TEXT NOT NULL,
    year                    SMALLINT NOT NULL,
    population              INTEGER,
    violent_crime_total     INTEGER,
    property_crime_total    INTEGER,
    murder                  INTEGER,
    rape                    INTEGER,
    robbery                 INTEGER,
    aggravated_assault      INTEGER,
    burglary                INTEGER,
    larceny                 INTEGER,
    motor_vehicle_theft     INTEGER,
    arson                   INTEGER,
    centroid                GEOGRAPHY(Point, 4326),

    CONSTRAINT uq_county_year UNIQUE (fips_code, year)
);

CREATE INDEX idx_county_stats_state ON crime_stats_county (state);
CREATE INDEX idx_county_stats_year ON crime_stats_county (year);
CREATE INDEX idx_county_stats_centroid ON crime_stats_county USING GIST (centroid);
