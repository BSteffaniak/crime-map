-- Allow NULL occurred_at for incidents where the source record has a
-- missing or unparseable date field. Previously the ingestion pipeline
-- silently substituted Utc::now() for such records, causing historical
-- crimes to appear as if they happened at ingestion time.
ALTER TABLE crime_incidents ALTER COLUMN occurred_at DROP NOT NULL;
