-- Restore the original category_id composite from migration 012.
DROP INDEX IF EXISTS idx_incidents_place_date_pcat;
CREATE INDEX IF NOT EXISTS idx_incidents_place_date_cat
  ON crime_incidents (census_place_geoid, occurred_at, category_id)
  WHERE census_place_geoid IS NOT NULL;

DROP INDEX IF EXISTS idx_incidents_parentcat_date;

ALTER TABLE crime_incidents DROP COLUMN IF EXISTS parent_category_id;
