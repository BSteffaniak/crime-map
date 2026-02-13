-- Denormalize parent_category_id onto crime_incidents.
--
-- Every analytics query filters or groups by parent (top-level) category,
-- requiring a JOIN to crime_categories + self-join to resolve parent_id.
-- For cities like Chicago with 8M+ rows, this join is the primary
-- bottleneck. Storing parent_category_id directly on the incident row
-- eliminates the join for category filtering entirely.

ALTER TABLE crime_incidents
  ADD COLUMN parent_category_id INTEGER REFERENCES crime_categories(id);

-- Backfill from the existing category hierarchy.
-- COALESCE handles top-level categories where parent_id IS NULL —
-- they are their own parent.
UPDATE crime_incidents i
SET parent_category_id = (
  SELECT COALESCE(c.parent_id, c.id)
  FROM crime_categories c
  WHERE c.id = i.category_id
);

ALTER TABLE crime_incidents
  ALTER COLUMN parent_category_id SET NOT NULL;

-- General-purpose index on parent category + date.
-- The planner can BitmapAnd this with the GIN trigram city index
-- for queries like "violent crime in Chicago, all time".
CREATE INDEX idx_incidents_parentcat_date
  ON crime_incidents (parent_category_id, occurred_at);

-- Replace the category_id composite from migration 012 with
-- parent_category_id now that the column exists. The old index
-- used category_id (subcategory) which doesn't match how the
-- analytics tools actually filter (by parent category).
DROP INDEX IF EXISTS idx_incidents_place_date_cat;
CREATE INDEX idx_incidents_place_date_pcat
  ON crime_incidents (census_place_geoid, occurred_at, parent_category_id)
  WHERE census_place_geoid IS NOT NULL;
