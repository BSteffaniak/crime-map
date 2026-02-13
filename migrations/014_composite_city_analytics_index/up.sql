-- Composite index for the dominant analytics query pattern.
--
-- Nearly every AI agent tool filters by (state, city) + optional
-- parent_category_id + date range.  Without this index the planner
-- must BitmapAnd two separate indexes (idx_incidents_state_city and
-- idx_incidents_parentcat_date), which is extremely expensive for
-- large cities like Chicago (8.4M rows = 55 % of the table).
--
-- With all four columns in one B-tree the planner can satisfy the
-- entire WHERE clause in a single range scan — no bitmap, no heap
-- recheck, orders of magnitude faster.
CREATE INDEX idx_incidents_city_cat_date
  ON crime_incidents (state, city, parent_category_id, occurred_at);
