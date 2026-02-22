-- Restore NOT NULL constraint on occurred_at.
-- Records with NULL occurred_at will need to be deleted or backfilled first.
UPDATE crime_incidents SET occurred_at = '1970-01-01T00:00:00Z' WHERE occurred_at IS NULL;
ALTER TABLE crime_incidents ALTER COLUMN occurred_at SET NOT NULL;
