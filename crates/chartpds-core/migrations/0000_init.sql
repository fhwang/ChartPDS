-- ChartPDS schema, migration 0000.
--
-- Intentionally empty. Concrete tables land in subsequent migrations as
-- the consuming modules (ingestion, sources, sync) materialize. Keeping
-- 0000 as a placeholder establishes the migration sequence and lets
-- `sqlx migrate run` succeed against a fresh database.

-- Force at least one statement so sqlite parses the file successfully:
PRAGMA application_id = 0xC4A2D505; -- 'CHARTPDS' loosely
