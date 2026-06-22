-- Add source_documents.document_date: the calendar date the document pertains
-- to (CCDA authored effectiveTime; Fitbit day; Oura sleep day). Distinct from
-- archived_at (when the bytes entered the archive). Nullable: a CCDA may omit
-- effectiveTime. Populated for all source types by ingestion/replay.
--
-- Forward-only per the migration policy; no down migration.
ALTER TABLE source_documents ADD COLUMN document_date TEXT;
