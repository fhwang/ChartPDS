-- Rename source_documents.ingested_at -> archived_at.
--
-- The column records when a blob's bytes first entered the archive (archival
-- truth), not when the index projection was last written. The old name was
-- ambiguous and the rebuild path rewrote it to "now" on every rebuild. With
-- sidecar manifests carrying the immutable archive-entry time, the projection
-- is populated from the manifest and the column name now matches its meaning.
--
-- Forward-only per the migration policy; no down migration.
ALTER TABLE source_documents RENAME COLUMN ingested_at TO archived_at;
