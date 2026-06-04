-- source_documents: one row per ingested archive blob.
--
-- Links a content-addressed archive blob (archive_key = SHA-256 hex of the
-- bytes) to the ingestion event that produced its structured rows. Multiple
-- observations / problems / medications may reference a single source
-- document via foreign keys.
--
-- Designed for re-ingest: if the same archive_key is ingested again (e.g.
-- during a rebuild-index run), the existing row is updated in place and
-- its dependent rows are regenerated.

CREATE TABLE source_documents (
    id INTEGER PRIMARY KEY,
    archive_key TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    source TEXT NOT NULL,
    original_filename TEXT,
    ingested_at TEXT NOT NULL
);

CREATE INDEX idx_source_documents_archive_key ON source_documents(archive_key);
CREATE INDEX idx_source_documents_kind ON source_documents(kind);
CREATE INDEX idx_source_documents_source ON source_documents(source);
