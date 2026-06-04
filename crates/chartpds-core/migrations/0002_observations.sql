-- observations: structured measurements extracted from source documents.
--
-- Each row carries one observation: a coded measurement (e.g. body weight,
-- sleep stage, MVPA minutes) with a time range and either a numeric or
-- string value. observations belong to a source_document; deleting the
-- parent cascades.

CREATE TABLE observations (
    id INTEGER PRIMARY KEY,
    source_document_id INTEGER NOT NULL REFERENCES source_documents(id) ON DELETE CASCADE,
    coding_system TEXT NOT NULL,
    coding_code TEXT NOT NULL,
    coding_display TEXT,
    effective_start TEXT NOT NULL,
    effective_end TEXT,
    value_quantity REAL,
    value_string TEXT,
    value_unit TEXT
);

CREATE INDEX idx_observations_code_time ON observations(coding_code, effective_start);
CREATE INDEX idx_observations_time ON observations(effective_start);
CREATE INDEX idx_observations_source_doc ON observations(source_document_id);
