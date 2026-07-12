-- narrative_texts: extracted plain text of narrative documents (clinical
-- PDFs). One row per narrative source_document. Deleting the parent
-- source_documents row cascades here; the FTS triggers below propagate
-- that delete into the full-text index.

CREATE TABLE narrative_texts (
    source_document_id INTEGER PRIMARY KEY
        REFERENCES source_documents(id) ON DELETE CASCADE,
    title TEXT,
    text TEXT NOT NULL
);

-- Full-text index over narrative_texts.text (external-content FTS5, BM25
-- ranking). rowid == source_document_id.
CREATE VIRTUAL TABLE narrative_texts_fts USING fts5(
    text,
    content='narrative_texts',
    content_rowid='source_document_id'
);

CREATE TRIGGER narrative_texts_ai AFTER INSERT ON narrative_texts BEGIN
    INSERT INTO narrative_texts_fts(rowid, text)
    VALUES (new.source_document_id, new.text);
END;

CREATE TRIGGER narrative_texts_ad AFTER DELETE ON narrative_texts BEGIN
    INSERT INTO narrative_texts_fts(narrative_texts_fts, rowid, text)
    VALUES ('delete', old.source_document_id, old.text);
END;

CREATE TRIGGER narrative_texts_au AFTER UPDATE ON narrative_texts BEGIN
    INSERT INTO narrative_texts_fts(narrative_texts_fts, rowid, text)
    VALUES ('delete', old.source_document_id, old.text);
    INSERT INTO narrative_texts_fts(rowid, text)
    VALUES (new.source_document_id, new.text);
END;

-- problems.section_label: the verbatim section heading a narrative-extracted
-- coding appeared under (e.g. 'Pre-Op Diagnosis/Indications'). Free-form
-- provenance for an LLM reader, not machine-aggregatable. NULL for CCDA rows.
ALTER TABLE problems ADD COLUMN section_label TEXT;
