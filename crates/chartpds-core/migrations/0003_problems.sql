-- problems: diagnoses extracted from source documents.

CREATE TABLE problems (
    id INTEGER PRIMARY KEY,
    source_document_id INTEGER NOT NULL REFERENCES source_documents(id) ON DELETE CASCADE,
    coding_system TEXT NOT NULL,
    coding_code TEXT NOT NULL,
    coding_display TEXT,
    status TEXT NOT NULL,
    onset_date TEXT
);

CREATE INDEX idx_problems_source_doc ON problems(source_document_id);
CREATE INDEX idx_problems_code ON problems(coding_code);
