-- medications: prescriptions/administrations extracted from source documents.

CREATE TABLE medications (
    id INTEGER PRIMARY KEY,
    source_document_id INTEGER NOT NULL REFERENCES source_documents(id) ON DELETE CASCADE,
    coding_system TEXT NOT NULL,
    coding_code TEXT NOT NULL,
    coding_display TEXT,
    status TEXT NOT NULL,
    dose TEXT,
    route TEXT,
    frequency TEXT,
    start_date TEXT,
    end_date TEXT
);

CREATE INDEX idx_medications_source_doc ON medications(source_document_id);
CREATE INDEX idx_medications_code ON medications(coding_code);
