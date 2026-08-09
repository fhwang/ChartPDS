-- observations.derivation / problems.derivation: how the coded claim was
-- derived from its source document.
--
--   'structured' — the code came from a structured field (CCDA entry,
--                  device API payload)
--   'verbatim'   — the code was extracted from prose and is literally
--                  present in the grounding quote (clinical-PDF path)
--   'inferred'   — an LLM mapped colloquial prose to a code; the code
--                  appears nowhere in the source (journal path)
--
-- Categorical on purpose — a fact about provenance, not a confidence
-- score. Orthogonal to day_confidence (device-sync settledness) and to
-- asserter (source_documents.source, one join away).
--
-- Forward-only per the migration policy; no down migration.
ALTER TABLE observations ADD COLUMN derivation TEXT NOT NULL DEFAULT 'structured';
ALTER TABLE problems ADD COLUMN derivation TEXT NOT NULL DEFAULT 'structured';

-- Backfill: problems from clinical PDFs were verbatim-extracted from prose.
-- (Fresh databases no-op here; live databases get corrected without a
-- rebuild.)
UPDATE problems SET derivation = 'verbatim'
WHERE source_document_id IN
    (SELECT id FROM source_documents WHERE kind = 'clinical-pdf');
