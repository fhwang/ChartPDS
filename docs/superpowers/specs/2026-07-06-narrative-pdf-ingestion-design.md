# Narrative PDF ingestion and search

**Date:** 2026-07-06
**Status:** Approved design

## Motivation

Clinical documents often arrive as PDFs printed from a provider portal — a
pathology report, an imaging report, a visit summary. These are *narrative*
documents: prose written for humans, unlike the machine data feeds (CCDA XML,
Fitbit/Oura JSON) ChartPDS ingests today. We want them:

1. **Archived** — raw bytes in the content-addressed archive, source of truth,
   surviving rebuilds like every other blob.
2. **Searchable free-form** — an LLM client can find and read them by keyword
   ("any pathology reports?").
3. **Indexed structurally where provable** — portal printouts frequently quote
   ICD-10 codes inline (e.g. `Pre-Op Diagnosis/Indications: Abdominal pain,
   unspecified - R10.9`). Those codes should land in `problems` alongside
   CCDA-extracted diagnoses.

"Narrative" is the chosen term for this document class. "Document" is already
taken: `source_documents` covers every archived blob, including data feeds.
What distinguishes this class is prose meant for human reading. (FHIR US Core
files pathology reports under "Clinical Notes"; "narrative" avoids both the
note/report mismatch and the document collision.)

## Design principles

- **ChartPDS owns interpretation.** Indexing from archived bytes to
  structured/searchable data is a ChartPDS pipeline, not delegated to
  whatever MCP client is connected.
- **LLM use is allowed inside ChartPDS, but its output must be mechanically
  verified and frozen.** The LLM runs once, at ingest. Every claim it makes is
  verified against the document text before acceptance. The verified result is
  frozen as its own blob in the **derived store** (`$DIR/derived/`, same
  content-addressed shape as the archive) — the archive holds only bytes that
  arrived from outside; machine-generated derivations get their own tier with
  their own lifecycle. `rebuild_index` replays the frozen artifact and never
  calls a model — rebuild stays hermetic and deterministic.
- **No embeddings (for now).** At personal-record scale, SQLite FTS5 keyword
  search plus an LLM that iterates queries covers retrieval. Embeddings would
  add an API dependency to the *query* path, weld the index to a specific
  embedding-model version (breaking rebuild hermeticity), and blur the exact
  identifiers (codes, dates, drug names) clinical search leans on. Revisit if
  the corpus reaches thousands of narratives or a non-LLM search UI appears;
  a vector table can be added alongside FTS without unwinding anything.

## Data flow

```
ingest_record(file_path, kind="clinical-pdf", source, original_filename?)
  → read PDF bytes → archive blob + manifest            (raw bytes)
  → deterministic text extraction (Rust)                (no LLM)
  → LLM extraction: document_date, title, codings       (Claude API, once)
  → mechanical verification of every LLM claim
  → verified extraction frozen in the derived store     (references PDF hash)
  → index: source_documents row + narrative_texts + FTS + problems rows
```

## Storage layers

Two blobs per narrative, in two stores: the PDF blob lives in the archive
(`$DIR/archive/`, source bytes from outside), the extraction artifact in the
derived store (`$DIR/derived/`, machine-generated derivations). Both stores
are content-addressed with sidecar manifests:

| | PDF blob (archive) | Extraction artifact (derived) |
|---|---|---|
| `type` (kind) | `clinical-pdf` | `narrative-extraction` |
| `datacontenttype` | `application/pdf` | `application/json` |
| `subject` | document date (if extracted) | referenced PDF content hash |
| `source` | caller-supplied (e.g. `manual-upload`) | `chartpds` |

The PDF manifest is written after extraction completes so `subject` can carry
the extracted document date; if extraction fails, `subject` is omitted.

## Extraction artifact format

```json
{
  "document": "<sha256 of the PDF blob>",
  "document_date": "2026-04-21",
  "document_date_quote": "Order Date: 04/21/2026",
  "title": "GI Pathology Report — colon biopsy",
  "codings": [
    {
      "system": "http://hl7.org/fhir/sid/icd-10-cm",
      "code": "R10.9",
      "display": "Abdominal pain, unspecified",
      "quote": "Pre-Op Diagnosis/Indications: Abdominal pain, unspecified - R10.9",
      "section_label": "Pre-Op Diagnosis/Indications"
    }
  ],
  "extractor": { "model": "<model id>", "prompt_version": 1 },
  "extracted_at": "<RFC 3339>"
}
```

### Verification rules (pure Rust, whitespace-normalized on both sides)

- Every `quote` must be a substring of the deterministically extracted text.
- Each coding's `code` must appear within its `quote`.
- `document_date_quote` must contain a date string parseable to the claimed
  `document_date`.
- Entries failing verification are dropped and reported in the tool result;
  they never reach the artifact. The archive only contains claims provable
  against the document text.

Prose-only findings (a diagnosis stated without a code) are **out of scope for
v1**. The artifact format leaves room for a future `method` field
(`llm-verified` today, `llm-inferred` later) when we decide to code them.

## Index schema (one forward migration)

- `narrative_texts` — `source_document_id` (FK, cascade delete) + extracted
  text. One row per narrative.
- `narrative_texts_fts` — FTS5 external-content virtual table over
  `narrative_texts`, BM25 ranking. Kept in sync by AFTER INSERT/UPDATE/DELETE
  triggers on `narrative_texts`, so cascade deletes (e.g. rebuild clearing
  `source_documents`) propagate to the FTS index without write-path code.
- `problems.section_label` — new nullable TEXT column. The verbatim section
  heading from the document (free-form, e.g. `Pre-Op Diagnosis/Indications`).
  CCDA-extracted rows leave it null. It is provenance for an LLM reader, not
  a machine-aggregatable field; a normalized enum column can be added later if
  a query needs it.

Narrative codings land in `problems` with `coding_system` = ICD-10-CM,
`status` = `"unknown"` (narratives carry no problem status; `status` is
already documented as unreliable), `onset_date` = the document date, and
`section_label` set.

**Implementation step zero:** verify FTS5 is enabled in sqlx's bundled SQLite
and that `sqlx::query!` + `just prepare-sql` handle virtual-table SQL. If not,
reconsider before building further.

## MCP tool surface (13 → 15 tools)

- **`ingest_record` (extended, no new write tool)** — accepts
  `kind = "clinical-pdf"` in addition to `"ccda"`, routing on kind. Returns
  what was extracted, what was verified, and what was dropped.
- **`search_narratives { query?, limit? }`** — FTS BM25 match returning
  `{ document_id, title, kind, document_date, snippet }`. Query omitted =
  catalog listing, newest first (at personal scale, often all an LLM needs).
- **`get_narrative { document_id }`** — full extracted text + metadata + the
  codings extracted from it.

## Rebuild

Two-phase within `rebuild_index`:

1. **Documents pass** — as today, plus `clinical-pdf`: re-extract text
   deterministically, rebuild the `source_documents` row, `narrative_texts`,
   and FTS.
2. **Artifacts pass** — apply each `narrative-extraction` blob (from the
   derived store; artifacts found in the archive — a legacy layout — replay
   identically) to its referenced document: document date, title, `problems`
   rows. Runs after the documents pass so foreign keys always resolve.

`RebuildResult` gains `narratives_ingested` and `extractions_applied`
counts. No LLM call ever happens during rebuild. (A failed extraction
leaves nothing behind to rebuild — see LLM failure handling below.)

## LLM client and configuration

New `extraction/` module in `chartpds-core`: PDF text extraction, Claude API
client (reqwest, already a workspace dependency), and verification.

- PDF text via a pure-Rust crate (candidate: `pdf-extract`); validate against
  a real portal printout before building on it.
- `ANTHROPIC_API_KEY` read from the environment. Model pinned in code;
  model id + prompt version recorded in every artifact.
- **No key configured:** ingest degrades gracefully to text-only (archive +
  FTS work, no codings), reported in-band as
  `extraction_status: "skipped_no_extractor"`. Total verification failure
  also still applies (an empty extraction with everything in `rejected`).
- **LLM failure:** transient failures (connection errors, HTTP 429/5xx) are
  retried in-band — 3 attempts with short linear backoff, sized for the
  interactive ingest-then-query session. A sustained outage then fails the
  whole ingest with nothing persisted (no archive blob, no index rows), so
  re-running `ingest_record` once the API recovers starts clean. Multi-hour
  outages are deliberately unaddressed for now: no durable retry queue; the
  caller re-runs the ingest.

## Error handling

- **Scanned PDF (no text layer):** fail with a clear "no text layer; OCR
  unsupported" error. OCR is out of scope.
- **Partial verification failure:** keep verified entries, drop and report the
  rest.
- **Duplicate ingest:** content-addressing dedupes; re-ingest upserts.

## Testing

- Verification logic is pure — direct unit tests, including whitespace
  normalization edge cases.
- The Claude client sits behind a small trait; extraction tests use a canned
  response.
- MCP tool tests construct the server directly per house style.
- **Fixtures are synthetic** — a generated pathology-lookalike PDF. Real
  personal documents never enter the repository (public repo).

## Deferred (explicitly out of scope for v1)

- Coding prose-only findings (`llm-inferred` tier).
- Normalized section-semantics enum alongside `section_label`.
- OCR for scanned PDFs.
- Embeddings / vector search.
- Non-PDF narrative formats (e.g. pasted text, HTML portal pages) — the
  pipeline shape accommodates them as new kinds later.
