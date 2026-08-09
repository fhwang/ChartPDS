# Journal `.md` ingestion — design

**Date:** 2026-08-08
**Issue:** [#35 — Handle simple .md journal files](https://github.com/fhwang/ChartPDS/issues/35)
**Status:** Approved design, pre-implementation

## Problem

ChartPDS ingests structured clinical data (CCDA), clinical narrative PDFs,
and device data (Fitbit, Oura). The author also keeps free-text journal
entries — colloquial, non-expert observations like "my left shoulder has
been aching all week." These should be queryable alongside everything
else, at two levels:

1. **Retrieval** — "show me everything about my shoulder" surfaces journal
   entries next to clinical notes.
2. **Analysis** — journal symptoms become `observations` rows so
   `observation_table`, `observation_relationship`, `episodes`, and
   `observation_stats` can include them (e.g. "does shoulder pain
   correlate with sleep quality?").

Hard constraint: **the author will never annotate.** Entries are free
colloquial prose, always. No frontmatter, no severity conventions, no
ontology tags. The system fills the gaps.

## Key decisions and rationale

### Coding vocabulary: inferred ICD-10-CM

Journal complaints are mapped by the LLM into ICD-10-CM — the vocabulary
the extractor already speaks. ICD-10-CM's symptom/complaint codes
(R-codes, pain codes like M25.512 *Pain in left shoulder*) code
complaints, not diagnoses, so the vocabulary fits self-reported data
without pretending to be diagnostic.

Rejected alternative: minting a ChartPDS "self-reported symptom" system
(the AASM sleep-stage precedent). Minting works for a tiny fixed set
described statically in `clinical/catalog.rs`; symptoms would mean
minting an open-ended homegrown ontology, and minted codes would never
join with the clinician's problem list.

The extraction prompt biases toward **coarse, common codes**. Analytical
queries key on `(coding_system, coding_code)`, so the same complaint must
map to the same code across months; coarse-but-stable beats
precise-but-jittery.

### Extraction vs. inference — the `derivation` axis

Until now every coding was *extracted*: the code literally appears in the
source (a CCDA field, or verbatim in clinical-PDF prose, enforced by the
quote-contains-code verification rule). Journal codings are *inferred*:
"left shoulder has been aching" never contains "M25.512". This is the
first time LLM-inferred codes enter the index, and it is recorded
explicitly rather than smoothed over:

- New column `derivation TEXT NOT NULL` on `observations` and `problems`:
  - `structured` — code came from a structured field (CCDA).
  - `verbatim` — code extracted from prose and present in the grounding
    quote (clinical PDF path).
  - `inferred` — LLM mapped colloquial prose to a code; the code appears
    nowhere in the source (journal path).
- Migration backfills existing rows from their parent document kind.
- This is a **categorical** marker, not a numeric confidence score.
  Numeric LLM confidence is uncalibrated theater; the system records the
  fact of how a claim arose, and the consuming agent applies judgment.
- `derivation` is orthogonal to `day_confidence` (device-sync
  settledness) and to asserter (who said it — already available via join
  to `source_documents.source`). Keep the axes separate.

### Multi-resolution transparency

A consuming agent can drill from signal to source at three tiers, all of
which already exist or fall out of this design:

1. **Coarse, queryable** — `observations` rows marked `inferred`.
2. **The receipt** — the frozen extraction artifact stores the verbatim
   quote per coding, surfaced by `narrative_get`.
3. **The text** — `narrative_texts` + FTS; the agent reads the entry and
   applies its own judgment.

Every observation row carries `source_document_id`, so each tier is one
join away.

### Hallucinated-code validation

LLMs emit plausible-looking ICD-10-CM codes that do not exist. Defense:
vendor the CMS/NCHS ICD-10-CM order file (public domain; ~74k codes;
strip to the code column) into `chartpds-core`, load lazily behind a
`OnceLock`, expose `is_valid_icd10cm(code) -> bool`. Verification rejects
invalid codes with a recorded reason, and the existing bounded retry loop
feeds the rejection back to the model ("M25.5121 is not a valid ICD-10-CM
code; re-map this complaint") — converting hallucination into
self-correction. The check also applies to the clinical-PDF path as free
hardening. Document the file's vintage; annual staleness is acceptable
for validation purposes.

### Valid-but-wrong codes: detection is auditability, not verification

A valid-but-wrong mapping cannot be caught mechanically at write time.
The design guarantees permanent auditability instead: every (quote, code)
pair is frozen in the artifact, so any reader can spot a mismatch.
Occasions to look:

1. **Ingest-time echo** (v1, free): the ingest outcome already returns
   coding + quote pairs; the tool description encourages the driving
   agent to narrate them ("filed: Pain in left shoulder (M25.512) from
   'shoulder's been aching all week'"), putting the mapping in front of
   the one person who knows what the entry meant, at the moment their
   attention is on it.
2. **Incidental audit during analysis** (v1, free): drill-down means
   agents read receipts as a side effect of caring about a signal.
3. **Deliberate audit sweep / amendment records / re-extraction command**
   — deferred (see below).

### Zero-coding entries are accepted, not rejected

"Felt great, long run, slept well" has nothing to code and is still
valuable: it surfaces in FTS retrieval and as context for adjacent days.
Rejecting it would punish good days. To keep such entries from being
invisible, the narrative catalog listing gains a codings count, making
"journal entries that produced no structured data" a one-glance query.

## Design

### Input & ingestion

- New document kind `"journal"`, source `"journal"`, ingested through the
  existing `record_ingest` MCP tool.
- One file = one entry (v1). Entry date resolved deterministically first:
  `YYYY-MM-DD` in the filename, else a parseable date in the first
  markdown header; fallback to the existing LLM document-date extraction
  with quote verification. A year-less header (`# Jul 26`) takes its year
  from the filename when one is present; if no year exists anywhere, no
  path can verifiably date the entry, and the ingest is rejected with a
  clear "add a year to the filename or header" error. A file with
  multiple date headers is rejected with a clear "one entry per file"
  error.
- Pipeline shape mirrors `ingest_narrative_pdf`: text → LLM extraction →
  mechanical verification → archive blob + frozen derived artifact →
  `source_documents` upsert → `narrative_texts` (FTS via triggers) →
  index rows. No text-layer step; the bytes are already text.
- Extraction failure (missing key, sustained outage) fails the whole
  ingest before anything persists — same invariant as the PDF path.
- Zero verified codings is not a failure.

### Extraction

- New journal prompt variant: infer coarse ICD-10-CM symptom/complaint
  codes from colloquial prose; every coding quote-grounded.
- **Severity**: extracted only when the author stated a number ("pain was
  maybe a 6 today" → 6). The number must appear in the grounding quote,
  keeping severity mechanically verifiable. No number → no value;
  presence is the signal.

### Verification (journal codings)

- Quote-grounds-in-text: mandatory, unchanged (whitespace-normalized
  substring).
- Code-appears-in-quote: **waived** for the journal path, replaced by the
  `is_valid_icd10cm` table check.
- Severity number must appear in the quote.
- Every drop recorded in `rejected` with a human-readable reason,
  surfaced in the ingest outcome.

### Index

- Migration: add `derivation TEXT NOT NULL` to `observations` and
  `problems`; backfill from parent document kind. Forward-only, then
  `just prepare-sql`.
- Journal codings become `observations` rows: `effective_start` = entry
  date, `value_quantity` = stated severity or NULL,
  `derivation = 'inferred'`.
- Journal codings do **not** enter `problems`. The problem list stays
  clinician-asserted; journal complaints reach retrieval via
  `observation_history` and FTS.

### Rebuild

- New dispatch arm for the journal kind: replay text + `apply_extraction`
  from the frozen artifact. Network-free, model-free, unchanged
  invariant.

### Query & MCP tool surface

- `observation_history` / `observation_latest` outputs expose
  `derivation`.
- Narrative catalog listing (`narrative_search` catalog mode) gains a
  codings count per document.
- `record_ingest` accepts the new kind; its description documents the
  journal path and encourages echoing coding/quote pairs to the author.
- README tool-surface and configuration lists updated in the same diff as
  any surface change (per CLAUDE.md).

### Testing

- Unit: date resolution (filename / header / LLM fallback,
  multi-header rejection), journal verification rules (waived
  code-in-quote, table check, severity-in-quote), `is_valid_icd10cm`.
- Integration: fake `LlmExtractor` (existing trait seam) driving the full
  ingest → index → query path; rebuild replay parity.

## Deferred, deliberately

- Valid-but-wrong audits: batch re-judging of (quote, code) pairs.
- Amendment/correction records (corrections as archived patient-authored
  documents applied during rebuild).
- Explicit re-extraction command (would loosen "LLM runs exactly once"
  into "once per extraction generation, human-initiated, never during
  rebuild").
- Multi-entry files / rolling journal files.
- Severity inferred from words ("excruciating" → number).
- Bucket-level derivation rollups in `observation_table` (journal and
  device codings barely share key space today; blending is theoretical).
