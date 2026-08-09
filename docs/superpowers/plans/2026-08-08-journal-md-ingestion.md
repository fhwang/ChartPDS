# Journal `.md` Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest free-text journal `.md` entries, map complaints to inferred ICD-10-CM codes as `observations` rows (marked `derivation='inferred'`), and make entries searchable/analyzable alongside clinical and device data.

**Architecture:** Mirrors the existing narrative-PDF pipeline (`ingestion/narrative.rs`): text → one-time LLM extraction → mechanical verification → archive blob + frozen derived artifact → index rows → FTS. New pieces: a vendored ICD-10-CM validity table, a `derivation` column on `observations`/`problems`, deterministic entry-date resolution, and a journal-specific extraction prompt/verification that waives the code-in-quote rule in favor of the table check.

**Tech Stack:** Rust stable (pinned), sqlx offline mode, `time`, `reqwest`, rmcp. No new crate dependencies.

**Spec:** `docs/superpowers/specs/2026-08-08-journal-md-ingestion-design.md` — read it first.

## Global Constraints

- Work on branch `journal-md-ingestion-spec` (already exists, has the spec).
- **Never bypass a lint.** No `#[allow(...)]` without `reason = "..."`. Every `pub` item needs a doc comment (`missing_docs` is promoted to error by `just lint`).
- Migrations are **forward-only**. After any migration or `sqlx::query!` change: `just prepare-sql`, and commit the `.sqlx/` cache in the same commit.
- **Protected paths — do not touch:** `holdout/`, `holdout.lock`, `.github/allowed_signers`, `.github/workflows/holdout.yml`. Never run `just holdout-bless`.
- Run checks with `env -u RUSTUP_TOOLCHAIN` prefix (a `RUSTUP_TOOLCHAIN=stable` env var in tool shells masks the repo's toolchain pin): e.g. `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core`.
- Commit messages end with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`; author is `Francis Hwang <sera@fhwang.net>` (use `git -c user.email=sera@fhwang.net -c user.name="Francis Hwang" commit ...`).
- Public repo: no personal health data in fixtures, tests, or commits. All journal test text must be synthetic.
- Tests must never hit the network. LLM behavior is tested through canned `JournalExtractor` impls (existing pattern: `MockExtractor` in `ingestion/narrative.rs` tests).
- When you change behavior, update the module `//!` docs and item docs in the same diff. MCP tool `description` strings in `crates/chartpds-mcp/src/server.rs` are the canonical per-tool docs.

---

### Task 1: Vendored ICD-10-CM validity table

**Files:**
- Create: `crates/chartpds-core/data/icd10cm_codes_fy2026.txt` (vendored data)
- Create: `crates/chartpds-core/src/extraction/icd10.rs`
- Modify: `crates/chartpds-core/src/extraction/mod.rs` (add `mod icd10;` + re-export)
- Modify: `crates/chartpds-core/src/extraction/verify.rs` (PDF-path hardening + test)

**Interfaces:**
- Produces: `pub fn is_valid_icd10cm(code: &str) -> bool` re-exported as `chartpds_core::extraction::is_valid_icd10cm` (later tasks import it via `super::icd10::is_valid_icd10cm` inside `extraction/`).

- [ ] **Step 1: Acquire and vendor the code table**

Download the FY2026 ICD-10-CM "order file" (public domain, published by NCHS/CMS). Primary source:

```bash
cd "$SCRATCHPAD"  # any scratch dir
curl -fLO https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Publications/ICD10CM/2026/icd10cm-order-2026.zip
unzip -o icd10cm-order-2026.zip
```

If that URL 404s, find the current "ICD-10-CM Order File" link on https://www.cms.gov/medicare/coding-billing/icd-10-codes (or the NCHS ICD-10-CM page) and download the FY2026 order file from there; if only a different fiscal year is available, use it and name the vendored file accordingly (and update the `include_str!` path and doc comments to match).

The order file (`icd10cm_order_2026.txt` or similar) is whitespace-separated: order number, code (dotless), header flag, descriptions. Extract the code column, including non-billable category codes (we *want* coarse codes):

```bash
awk '{print $2}' icd10cm_order_2026.txt | LC_ALL=C sort -u > /Users/fhwang/Code/ChartPDS/crates/chartpds-core/data/icd10cm_codes_fy2026.txt
wc -l /Users/fhwang/Code/ChartPDS/crates/chartpds-core/data/icd10cm_codes_fy2026.txt
```

Sanity checks (all must hold before proceeding):
- Line count is between 70,000 and 110,000.
- `grep -c '^M25512$' ...` is 1 (Pain in left shoulder, dotless).
- `grep -c '^R109$' ...` is 1 (Abdominal pain, unspecified).
- Every line matches `^[A-Z][0-9][0-9A-Z]{0,5}$`: `grep -vcE '^[A-Z][0-9][0-9A-Z]{0,5}$' ...` prints 0.

- [ ] **Step 2: Write the failing test**

Create `crates/chartpds-core/src/extraction/icd10.rs` with only the test module first:

```rust
//! ICD-10-CM code validity: a vendored table of every valid code.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_real_codes_with_or_without_dot_any_case() {
        assert!(is_valid_icd10cm("M25.512"));
        assert!(is_valid_icd10cm("M25512"));
        assert!(is_valid_icd10cm("m25.512"));
        assert!(is_valid_icd10cm("R10.9"));
        assert!(is_valid_icd10cm(" R10.9 "));
        // Non-billable category code — deliberately valid (coarse coding).
        assert!(is_valid_icd10cm("M25"));
    }

    #[test]
    fn rejects_hallucinated_and_malformed_codes() {
        assert!(!is_valid_icd10cm("Q99.9999"));
        assert!(!is_valid_icd10cm("M25.5129"));
        assert!(!is_valid_icd10cm(""));
        assert!(!is_valid_icd10cm("NOTACODE"));
        assert!(!is_valid_icd10cm("123.45"));
    }
}
```

Add to `crates/chartpds-core/src/extraction/mod.rs`:

```rust
mod icd10;
```

and extend the existing `pub use` list with:

```rust
pub use icd10::is_valid_icd10cm;
```

- [ ] **Step 3: Run test to verify it fails**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core icd10`
Expected: COMPILE FAIL — `is_valid_icd10cm` not found.

- [ ] **Step 4: Write the implementation**

Prepend to `icd10.rs` (above the test module):

```rust
//! ICD-10-CM code validity: a vendored table of every valid code.
//!
//! Source: the NCHS/CMS ICD-10-CM FY2026 "order file" (public domain),
//! stripped to the code column — dotless, uppercase, one code per line,
//! including non-billable category codes (journal extraction deliberately
//! prefers coarse codes, and a category like `M25` is a real code).
//! Vintage: FY2026. Annual staleness is acceptable for validity checking;
//! to refresh, re-run the acquisition step in the implementation plan and
//! replace the data file.

use std::sync::OnceLock;

const RAW: &str = include_str!("../../data/icd10cm_codes_fy2026.txt");

/// The sorted code table, parsed once on first use.
fn codes() -> &'static [&'static str] {
    static CODES: OnceLock<Vec<&'static str>> = OnceLock::new();
    CODES.get_or_init(|| {
        let mut v: Vec<&'static str> = RAW.lines().filter(|l| !l.is_empty()).collect();
        v.sort_unstable();
        v
    })
}

/// True when `code` is a real ICD-10-CM code per the vendored FY2026 table.
///
/// Case-insensitive; the conventional dot after the third character is
/// optional (`"M25.512"` and `"M25512"` are the same code). Leading and
/// trailing whitespace is ignored.
#[must_use]
pub fn is_valid_icd10cm(code: &str) -> bool {
    let normalized: String = code
        .trim()
        .chars()
        .filter(|c| *c != '.')
        .map(|c| c.to_ascii_uppercase())
        .collect();
    !normalized.is_empty() && codes().binary_search(&normalized.as_str()).is_ok()
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core icd10`
Expected: PASS (both tests).

- [ ] **Step 6: Harden the clinical-PDF path (test first)**

In `crates/chartpds-core/src/extraction/verify.rs` tests, add:

```rust
#[test]
fn rejects_code_not_in_the_icd10cm_table_even_when_quoted() {
    // A well-formed but nonexistent code, "present" verbatim in the text:
    // the vocabulary table must still reject it.
    let text = "Diagnosis: Something odd - Q99.9999";
    let v = verify_extraction(
        text,
        raw(vec![coding("Q99.9999", "Something odd - Q99.9999")], None, None),
    );
    assert!(v.codings.is_empty());
    assert_eq!(v.rejected.len(), 1);
    assert!(v.rejected[0].contains("not a valid ICD-10-CM code"));
}
```

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core verify`
Expected: FAIL — the coding is currently accepted.

Then in `verify_extraction`'s coding loop, insert a new arm after the `!norm_quote.contains(&c.code)` check (order matters — quote checks first, then vocabulary):

```rust
        } else if !super::icd10::is_valid_icd10cm(&c.code) {
            rejected.push(format!(
                "coding {}: not a valid ICD-10-CM code (vendored FY2026 table)",
                c.code
            ));
```

Also update the module `//!` header of `verify.rs` to mention the vocabulary check ("...a coding's code must appear inside its quote, be a real ICD-10-CM code per the vendored table, and a claimed date...").

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core extraction`
Expected: PASS (all extraction tests — existing tests use real codes R10.9/K62.5/Z12.11 and still pass).

- [ ] **Step 7: Commit**

```bash
git add crates/chartpds-core/data/icd10cm_codes_fy2026.txt crates/chartpds-core/src/extraction/
git -c user.email=sera@fhwang.net -c user.name="Francis Hwang" commit -m "Vendor ICD-10-CM code table; reject nonexistent codes in verification

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: `derivation` column on observations and problems

**Files:**
- Create: `crates/chartpds-core/migrations/0014_derivation.sql`
- Modify: `crates/chartpds-core/src/index/observations.rs` (struct, InsertParams, SQL)
- Modify: `crates/chartpds-core/src/index/problems.rs` (struct, InsertParams, SQL)
- Modify: `crates/chartpds-core/src/queries/observation_history.rs`, `crates/chartpds-core/src/queries/latest_by_coding.rs` (SELECT + mapping)
- Modify (mechanical — add one field to each `InsertParams` literal): `crates/chartpds-core/src/ingestion/ingest.rs`, `crates/chartpds-core/src/ingestion/ccda/vitals.rs`, `crates/chartpds-core/src/ingestion/narrative.rs`, `crates/chartpds-core/src/sources/fitbit/storage.rs`, `crates/chartpds-core/src/sources/oura/storage.rs`, `crates/chartpds-core/src/queries/test_support.rs`, `crates/chartpds-core/src/queries/day_confidence.rs`, `crates/chartpds-core/src/queries/observation_stats.rs`, `crates/chartpds-core/src/queries/current_problems.rs`, `crates/chartpds-core/src/queries/get_narrative.rs` (only where these files construct `InsertObservationParams`/`InsertProblemParams`, mostly in tests)
- Modify: `.sqlx/` cache via `just prepare-sql`

**Interfaces:**
- Produces: `Observation.derivation: String` and `Problem.derivation: String` (serialized into MCP outputs automatically); `InsertObservationParams.derivation: &'a str` and `InsertProblemParams.derivation: &'a str`. Values are exactly `"structured"`, `"verbatim"`, `"inferred"`. Task 6 inserts journal observations with `derivation: "inferred"`.

- [ ] **Step 1: Write the migration**

`crates/chartpds-core/migrations/0014_derivation.sql`:

```sql
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
```

- [ ] **Step 2: Make existing tests demand the field (failing tests)**

In `crates/chartpds-core/src/index/observations.rs` test `insert_and_list_for_source_document_round_trips`, add `derivation: "structured",` to the `InsertParams` literal and this assert after the existing ones:

```rust
        assert_eq!(rows[0].derivation, "structured");
```

In `crates/chartpds-core/src/ingestion/narrative.rs` test `ingests_pdf_with_verified_extraction`, after the existing `section_label` assert:

```rust
        assert_eq!(
            problems[0].derivation, "verbatim",
            "narrative-extracted problems are verbatim-derived"
        );
```

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core observations`
Expected: COMPILE FAIL — no field `derivation`.

- [ ] **Step 3: Thread the field through the index layer**

`index/observations.rs`:
- `Observation` gains (after `value_unit`):
  ```rust
      /// How the coded claim was derived from its source document:
      /// `"structured"` (structured field), `"verbatim"` (code present in the
      /// grounding quote), or `"inferred"` (LLM mapped prose to the code).
      pub derivation: String,
  ```
- `InsertParams` gains:
  ```rust
      /// Derivation class: `"structured"`, `"verbatim"`, or `"inferred"`.
      pub derivation: &'a str,
  ```
- `insert`: add `derivation` to the column list, a `?` to VALUES, and `params.derivation` to the bind list.
- `list_by_source_document`: add `derivation` to the SELECT and `derivation: r.derivation,` to the mapping.

`index/problems.rs`: same four changes (`Problem` struct doc: same wording; SELECT in `list_by_source_document`; `insert` column/bind).

`queries/observation_history.rs` and `queries/latest_by_coding.rs`: add `derivation` to the SELECT column list and `derivation: r.derivation,` to the `Observation` construction.

- [ ] **Step 4: Fix every `InsertParams` construction site**

Find them all:

```bash
grep -rn "InsertObservationParams {\|InsertProblemParams {\|observations::InsertParams {\|problems::InsertParams {" crates/
```

Add one line to each literal:
- `derivation: "verbatim",` — ONLY in `ingestion/narrative.rs::apply_extraction` (the clinical-PDF problems insert).
- `derivation: "structured",` — everywhere else (CCDA ingest paths in `ingestion/ingest.rs` / `ingestion/ccda/vitals.rs`, Fitbit/Oura storage, and all test/seed sites in `queries/test_support.rs`, `queries/day_confidence.rs`, `queries/observation_stats.rs`, `queries/current_problems.rs`, `queries/get_narrative.rs`, `index/observations.rs`, `index/problems.rs`).

- [ ] **Step 5: Regenerate the sqlx cache and run the tests**

```bash
just prepare-sql
env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core
```

Expected: PASS (all chartpds-core tests, including the two new asserts).

- [ ] **Step 6: Commit (migration + cache together)**

```bash
git add crates/chartpds-core/migrations/0014_derivation.sql crates/chartpds-core/src/ .sqlx/
git -c user.email=sera@fhwang.net -c user.name="Francis Hwang" commit -m "Add derivation column to observations and problems

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Journal claim model + verification

**Files:**
- Create: `crates/chartpds-core/src/extraction/journal.rs`
- Modify: `crates/chartpds-core/src/extraction/verify.rs` (widen helper visibility)
- Modify: `crates/chartpds-core/src/extraction/mod.rs` (wire module + re-exports)

**Interfaces:**
- Consumes: `is_valid_icd10cm` (Task 1); `normalize_ws`, `contains_anchored`, `date_candidates` from `verify.rs`; `ExtractorInfo`, `ICD10_CM_SYSTEM` from `artifact.rs`.
- Produces (all re-exported from `chartpds_core::extraction`):
  - `RawJournalExtraction { entry_date: Option<String>, entry_date_quote: Option<String>, title: Option<String>, codings: Vec<RawJournalCoding> }` (Deserialize)
  - `RawJournalCoding { code: String, display: String, quote: String, severity: Option<f64> }` (Deserialize)
  - `JournalCoding { system: String, code: String, display: String, quote: String, severity: Option<f64> }` (Serialize + Deserialize)
  - `VerifiedJournalExtraction { entry_date: Option<String>, entry_date_quote: Option<String>, title: Option<String>, codings: Vec<JournalCoding>, invalid_codes: Vec<String>, rejected: Vec<String> }`
  - `JournalExtractionArtifact { document: String, entry_date: String, title: Option<String>, codings: Vec<JournalCoding>, extractor: ExtractorInfo, extracted_at: OffsetDateTime }` (Serialize + Deserialize)
  - `pub fn verify_journal_extraction(text: &str, raw: RawJournalExtraction) -> VerifiedJournalExtraction`

- [ ] **Step 1: Widen helper visibility in `verify.rs`**

Change `fn contains_anchored` and `fn date_candidates` from private to `pub(super)`. (`normalize_ws` is already `pub(crate)`.)

- [ ] **Step 2: Write the failing tests**

Create `extraction/journal.rs` with the test module (types come in step 4):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::extraction::artifact::{ExtractorInfo, ICD10_CM_SYSTEM};

    const ENTRY: &str = "Left shoulder aching again after climbing.\n\
        Pain was maybe a 6 today. Slept badly, kept waking up.\n";

    fn raw_coding(code: &str, quote: &str, severity: Option<f64>) -> RawJournalCoding {
        RawJournalCoding {
            code: code.to_owned(),
            display: "Pain in left shoulder".to_owned(),
            quote: quote.to_owned(),
            severity,
        }
    }

    fn raw(codings: Vec<RawJournalCoding>) -> RawJournalExtraction {
        RawJournalExtraction {
            entry_date: None,
            entry_date_quote: None,
            title: Some("Journal — shoulder ache".to_owned()),
            codings,
        }
    }

    #[test]
    fn accepts_inferred_coding_whose_quote_grounds_and_code_is_real() {
        // The code M25.512 appears nowhere in ENTRY — that is the point:
        // inferred codings waive the code-in-quote rule.
        let v = verify_journal_extraction(
            ENTRY,
            raw(vec![raw_coding(
                "M25.512",
                "Left shoulder aching again after climbing. Pain was maybe a 6 today.",
                Some(6.0),
            )]),
        );
        assert_eq!(v.codings.len(), 1);
        assert_eq!(v.codings[0].system, ICD10_CM_SYSTEM);
        assert_eq!(v.codings[0].code, "M25.512");
        assert_eq!(v.codings[0].severity, Some(6.0));
        assert!(v.rejected.is_empty());
        assert!(v.invalid_codes.is_empty());
    }

    #[test]
    fn rejects_quote_not_in_entry() {
        let v = verify_journal_extraction(
            ENTRY,
            raw(vec![raw_coding("M25.512", "My knee hurts", None)]),
        );
        assert!(v.codings.is_empty());
        assert_eq!(v.rejected.len(), 1);
        assert!(v.rejected[0].contains("quote not found"));
    }

    #[test]
    fn rejects_invalid_code_and_reports_it_for_retry_feedback() {
        let v = verify_journal_extraction(
            ENTRY,
            raw(vec![raw_coding(
                "M25.5129",
                "Left shoulder aching again after climbing.",
                None,
            )]),
        );
        assert!(v.codings.is_empty());
        assert_eq!(v.invalid_codes, vec!["M25.5129".to_owned()]);
        assert_eq!(v.rejected.len(), 1);
        assert!(v.rejected[0].contains("not a valid ICD-10-CM code"));
    }

    #[test]
    fn drops_severity_not_stated_in_quote_but_keeps_the_coding() {
        let v = verify_journal_extraction(
            ENTRY,
            raw(vec![raw_coding(
                "M25.512",
                "Left shoulder aching again after climbing.",
                Some(6.0), // "6" is in the entry but NOT in this quote
            )]),
        );
        assert_eq!(v.codings.len(), 1);
        assert_eq!(v.codings[0].severity, None, "unstated severity dropped");
        assert_eq!(v.rejected.len(), 1);
        assert!(v.rejected[0].contains("severity"));
    }

    #[test]
    fn severity_must_not_match_inside_a_longer_number() {
        let text = "Rated it 26 out of 100 on the clinic form.";
        let v = verify_journal_extraction(
            text,
            RawJournalExtraction {
                entry_date: None,
                entry_date_quote: None,
                title: None,
                codings: vec![raw_coding(
                    "R52",
                    "Rated it 26 out of 100 on the clinic form.",
                    Some(6.0),
                )],
            },
        );
        assert_eq!(v.codings[0].severity, None, "6 inside 26 must not count");
    }

    #[test]
    fn verifies_entry_date_like_document_date() {
        let text = "July 26, 2026. Shoulder felt fine.";
        let v = verify_journal_extraction(
            text,
            RawJournalExtraction {
                entry_date: Some("2026-07-26".to_owned()),
                entry_date_quote: Some("July 26, 2026".to_owned()),
                title: None,
                codings: vec![],
            },
        );
        assert_eq!(v.entry_date.as_deref(), Some("2026-07-26"));

        let bad = verify_journal_extraction(
            text,
            RawJournalExtraction {
                entry_date: Some("2026-07-27".to_owned()),
                entry_date_quote: Some("July 26, 2026".to_owned()),
                title: None,
                codings: vec![],
            },
        );
        assert_eq!(bad.entry_date, None);
        assert_eq!(bad.rejected.len(), 1);
    }

    #[test]
    fn artifact_round_trips_through_json() {
        let a = JournalExtractionArtifact {
            document: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .to_owned(),
            entry_date: "2026-07-26".to_owned(),
            title: Some("Journal — shoulder ache".to_owned()),
            codings: vec![JournalCoding {
                system: ICD10_CM_SYSTEM.to_owned(),
                code: "M25.512".to_owned(),
                display: "Pain in left shoulder".to_owned(),
                quote: "Left shoulder aching again".to_owned(),
                severity: Some(6.0),
            }],
            extractor: ExtractorInfo {
                model: "claude-opus-4-8".to_owned(),
                prompt_version: 1,
            },
            extracted_at: time::macros::datetime!(2026-07-26 12:00:00 UTC),
        };
        let bytes = serde_json::to_vec(&a).expect("serialize");
        let back: JournalExtractionArtifact = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(a, back);
    }
}
```

Wire the module in `extraction/mod.rs`:

```rust
mod journal;
```

and extend the `pub use` list:

```rust
pub use journal::{
    verify_journal_extraction, JournalCoding, JournalExtractionArtifact, RawJournalCoding,
    RawJournalExtraction, VerifiedJournalExtraction,
};
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core extraction::journal`
Expected: COMPILE FAIL — types not defined.

- [ ] **Step 4: Write the implementation**

Above the test module in `journal.rs`:

```rust
//! Journal-entry claim model and verification.
//!
//! Journal codings are *inferred*: the LLM maps colloquial prose ("my left
//! shoulder has been aching") onto an ICD-10-CM code that appears nowhere in
//! the source text. The clinical-PDF rule "code must appear in its quote"
//! is therefore structurally impossible here and is replaced by a vocabulary
//! check against the vendored ICD-10-CM table. The quote-grounds-in-text
//! rule stays mandatory, and a claimed numeric severity must literally
//! appear in the grounding quote — otherwise the severity (not the coding)
//! is dropped. Index rows produced from these claims carry
//! `derivation = 'inferred'`.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use super::artifact::{ExtractorInfo, ICD10_CM_SYSTEM};
use super::icd10::is_valid_icd10cm;
use super::verify::{contains_anchored, date_candidates, normalize_ws};

/// Un-verified journal LLM output, as parsed from the structured response.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RawJournalExtraction {
    /// Claimed entry date (ISO-8601), if the entry text states one.
    pub entry_date: Option<String>,
    /// Claimed verbatim span containing the date.
    pub entry_date_quote: Option<String>,
    /// Proposed title.
    pub title: Option<String>,
    /// Proposed codings, pre-verification.
    pub codings: Vec<RawJournalCoding>,
}

/// One un-verified proposed journal coding.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RawJournalCoding {
    /// Proposed ICD-10-CM code (inferred — not expected in the text).
    pub code: String,
    /// Standard ICD-10-CM description for the code.
    pub display: String,
    /// Claimed verbatim span describing the complaint.
    pub quote: String,
    /// Author-stated numeric severity, only when explicitly written.
    pub severity: Option<f64>,
}

/// One verified journal coding: quote grounded, code in the vocabulary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JournalCoding {
    /// Coding system URI (always [`ICD10_CM_SYSTEM`]).
    pub system: String,
    /// The inferred ICD-10-CM code.
    pub code: String,
    /// Standard ICD-10-CM description.
    pub display: String,
    /// Verbatim entry span the inference was grounded in.
    pub quote: String,
    /// Author-stated severity; verified to appear in the quote.
    pub severity: Option<f64>,
}

/// Journal extraction output that survived verification.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedJournalExtraction {
    /// Verified entry date (ISO-8601), if claimed and provable.
    pub entry_date: Option<String>,
    /// The verbatim span supporting the date.
    pub entry_date_quote: Option<String>,
    /// Title (passed through unverified — presentational only).
    pub title: Option<String>,
    /// Codings that verified.
    pub codings: Vec<JournalCoding>,
    /// Codes dropped for failing the ICD-10-CM table check — surfaced
    /// separately so ingestion can feed them back to the model for one
    /// corrective retry.
    pub invalid_codes: Vec<String>,
    /// Human-readable reasons for every dropped claim.
    pub rejected: Vec<String>,
}

/// The frozen extraction artifact for one journal entry. Replayed verbatim
/// on rebuild — never regenerated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JournalExtractionArtifact {
    /// SHA-256 hex of the journal blob this artifact describes.
    pub document: String,
    /// The entry's calendar date (ISO-8601). Required: undated entries are
    /// rejected at ingest, so every artifact carries a date.
    pub entry_date: String,
    /// Short human-readable label (extractor-authored, not verified).
    pub title: Option<String>,
    /// Verified codings.
    pub codings: Vec<JournalCoding>,
    /// Who produced this artifact.
    pub extractor: ExtractorInfo,
    /// When extraction ran (RFC 3339).
    #[serde(with = "time::serde::rfc3339")]
    pub extracted_at: OffsetDateTime,
}

/// True when `severity` is literally stated in the (normalized) quote,
/// not flanked by other digits (a 6 inside "26" does not count).
fn severity_stated(norm_quote: &str, severity: f64) -> bool {
    contains_anchored(norm_quote, &format!("{severity}"))
}

/// Verify raw journal LLM output against the entry text.
///
/// Rules: quote must ground in the text (whitespace-normalized substring);
/// the code must be a real ICD-10-CM code per the vendored table (the
/// code-in-quote rule is waived — journal codes are inferred); a claimed
/// severity must appear in the quote, else the severity alone is dropped;
/// a claimed entry date is verified exactly like a clinical document date.
#[must_use]
pub fn verify_journal_extraction(
    text: &str,
    raw: RawJournalExtraction,
) -> VerifiedJournalExtraction {
    let norm_text = normalize_ws(text);
    let mut rejected = Vec::new();
    let mut invalid_codes = Vec::new();

    let mut codings = Vec::new();
    for c in raw.codings {
        let norm_quote = normalize_ws(&c.quote);
        if c.code.trim().is_empty() {
            rejected.push("coding with empty code".to_owned());
        } else if norm_quote.is_empty() {
            rejected.push(format!("coding {}: empty quote", c.code));
        } else if !norm_text.contains(&norm_quote) {
            rejected.push(format!(
                "coding {}: quote not found in entry text: {:?}",
                c.code, c.quote
            ));
        } else if !is_valid_icd10cm(&c.code) {
            rejected.push(format!(
                "coding {}: not a valid ICD-10-CM code (vendored FY2026 table)",
                c.code
            ));
            invalid_codes.push(c.code);
        } else {
            let severity = match c.severity {
                Some(s) if severity_stated(&norm_quote, s) => Some(s),
                Some(s) => {
                    rejected.push(format!(
                        "coding {}: severity {s} not stated in quote {:?} — kept the coding, dropped the severity",
                        c.code, c.quote
                    ));
                    None
                }
                None => None,
            };
            codings.push(JournalCoding {
                system: ICD10_CM_SYSTEM.to_owned(),
                code: c.code,
                display: c.display,
                quote: c.quote,
                severity,
            });
        }
    }

    let (entry_date, entry_date_quote) = match (raw.entry_date, raw.entry_date_quote) {
        (Some(date), Some(quote)) => {
            let norm_quote = normalize_ws(&quote);
            match date_candidates(&date) {
                Some(cands)
                    if norm_text.contains(&norm_quote)
                        && cands.iter().any(|c| contains_anchored(&norm_quote, c)) =>
                {
                    (Some(date), Some(quote))
                }
                _ => {
                    rejected.push(format!(
                        "entry_date {date}: quote missing from text or date not in quote: {quote:?}"
                    ));
                    (None, None)
                }
            }
        }
        (Some(date), None) => {
            rejected.push(format!("entry_date {date}: no supporting quote"));
            (None, None)
        }
        (None, _) => (None, None),
    };

    VerifiedJournalExtraction {
        entry_date,
        entry_date_quote,
        title: raw.title,
        codings,
        invalid_codes,
        rejected,
    }
}
```

Note: `RawJournalExtraction`/`RawJournalCoding`/`JournalCoding` deliberately derive `PartialEq` without `Eq` (they contain `f64`).

- [ ] **Step 5: Run tests to verify they pass**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core extraction`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/chartpds-core/src/extraction/
git -c user.email=sera@fhwang.net -c user.name="Francis Hwang" commit -m "Journal claim model and verification (inferred ICD-10-CM codings)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Journal LLM extraction

**Files:**
- Modify: `crates/chartpds-core/src/extraction/llm.rs`
- Modify: `crates/chartpds-core/src/extraction/mod.rs` (re-exports)

**Interfaces:**
- Consumes: `RawJournalExtraction` (Task 3).
- Produces (re-exported from `chartpds_core::extraction`):
  - `pub trait JournalExtractor { fn extract_journal(&self, text: &str, feedback: Option<&str>) -> impl Future<Output = Result<RawJournalExtraction, Error>> + Send; }`
  - `pub const JOURNAL_PROMPT_VERSION: u32 = 1;`
  - `ClaudeExtractor` implements `JournalExtractor` (same retry mechanics as `extract`).

- [ ] **Step 1: Write the failing tests**

Add to `llm.rs` tests:

```rust
    #[test]
    fn journal_request_body_pins_model_schema_and_embeds_entry() {
        let body = build_journal_request_body("SAMPLE JOURNAL ENTRY", None);
        assert_eq!(body["model"], EXTRACTION_MODEL);
        assert_eq!(body["output_config"]["format"]["type"], "json_schema");
        assert_eq!(body["thinking"]["type"], "adaptive");
        let content = body["messages"][0]["content"].as_str().expect("content");
        assert!(content.contains("SAMPLE JOURNAL ENTRY"));
        assert!(content.contains("ICD-10-CM"));
        assert!(content.contains("severity"));
        assert!(!content.contains("invalid ICD-10-CM codes"), "no feedback block");
        // Schema must include the journal-only fields.
        let schema = &body["output_config"]["format"]["schema"];
        assert!(schema["properties"]["entry_date"].is_object());
        assert!(
            schema["properties"]["codings"]["items"]["properties"]["severity"].is_object()
        );
    }

    #[test]
    fn journal_feedback_is_appended_to_the_prompt() {
        let body = build_journal_request_body(
            "ENTRY",
            Some("M25.5129 is not a valid ICD-10-CM code"),
        );
        let content = body["messages"][0]["content"].as_str().expect("content");
        assert!(content.contains("M25.5129 is not a valid ICD-10-CM code"));
    }

    #[tokio::test]
    async fn extract_journal_parses_a_successful_response() {
        let (base_url, _hits) = scripted_server(vec![(
            200,
            serde_json::json!({
                "stop_reason": "end_turn",
                "content": [{
                    "type": "text",
                    "text": r#"{"entry_date":null,"entry_date_quote":null,"title":"Journal","codings":[{"code":"M25.512","display":"Pain in left shoulder","quote":"shoulder aching","severity":6}]}"#
                }]
            })
            .to_string(),
        )]);
        let raw = fast_retry_extractor(base_url)
            .extract_journal("shoulder aching", None)
            .await
            .expect("extract");
        assert_eq!(raw.codings.len(), 1);
        assert_eq!(raw.codings[0].code, "M25.512");
        assert_eq!(raw.codings[0].severity, Some(6.0));
    }
```

Add `use super::super::journal::RawJournalExtraction;`-style imports as needed (the file already does `use super::artifact::RawExtraction;` — add `use super::journal::RawJournalExtraction;` at the top of `llm.rs`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core llm`
Expected: COMPILE FAIL — `build_journal_request_body` / `extract_journal` not found.

- [ ] **Step 3: Implement**

In `llm.rs`:

1. Add the version const next to `PROMPT_VERSION`:

```rust
/// Version of the journal extraction request ([`JOURNAL_PROMPT`] plus
/// request configuration). Bump when either changes in a way that affects
/// output.
pub const JOURNAL_PROMPT_VERSION: u32 = 1;
```

2. Add the prompt next to `EXTRACTION_PROMPT`:

```rust
const JOURNAL_PROMPT: &str = "You are reading one personal health-journal entry, written by a \
patient in casual, non-medical language. Map the complaints and symptoms the author describes \
onto ICD-10-CM codes so the entry can be found and analyzed alongside clinical records. The \
author is not a clinician: do not diagnose, and prefer coarse, common symptom/complaint codes \
(R-codes; site-specific pain codes such as M25.512 Pain in left shoulder) over specific disease \
codes. Use the same code for the same complaint every time.\n\
\n\
Extract:\n\
1. entry_date: the calendar date the entry is about, formatted YYYY-MM-DD, ONLY if a date \
appears in the entry text itself, with entry_date_quote set to an exact verbatim span \
containing that date. Otherwise null for both.\n\
2. title: a short human-readable label, e.g. \"Journal — left shoulder ache, poor sleep\".\n\
3. codings: one per distinct symptom or complaint the author describes as their own, current \
experience. For each: code = a valid ICD-10-CM code for the complaint; display = the standard \
ICD-10-CM description; quote = an exact verbatim span from the entry describing the complaint; \
severity = a number ONLY when the author explicitly writes a numeric rating (e.g. \"pain was a \
6 today\" -> 6) and that number appears inside the quote, otherwise null. Never turn words like \
\"awful\" into a number.\n\
\n\
Do not code things the author denies, describes in someone else, or mentions only as history. \
Copy quotes exactly — they are checked mechanically against the entry, and any quote that is \
not a verbatim substring is discarded.";
```

3. Add the journal schema next to `output_schema`:

```rust
/// The JSON schema for journal extraction (structured outputs).
fn journal_output_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "entry_date": {"type": ["string", "null"]},
            "entry_date_quote": {"type": ["string", "null"]},
            "title": {"type": ["string", "null"]},
            "codings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string"},
                        "display": {"type": "string"},
                        "quote": {"type": "string"},
                        "severity": {"type": ["number", "null"]}
                    },
                    "required": ["code", "display", "quote", "severity"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["entry_date", "entry_date_quote", "title", "codings"],
        "additionalProperties": false
    })
}

/// Build the `POST /v1/messages` request body for one journal entry.
/// `feedback` carries rejection reasons from a prior attempt (e.g. invalid
/// codes) so the model can correct itself on the single semantic retry.
fn build_journal_request_body(text: &str, feedback: Option<&str>) -> serde_json::Value {
    let mut prompt = JOURNAL_PROMPT.to_owned();
    if let Some(fb) = feedback {
        prompt.push_str("\n\nA previous attempt was rejected for these reasons — correct them:\n");
        prompt.push_str(fb);
    }
    serde_json::json!({
        "model": EXTRACTION_MODEL,
        "max_tokens": 16000,
        "thinking": {"type": "adaptive"},
        "output_config": {"format": {"type": "json_schema", "schema": journal_output_schema()}},
        "messages": [{
            "role": "user",
            "content": format!("{prompt}\n\n<entry>\n{text}\n</entry>"),
        }],
    })
}
```

4. Genericize response parsing. Replace `parse_response`'s body with a generic helper and keep the old name for the PDF path:

```rust
/// Parse the Messages API response body into the expected structured type.
fn parse_response_as<T: serde::de::DeserializeOwned>(
    body: &serde_json::Value,
) -> Result<T, Error> {
    // ... identical to the current parse_response, except the final line:
    serde_json::from_str(text).map_err(|err| Error::InvalidResponse {
        reason: format!("structured output did not parse as expected type: {err}"),
    })
}

/// Parse the Messages API response body into a [`RawExtraction`].
fn parse_response(body: &serde_json::Value) -> Result<RawExtraction, Error> {
    parse_response_as(body)
}
```

(Keep the existing stop_reason refusal/max_tokens handling inside `parse_response_as` verbatim.)

5. Genericize the retry transport. Change `ClaudeExtractor::attempt` to return the raw JSON body instead of parsing, and add a retry wrapper; then both trait impls parse after the loop:

```rust
impl ClaudeExtractor {
    /// One `POST /v1/messages` attempt, returning the raw response JSON. ...
    async fn attempt(&self, body: &serde_json::Value) -> Result<serde_json::Value, (Error, bool)> {
        // identical to today, minus the final parse_response call:
        // the last line becomes `Ok(body)` after `response.json()`.
    }

    /// Run [`Self::attempt`] with the bounded transient-retry loop.
    async fn request_with_retries(
        &self,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let mut attempt = 1u32;
        loop {
            match self.attempt(body).await {
                Ok(v) => return Ok(v),
                Err((error, transient)) => {
                    if !transient || attempt >= MAX_ATTEMPTS {
                        return Err(error);
                    }
                    tracing::warn!(attempt, error = %error, "transient extraction failure; retrying");
                    tokio::time::sleep(self.retry_backoff * attempt).await;
                    attempt += 1;
                }
            }
        }
    }
}

impl LlmExtractor for ClaudeExtractor {
    async fn extract(&self, text: &str) -> Result<RawExtraction, Error> {
        let body = build_request_body(text);
        let v = self.request_with_retries(&body).await?;
        parse_response(&v)
    }
}
```

Behavior note: today a parse failure inside `attempt` is marked non-transient and aborts the loop; after this refactor parsing happens once after the loop — same observable behavior (parse failures were never retried), and the existing `garbage_content_maps_to_invalid_response` / retry tests must still pass unchanged.

6. Add the trait and impl:

```rust
/// Anything that can turn one journal entry into a [`RawJournalExtraction`].
///
/// Separate from [`LlmExtractor`] because the journal prompt, output schema,
/// and feedback-retry contract differ; ingestion tests use canned impls.
pub trait JournalExtractor {
    /// Extract structured claims from one journal entry. `feedback`, when
    /// present, carries rejection reasons from a prior attempt (invalid
    /// codes) for a single corrective retry.
    fn extract_journal(
        &self,
        text: &str,
        feedback: Option<&str>,
    ) -> impl Future<Output = Result<RawJournalExtraction, Error>> + Send;
}

impl JournalExtractor for ClaudeExtractor {
    async fn extract_journal(
        &self,
        text: &str,
        feedback: Option<&str>,
    ) -> Result<RawJournalExtraction, Error> {
        let body = build_journal_request_body(text, feedback);
        let v = self.request_with_retries(&body).await?;
        parse_response_as(&v)
    }
}
```

7. Update the `llm.rs` module `//!` header (mentions one prompt today) to say it holds both the clinical-PDF and journal prompts. Extend `extraction/mod.rs` re-exports:

```rust
pub use llm::{
    ClaudeExtractor, JournalExtractor, LlmExtractor, EXTRACTION_MODEL, JOURNAL_PROMPT_VERSION,
    PROMPT_VERSION,
};
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core llm`
Expected: PASS — new journal tests and all pre-existing retry/parse tests.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-core/src/extraction/
git -c user.email=sera@fhwang.net -c user.name="Francis Hwang" commit -m "Journal LLM extraction: prompt, schema, feedback retry hook

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Deterministic entry-date resolution

**Files:**
- Create: `crates/chartpds-core/src/ingestion/journal.rs` (date resolution + errors only; orchestrator lands in Task 6)
- Modify: `crates/chartpds-core/src/ingestion/error.rs` (three new variants)
- Modify: `crates/chartpds-core/src/ingestion/mod.rs` (add `mod journal;`)

**Interfaces:**
- Produces:
  - `pub(crate) fn resolve_entry_date(original_filename: Option<&str>, text: &str) -> Result<Option<time::Date>>` — `Err(Error::MultiEntryJournal)` when ≥2 markdown headers parse as dates; `Ok(None)` when nothing deterministic.
  - `ingestion::Error` variants: `MultiEntryJournal`, `UndatedJournal`, `JournalNotUtf8` (Task 6 and the MCP layer rely on their `Display` messages).

- [ ] **Step 1: Add the error variants**

In `ingestion/error.rs`, add to the enum (following the existing thiserror style):

```rust
    /// A journal file contains more than one dated markdown header — it is
    /// a multi-entry file, which v1 does not support.
    #[error("journal file contains multiple dated headers; split it into one entry per file and re-run the ingest")]
    MultiEntryJournal,

    /// No verifiable date exists for a journal entry: nothing deterministic
    /// in the filename or headers, and the LLM fallback could not prove a
    /// date against the text.
    #[error("journal entry has no verifiable date: put a YYYY-MM-DD date in the filename, or a dated header with a year (e.g. \"# Jul 26, 2026\"), and re-run the ingest")]
    UndatedJournal,

    /// Journal ingestion was given bytes that are not UTF-8 text.
    #[error("journal file is not valid UTF-8 text")]
    JournalNotUtf8,
}
```

- [ ] **Step 2: Write the failing tests**

Create `ingestion/journal.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::date;

    #[test]
    fn filename_iso_date_wins() {
        let d = resolve_entry_date(Some("2026-07-26.md"), "no dates in text")
            .expect("resolve");
        assert_eq!(d, Some(date!(2026 - 07 - 26)));
    }

    #[test]
    fn header_full_date_forms_parse() {
        for text in [
            "# 2026-07-26\n\nbody",
            "# Jul 26, 2026\n\nbody",
            "# July 26, 2026\n\nbody",
            "# 7/26/2026\n\nbody",
            "## Journal for Jul 26, 2026\n\nbody",
        ] {
            let d = resolve_entry_date(None, text).expect("resolve");
            assert_eq!(d, Some(date!(2026 - 07 - 26)), "failed for {text:?}");
        }
    }

    #[test]
    fn yearless_header_takes_year_from_filename() {
        let d = resolve_entry_date(Some("journal-2026.md"), "# Jul 26\n\nbody")
            .expect("resolve");
        assert_eq!(d, Some(date!(2026 - 07 - 26)));
    }

    #[test]
    fn yearless_header_without_filename_year_is_not_deterministic() {
        let d = resolve_entry_date(None, "# Jul 26\n\nbody").expect("resolve");
        assert_eq!(d, None);
    }

    #[test]
    fn multiple_dated_headers_reject_as_multi_entry() {
        let err = resolve_entry_date(None, "# Jul 26, 2026\n\nfoo\n\n# Jul 27, 2026\n\nbar")
            .expect_err("multi-entry must reject");
        assert!(matches!(err, Error::MultiEntryJournal));
    }

    #[test]
    fn undated_headerless_text_is_not_deterministic() {
        let d = resolve_entry_date(None, "Shoulder still sore today.").expect("resolve");
        assert_eq!(d, None);
    }

    #[test]
    fn non_date_headers_are_ignored() {
        let d = resolve_entry_date(None, "# Morning notes\n\n# Evening notes\n\nbody")
            .expect("resolve");
        assert_eq!(d, None);
    }
}
```

Add `mod journal;` to `ingestion/mod.rs` (exports come in Task 6).

- [ ] **Step 3: Run tests to verify they fail**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core ingestion::journal`
Expected: COMPILE FAIL.

- [ ] **Step 4: Implement**

Above the test module in `ingestion/journal.rs`:

```rust
//! Journal-entry ingestion: free colloquial `.md` text → one-time verified
//! LLM inference of ICD-10-CM codings (frozen as an artifact in the derived
//! store) → `observations` rows marked `derivation = 'inferred'`.
//!
//! Entry-date resolution is deterministic-first (a `YYYY-MM-DD` in the
//! filename, else a single dated markdown header, a year-less header
//! borrowing its year from the filename), falling back to verified LLM
//! date extraction. A file with two or more dated headers is a multi-entry
//! file and is rejected; an entry no path can date is rejected — see
//! [`crate::ingestion::Error::MultiEntryJournal`] and
//! [`crate::ingestion::Error::UndatedJournal`].

use crate::ingestion::{Error, Result};
use time::{Date, Month};

/// Find a `YYYY-MM-DD` substring and parse it as a date.
fn iso_date_in(s: &str) -> Option<Date> {
    let b = s.as_bytes();
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    for i in 0..b.len().saturating_sub(9) {
        let w = &b[i..i + 10];
        let shaped = w
            .iter()
            .enumerate()
            .all(|(j, c)| if matches!(j, 4 | 7) { *c == b'-' } else { c.is_ascii_digit() });
        if shaped {
            // All-ASCII window, so the slice is on char boundaries.
            if let Ok(d) = Date::parse(&s[i..i + 10], &fmt) {
                return Some(d);
            }
        }
    }
    None
}

/// Find a standalone plausible 4-digit year (1900–2100).
fn year_in(s: &str) -> Option<i32> {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i].is_ascii_digit() {
            let start = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            if i - start == 4 {
                if let Ok(y) = s[start..i].parse::<i32>() {
                    if (1900..=2100).contains(&y) {
                        return Some(y);
                    }
                }
            }
        } else {
            i += 1;
        }
    }
    None
}

/// Month number for an English month name or 3-letter abbreviation.
fn month_from_name(token: &str) -> Option<Month> {
    const MONTHS: [&str; 12] = [
        "january", "february", "march", "april", "may", "june", "july", "august", "september",
        "october", "november", "december",
    ];
    let lower = token.to_ascii_lowercase();
    MONTHS
        .iter()
        .position(|m| *m == lower || (lower.len() == 3 && m.starts_with(&lower)))
        .and_then(|idx| Month::try_from(idx as u8 + 1).ok())
}

/// Parse one markdown-header's content as a date. Accepts ISO anywhere in
/// the header, `Month D, YYYY` / `Mon D, YYYY`, `M/D/YYYY`, and year-less
/// `Month D` / `Mon D` completed by `filename_year`.
fn parse_header_date(content: &str, filename_year: Option<i32>) -> Option<Date> {
    if let Some(d) = iso_date_in(content) {
        return Some(d);
    }
    let tokens: Vec<&str> = content
        .split(|c: char| c.is_whitespace() || c == ',' || c == '/')
        .filter(|t| !t.is_empty())
        .collect();
    // Scan for a month-name token anywhere (headers may carry a prefix,
    // e.g. "Journal for Jul 26, 2026").
    for (i, tok) in tokens.iter().enumerate() {
        if let Some(month) = month_from_name(tok) {
            if let Some(Ok(day)) = tokens.get(i + 1).map(|t| t.parse::<u8>()) {
                let year = tokens
                    .get(i + 2)
                    .and_then(|t| t.parse::<i32>().ok())
                    .filter(|y| (1900..=2100).contains(y))
                    .or(filename_year);
                if let Some(y) = year {
                    if let Ok(d) = Date::from_calendar_date(y, month, day) {
                        return Some(d);
                    }
                }
            }
        }
    }
    // Numeric M/D/YYYY (the '/' split above turned it into three tokens).
    if let [m, d, y] = tokens.as_slice() {
        if let (Ok(m), Ok(d), Ok(y)) = (m.parse::<u8>(), d.parse::<u8>(), y.parse::<i32>()) {
            if (1900..=2100).contains(&y) {
                if let Ok(month) = Month::try_from(m) {
                    if let Ok(date) = Date::from_calendar_date(y, month, d) {
                        return Some(date);
                    }
                }
            }
        }
    }
    None
}

/// Deterministically resolve one journal entry's date from its filename and
/// markdown headers.
///
/// # Errors
///
/// Returns [`Error::MultiEntryJournal`] when two or more headers parse as
/// dates (a multi-entry file).
pub(crate) fn resolve_entry_date(
    original_filename: Option<&str>,
    text: &str,
) -> Result<Option<Date>> {
    let filename_year = original_filename.and_then(year_in);
    let header_dates: Vec<Date> = text
        .lines()
        .map(str::trim_start)
        .filter(|l| l.starts_with('#'))
        .filter_map(|l| parse_header_date(l.trim_start_matches('#').trim(), filename_year))
        .collect();
    if header_dates.len() >= 2 {
        return Err(Error::MultiEntryJournal);
    }
    if let Some(d) = original_filename.and_then(iso_date_in) {
        return Ok(Some(d));
    }
    Ok(header_dates.into_iter().next())
}
```

Note the `idx as u8 + 1` cast: `idx` is 0–11 so this cannot truncate, but if clippy's `cast_possible_truncation` fires under the workspace lint set, use `u8::try_from(idx + 1).ok()?` instead of a lint bypass.

- [ ] **Step 5: Run tests to verify they pass**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core ingestion::journal`
Expected: PASS (7 tests).

- [ ] **Step 6: Commit**

```bash
git add crates/chartpds-core/src/ingestion/
git -c user.email=sera@fhwang.net -c user.name="Francis Hwang" commit -m "Deterministic journal entry-date resolution

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: `ingest_journal` orchestrator, artifact apply, replay

**Files:**
- Modify: `crates/chartpds-core/src/ingestion/journal.rs` (add orchestrator + apply + replay + tests)
- Modify: `crates/chartpds-core/src/ingestion/mod.rs` (exports + module docs)

**Interfaces:**
- Consumes: `resolve_entry_date` (Task 5); `verify_journal_extraction`, `JournalCoding`, `JournalExtractionArtifact` (Task 3); `JournalExtractor`, `EXTRACTION_MODEL`, `JOURNAL_PROMPT_VERSION` (Task 4); `NarrativeIngestParams` (existing, reused); index CRUD incl. `insert_observation` with `derivation` (Task 2); `Archive::put_with_manifest`.
- Produces (re-exported from `chartpds_core::ingestion`):
  - `pub const JOURNAL_KIND: &str = "journal";`
  - `pub const JOURNAL_EXTRACTION_KIND: &str = "journal-extraction";`
  - `pub struct JournalIngestOutcome { source_document_id: i64, title: Option<String>, entry_date: String, codings: Vec<JournalCoding>, rejected: Vec<String> }` (Serialize — the MCP layer returns it verbatim; `codings` carries code+display+quote+severity so the driving agent can echo mappings to the author)
  - `pub async fn ingest_journal<E: JournalExtractor>(archive: &Archive, derived: &Archive, pool: &SqlitePool, content: Bytes, params: NarrativeIngestParams<'_>, extractor: Option<&E>) -> Result<JournalIngestOutcome>`
  - `pub(crate) async fn apply_journal_extraction(pool: &SqlitePool, source_document_id: i64, artifact: &JournalExtractionArtifact) -> Result<u64>` (Task 7 calls it from rebuild)
  - `pub(crate) async fn replay_journal(pool: &SqlitePool, key: &BlobKey, content: &Bytes, manifest: &Manifest) -> Result<i64>` (Task 7)

- [ ] **Step 1: Write the failing tests**

Extend the `tests` module in `ingestion/journal.rs` (keep the Task 5 tests; add the following — plus the imports they need: `bytes::Bytes`, `time::OffsetDateTime`, `crate::archive::{Archive, Manifest}`, `crate::extraction::{JournalExtractor, RawJournalCoding, RawJournalExtraction}`, `crate::index::{list_observations_by_source_document, open_pool}`, `object_store::memory::InMemory`, `std::sync::Arc`, `std::sync::Mutex`):

```rust
    const ENTRY: &str = "# Jul 26, 2026\n\nLeft shoulder aching again after climbing. \
Pain was maybe a 6 today. Slept badly, kept waking up.\n";

    async fn fresh_pool_and_stores() -> (sqlx::SqlitePool, Archive, Archive) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        let derived_backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        (pool, Archive::new(backend), Archive::new(derived_backend))
    }

    /// Scripted journal extractor: pops canned responses in order and
    /// records the feedback each call received.
    struct MockJournalExtractor {
        responses: Mutex<Vec<RawJournalExtraction>>,
        feedback_seen: Mutex<Vec<Option<String>>>,
    }

    impl MockJournalExtractor {
        fn new(responses: Vec<RawJournalExtraction>) -> Self {
            Self {
                responses: Mutex::new(responses),
                feedback_seen: Mutex::new(Vec::new()),
            }
        }
    }

    impl JournalExtractor for MockJournalExtractor {
        async fn extract_journal(
            &self,
            _text: &str,
            feedback: Option<&str>,
        ) -> std::result::Result<RawJournalExtraction, crate::extraction::Error> {
            self.feedback_seen
                .lock()
                .expect("lock")
                .push(feedback.map(str::to_owned));
            Ok(self.responses.lock().expect("lock").remove(0))
        }
    }

    fn shoulder_extraction(code: &str) -> RawJournalExtraction {
        RawJournalExtraction {
            entry_date: None,
            entry_date_quote: None,
            title: Some("Journal — shoulder ache".to_owned()),
            codings: vec![RawJournalCoding {
                code: code.to_owned(),
                display: "Pain in left shoulder".to_owned(),
                quote: "Left shoulder aching again after climbing. Pain was maybe a 6 today."
                    .to_owned(),
                severity: Some(6.0),
            }],
        }
    }

    fn params(filename: Option<&'static str>) -> crate::ingestion::NarrativeIngestParams<'static> {
        crate::ingestion::NarrativeIngestParams {
            source: "journal",
            original_filename: filename,
            archived_at: time::macros::datetime!(2026-07-26 21:00:00 UTC),
        }
    }

    #[tokio::test]
    async fn ingests_entry_into_inferred_observations() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockJournalExtractor::new(vec![shoulder_extraction("M25.512")]);

        let outcome = ingest_journal(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(ENTRY.as_bytes()),
            params(Some("2026-07-26.md")),
            Some(&extractor),
        )
        .await
        .expect("ingest");

        assert_eq!(outcome.entry_date, "2026-07-26");
        assert_eq!(outcome.codings.len(), 1);
        assert_eq!(outcome.codings[0].code, "M25.512");
        assert_eq!(outcome.codings[0].severity, Some(6.0));
        assert!(
            !outcome.codings[0].quote.is_empty(),
            "outcome must echo the grounding quote for the author to audit"
        );

        // One blob per store.
        assert_eq!(archive.list_keys().await.expect("keys").len(), 1);
        assert_eq!(derived.list_keys().await.expect("keys").len(), 1);

        // The observation row: inferred, dated, severity as value_quantity.
        let obs = list_observations_by_source_document(&pool, outcome.source_document_id)
            .await
            .expect("observations");
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].coding_system, crate::extraction::ICD10_CM_SYSTEM);
        assert_eq!(obs[0].coding_code, "M25.512");
        assert_eq!(obs[0].derivation, "inferred");
        assert_eq!(obs[0].value_quantity, Some(6.0));
        assert_eq!(
            obs[0].effective_start,
            time::macros::datetime!(2026-07-26 00:00:00 UTC)
        );
        assert_eq!(obs[0].effective_end, None);

        // No problems rows — journal complaints stay out of the problem list.
        let problems =
            crate::index::list_problems_by_source_document(&pool, outcome.source_document_id)
                .await
                .expect("problems");
        assert!(problems.is_empty());

        // Document row and FTS text.
        let doc = crate::index::get_source_document_by_id(&pool, outcome.source_document_id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(doc.kind, JOURNAL_KIND);
        assert_eq!(doc.source, "journal");
        assert_eq!(doc.document_date.as_deref(), Some("2026-07-26"));
        let fts: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM narrative_texts_fts WHERE narrative_texts_fts MATCH 'climbing'",
        )
        .fetch_one(&pool)
        .await
        .expect("fts");
        assert_eq!(fts.0, 1);
    }

    #[tokio::test]
    async fn invalid_code_triggers_one_feedback_retry() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockJournalExtractor::new(vec![
            shoulder_extraction("M25.5129"), // invalid: not in the table
            shoulder_extraction("M25.512"),  // corrected on retry
        ]);

        let outcome = ingest_journal(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(ENTRY.as_bytes()),
            params(None),
            Some(&extractor),
        )
        .await
        .expect("ingest");

        let feedback = extractor.feedback_seen.lock().expect("lock").clone();
        assert_eq!(feedback.len(), 2, "exactly one semantic retry");
        assert_eq!(feedback[0], None);
        assert!(
            feedback[1].as_deref().is_some_and(|f| f.contains("M25.5129")),
            "retry feedback names the invalid code: {feedback:?}"
        );
        assert_eq!(outcome.codings.len(), 1);
        assert_eq!(outcome.codings[0].code, "M25.512");
        assert!(
            outcome.rejected.iter().any(|r| r.contains("M25.5129")),
            "first-attempt rejection stays visible: {:?}",
            outcome.rejected
        );
    }

    #[tokio::test]
    async fn zero_codings_is_accepted_and_text_indexed() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockJournalExtractor::new(vec![RawJournalExtraction {
            entry_date: None,
            entry_date_quote: None,
            title: Some("Journal — good day".to_owned()),
            codings: vec![],
        }]);

        let text = "# Jul 27, 2026\n\nFelt great. Long run, slept well.\n";
        let outcome = ingest_journal(
            &archive,
            &derived,
            &pool,
            Bytes::from(text.as_bytes().to_vec()),
            params(None),
            Some(&extractor),
        )
        .await
        .expect("zero codings must not fail the ingest");
        assert!(outcome.codings.is_empty());
        let fts: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM narrative_texts_fts WHERE narrative_texts_fts MATCH 'run'",
        )
        .fetch_one(&pool)
        .await
        .expect("fts");
        assert_eq!(fts.0, 1);
    }

    #[tokio::test]
    async fn undated_entry_fails_with_nothing_persisted() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockJournalExtractor::new(vec![RawJournalExtraction {
            entry_date: None,
            entry_date_quote: None,
            title: None,
            codings: vec![],
        }]);

        let err = ingest_journal(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(b"Shoulder still sore today.\n"),
            params(None),
            Some(&extractor),
        )
        .await
        .expect_err("undated entry must fail");
        assert!(matches!(err, Error::UndatedJournal));
        assert!(err.to_string().contains("YYYY-MM-DD"), "actionable: {err}");
        assert!(archive.list_keys().await.expect("keys").is_empty());
        assert!(derived.list_keys().await.expect("keys").is_empty());
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM source_documents")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(count.0, 0);
    }

    #[tokio::test]
    async fn no_extractor_fails_with_nothing_persisted() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let err = ingest_journal(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(ENTRY.as_bytes()),
            params(None),
            None::<&crate::extraction::ClaudeExtractor>,
        )
        .await
        .expect_err("missing extractor must fail");
        assert!(matches!(err, Error::ExtractorNotConfigured));
        assert!(archive.list_keys().await.expect("keys").is_empty());
        assert!(derived.list_keys().await.expect("keys").is_empty());
    }

    #[tokio::test]
    async fn non_utf8_bytes_fail_before_anything_persists() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockJournalExtractor::new(vec![]);
        let err = ingest_journal(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(&[0xFF, 0xFE, 0x00]),
            params(None),
            Some(&extractor),
        )
        .await
        .expect_err("non-utf8 must fail");
        assert!(matches!(err, Error::JournalNotUtf8));
        assert!(archive.list_keys().await.expect("keys").is_empty());
    }

    #[tokio::test]
    async fn re_ingest_of_same_entry_upserts_without_duplicates() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockJournalExtractor::new(vec![
            shoulder_extraction("M25.512"),
            shoulder_extraction("M25.512"),
        ]);
        for _ in 0..2 {
            ingest_journal(
                &archive,
                &derived,
                &pool,
                Bytes::from_static(ENTRY.as_bytes()),
                params(None),
                Some(&extractor),
            )
            .await
            .expect("ingest");
        }
        let doc_count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM source_documents WHERE kind = 'journal'")
                .fetch_one(&pool)
                .await
                .expect("count");
        assert_eq!(doc_count.0, 1, "same bytes must not duplicate");
        let obs_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM observations")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(obs_count.0, 1, "observations must not accumulate");
    }

    #[tokio::test]
    async fn replay_journal_rebuilds_document_and_text_from_a_bare_blob() {
        let (pool, archive, _derived) = fresh_pool_and_stores().await;
        let content = Bytes::from_static(ENTRY.as_bytes());
        let manifest = Manifest::new(
            "journal",
            JOURNAL_KIND,
            "text/markdown",
            Some("2026-07-26".to_owned()),
            time::macros::datetime!(2026-07-26 21:00:00 UTC),
            Some("2026-07-26.md".to_owned()),
        );
        let key = archive
            .put_with_manifest(content.clone(), manifest.clone())
            .await
            .expect("put");

        let id = replay_journal(&pool, &key, &content, &manifest)
            .await
            .expect("replay");
        let doc = crate::index::get_source_document_by_id(&pool, id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(doc.kind, JOURNAL_KIND);
        assert_eq!(doc.document_date.as_deref(), Some("2026-07-26"));
        let text = crate::index::get_narrative_text(&pool, id)
            .await
            .expect("get")
            .expect("present");
        assert!(text.text.contains("climbing"));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core ingestion::journal`
Expected: COMPILE FAIL — `ingest_journal` etc. not defined.

- [ ] **Step 3: Implement the orchestrator**

Add to `ingestion/journal.rs` (below the date-resolution code, above tests). Follow `narrative.rs` step-numbering style:

```rust
use bytes::Bytes;
use sqlx::SqlitePool;

use crate::archive::{Archive, BlobKey, Manifest};
use crate::extraction::{
    verify_journal_extraction, ExtractorInfo, JournalCoding, JournalExtractionArtifact,
    JournalExtractor, VerifiedJournalExtraction, EXTRACTION_MODEL, JOURNAL_PROMPT_VERSION,
};
use crate::index::{
    delete_source_document, fetch_source_document_by_archive_key, insert_observation,
    insert_source_document, set_narrative_title, set_source_document_date, upsert_narrative_text,
    InsertObservationParams, InsertSourceDocumentParams, UpsertNarrativeTextParams,
};
use crate::ingestion::narrative::NarrativeIngestParams;

/// `source_documents.kind` / manifest `type` for a journal-entry blob.
pub const JOURNAL_KIND: &str = "journal";
/// Manifest `type` for the frozen journal extraction artifact blob.
pub const JOURNAL_EXTRACTION_KIND: &str = "journal-extraction";

/// What `ingest_journal` did, reported in-band to the tool caller.
///
/// `codings` carries each verified coding with its grounding quote so the
/// driving agent can echo the mappings back to the author — the cheapest
/// audit of an inferred code is the author reading it at ingest time.
#[derive(Debug, Clone, serde::Serialize)]
pub struct JournalIngestOutcome {
    /// The `source_documents.id` of the ingested entry.
    pub source_document_id: i64,
    /// Extractor-authored title.
    pub title: Option<String>,
    /// The entry's resolved calendar date (ISO-8601).
    pub entry_date: String,
    /// Verified codings, with quotes and any stated severity.
    pub codings: Vec<JournalCoding>,
    /// Human-readable reasons for claims dropped by verification (both
    /// attempts, when the invalid-code retry ran).
    pub rejected: Vec<String>,
}

/// Ingest one journal entry (markdown or plain text, UTF-8).
///
/// Steps: decode UTF-8 (fail fast) → resolve a deterministic entry date
/// (filename, then a single dated header) → LLM inference + mechanical
/// verification, with one corrective retry when codes fail the ICD-10-CM
/// table → finalize the date (deterministic wins; verified LLM date as
/// fallback; no date fails the ingest) → archive the text blob (manifest
/// `subject` = entry date) → freeze the verified extraction in the derived
/// store → upsert index rows (document, narrative text, inferred
/// observations).
///
/// LLM extraction is required, exactly as for narrative PDFs: no extractor
/// or an exhausted-retries failure fails the whole ingest before anything
/// is archived or indexed. Zero verified codings is NOT a failure — a
/// good-day entry has nothing to code and still earns FTS retrieval.
///
/// # Errors
///
/// [`Error::JournalNotUtf8`], [`Error::MultiEntryJournal`],
/// [`Error::UndatedJournal`], [`Error::ExtractorNotConfigured`],
/// [`Error::Extraction`] — all before anything persists — plus
/// [`Error::Archive`]/[`Error::Database`] on storage failures.
pub async fn ingest_journal<E: JournalExtractor>(
    archive: &Archive,
    derived: &Archive,
    pool: &SqlitePool,
    content: Bytes,
    params: NarrativeIngestParams<'_>,
    extractor: Option<&E>,
) -> Result<JournalIngestOutcome> {
    let NarrativeIngestParams {
        source,
        original_filename,
        archived_at,
    } = params;

    // 1. Decode. Journal entries are text by definition.
    let text = std::str::from_utf8(&content).map_err(|_| Error::JournalNotUtf8)?;

    // 2. Deterministic date first (may reject a multi-entry file).
    let deterministic_date = resolve_entry_date(original_filename, text)?;

    // 3. LLM inference + verification, with one corrective retry when the
    //    model proposed codes that are not in the ICD-10-CM table.
    let Some(extractor) = extractor else {
        return Err(Error::ExtractorNotConfigured);
    };
    let verified = extract_and_verify(extractor, text).await?;

    // 4. Final date: deterministic wins; verified LLM date is the fallback;
    //    nothing verifiable fails the ingest before anything persists.
    let entry_date = match deterministic_date {
        Some(d) => format_iso_date(d),
        None => verified.entry_date.clone().ok_or(Error::UndatedJournal)?,
    };

    // 5-6. Archive the text blob and freeze the artifact.
    let (key, artifact) = archive_journal_blobs(
        archive,
        derived,
        content,
        source,
        original_filename,
        archived_at,
        &entry_date,
        &verified,
    )
    .await?;

    // 7. Upsert the document row (re-ingest of the same bytes replaces the
    //    prior rows; cascade cleans narrative_texts + observations and the
    //    FTS delete trigger fires on the cascade).
    if let Some(existing) = fetch_source_document_by_archive_key(pool, &key).await? {
        delete_source_document(pool, existing.id).await?;
    }
    let source_document_id = insert_source_document(
        pool,
        InsertSourceDocumentParams {
            archive_key: &key,
            kind: JOURNAL_KIND,
            source,
            original_filename,
            archived_at,
            document_date: None, // applied from the artifact below
        },
    )
    .await?;

    // 8. Index the text (FTS via triggers).
    upsert_narrative_text(
        pool,
        UpsertNarrativeTextParams {
            source_document_id,
            title: None, // applied from the artifact below
            text,
        },
    )
    .await?;

    // 9. Apply the artifact (date, title, inferred observations).
    apply_journal_extraction(pool, source_document_id, &artifact).await?;

    Ok(JournalIngestOutcome {
        source_document_id,
        title: artifact.title,
        entry_date,
        codings: artifact.codings,
        rejected: verified.rejected,
    })
}

/// ISO-8601 (`YYYY-MM-DD`) rendering of a date.
fn format_iso_date(d: Date) -> String {
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    d.format(&fmt).unwrap_or_else(|_| d.to_string())
}

/// LLM inference + verification with at most one corrective retry, fired
/// only when the first attempt proposed codes missing from the ICD-10-CM
/// table. The retry's verification result wins; the first attempt's
/// rejection reasons are prepended (marked) so nothing dropped goes
/// unreported.
async fn extract_and_verify<E: JournalExtractor>(
    extractor: &E,
    text: &str,
) -> Result<VerifiedJournalExtraction> {
    let first = verify_journal_extraction(text, extractor.extract_journal(text, None).await?);
    if first.invalid_codes.is_empty() {
        return Ok(first);
    }
    let feedback = first
        .rejected
        .iter()
        .filter(|r| r.contains("not a valid ICD-10-CM code"))
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    let mut second =
        verify_journal_extraction(text, extractor.extract_journal(text, Some(&feedback)).await?);
    let mut rejected: Vec<String> = first
        .rejected
        .into_iter()
        .map(|r| format!("(first attempt) {r}"))
        .collect();
    rejected.append(&mut second.rejected);
    second.rejected = rejected;
    Ok(second)
}

/// Archive the journal text blob and freeze its verified extraction as an
/// artifact blob in the derived store (same two-store split as
/// [`super::narrative::archive_narrative_blobs`]).
#[expect(clippy::too_many_arguments, reason = "internal step splitter for ingest_journal, mirrors archive_narrative_blobs")]
async fn archive_journal_blobs(
    archive: &Archive,
    derived: &Archive,
    content: Bytes,
    source: &str,
    original_filename: Option<&str>,
    archived_at: time::OffsetDateTime,
    entry_date: &str,
    verified: &VerifiedJournalExtraction,
) -> Result<(BlobKey, JournalExtractionArtifact)> {
    let manifest = Manifest::new(
        source,
        JOURNAL_KIND,
        "text/markdown",
        Some(entry_date.to_owned()),
        archived_at,
        original_filename.map(str::to_owned),
    );
    let key = archive.put_with_manifest(content, manifest).await?;

    let artifact = JournalExtractionArtifact {
        document: key.to_string(),
        entry_date: entry_date.to_owned(),
        title: verified.title.clone(),
        codings: verified.codings.clone(),
        extractor: ExtractorInfo {
            model: EXTRACTION_MODEL.to_owned(),
            prompt_version: JOURNAL_PROMPT_VERSION,
        },
        extracted_at: archived_at,
    };
    let bytes = serde_json::to_vec(&artifact).map_err(|err| {
        Error::Extraction(crate::extraction::Error::InvalidResponse {
            reason: format!("serializing journal artifact: {err}"),
        })
    })?;
    let artifact_manifest = Manifest::new(
        "chartpds",
        JOURNAL_EXTRACTION_KIND,
        "application/json",
        Some(key.to_string()),
        archived_at,
        None,
    );
    derived
        .put_with_manifest(Bytes::from(bytes), artifact_manifest)
        .await?;

    Ok((key, artifact))
}
```

If the codebase's pinned toolchain rejects `#[expect]`, group the archive/date/provenance arguments into a small internal struct instead of allowing the lint — do NOT use a reasonless `#[allow]`. (Alternative that avoids the question entirely: reuse `NarrativeIngestParams` as a field: `async fn archive_journal_blobs(archive, derived, content, params: NarrativeIngestParams<'_>, entry_date, verified)` — prefer this shape if the argument-count lint fires.)

Then the apply + replay functions:

```rust
/// Apply a frozen journal extraction artifact to an indexed journal
/// document: set the document date and title, insert one inferred
/// `observations` row per coding (entry-date midnight UTC, severity as
/// `value_quantity`, `derivation = 'inferred'`).
///
/// Shared by live ingestion and `rebuild_index` — the ONLY code path that
/// turns a journal artifact into index rows.
pub(crate) async fn apply_journal_extraction(
    pool: &SqlitePool,
    source_document_id: i64,
    artifact: &JournalExtractionArtifact,
) -> Result<u64> {
    set_source_document_date(pool, source_document_id, &artifact.entry_date).await?;
    if let Some(title) = &artifact.title {
        set_narrative_title(pool, source_document_id, title).await?;
    }
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    let date = Date::parse(&artifact.entry_date, &fmt).map_err(|err| {
        Error::Extraction(crate::extraction::Error::InvalidResponse {
            reason: format!(
                "journal artifact entry_date {:?} unparseable: {err}",
                artifact.entry_date
            ),
        })
    })?;
    let effective_start = date.midnight().assume_utc();
    let mut count = 0u64;
    for c in &artifact.codings {
        insert_observation(
            pool,
            InsertObservationParams {
                source_document_id,
                coding_system: &c.system,
                coding_code: &c.code,
                coding_display: Some(&c.display),
                effective_start,
                effective_end: None,
                value_quantity: c.severity,
                value_string: None,
                value_unit: None,
                derivation: "inferred",
            },
        )
        .await?;
        count += 1;
    }
    Ok(count)
}

/// Text-only replay of an archived journal blob during rebuild. The
/// document date comes from the manifest `subject` (re-applied by the
/// artifact pass when an artifact exists). Returns the new
/// `source_documents.id`.
pub(crate) async fn replay_journal(
    pool: &SqlitePool,
    key: &BlobKey,
    content: &Bytes,
    manifest: &Manifest,
) -> Result<i64> {
    let text = std::str::from_utf8(content).map_err(|_| Error::JournalNotUtf8)?;
    let source_document_id = insert_source_document(
        pool,
        InsertSourceDocumentParams {
            archive_key: key,
            kind: JOURNAL_KIND,
            source: &manifest.source,
            original_filename: manifest.original_filename.as_deref(),
            archived_at: manifest.archived_at,
            document_date: manifest.subject.as_deref(),
        },
    )
    .await?;
    upsert_narrative_text(
        pool,
        UpsertNarrativeTextParams {
            source_document_id,
            title: None,
            text,
        },
    )
    .await?;
    Ok(source_document_id)
}
```

Update `ingestion/mod.rs`: module `//!` docs gain a sentence about journal ingestion, and:

```rust
pub use journal::{
    ingest_journal, JournalIngestOutcome, JOURNAL_EXTRACTION_KIND, JOURNAL_KIND,
};
```

(`NarrativeIngestParams` is already exported.) Also make `narrative.rs`'s `NarrativeIngestParams` usable from `journal.rs` — it is `pub` already; import via `crate::ingestion::narrative::NarrativeIngestParams` requires `mod narrative;` visibility, which exists (sibling module, `pub struct`). If the compiler objects to the path, use `super::narrative::NarrativeIngestParams`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core ingestion`
Expected: PASS — all new journal tests plus every pre-existing ingestion test.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-core/src/ingestion/
git -c user.email=sera@fhwang.net -c user.name="Francis Hwang" commit -m "Journal entry ingestion: verified inference into observations

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: Rebuild replay arms for journal blobs and artifacts

**Files:**
- Modify: `crates/chartpds-core/src/ingestion/rebuild.rs`

**Interfaces:**
- Consumes: `journal::{replay_journal, apply_journal_extraction, JOURNAL_KIND, JOURNAL_EXTRACTION_KIND}`, `JournalExtractionArtifact`.
- Produces: `RebuildResult` gains `pub journals_ingested: u64` and `pub journal_extractions_applied: u64` (serialized into the `index_rebuild` tool output automatically).

- [ ] **Step 1: Write the failing test**

Add to `rebuild.rs` tests:

```rust
    /// Canned journal extractor for rebuild tests: no network.
    struct MockJournalExtractor;
    impl crate::extraction::JournalExtractor for MockJournalExtractor {
        async fn extract_journal(
            &self,
            _text: &str,
            _feedback: Option<&str>,
        ) -> std::result::Result<crate::extraction::RawJournalExtraction, crate::extraction::Error>
        {
            use crate::extraction::{RawJournalCoding, RawJournalExtraction};
            Ok(RawJournalExtraction {
                entry_date: None,
                entry_date_quote: None,
                title: Some("Journal — shoulder ache".to_owned()),
                codings: vec![RawJournalCoding {
                    code: "M25.512".to_owned(),
                    display: "Pain in left shoulder".to_owned(),
                    quote: "Left shoulder aching again after climbing.".to_owned(),
                    severity: None,
                }],
            })
        }
    }

    #[tokio::test]
    async fn rebuild_replays_journal_and_applies_artifact_without_llm() {
        use crate::ingestion::{ingest_journal, NarrativeIngestParams, JOURNAL_KIND};

        let (pool, archive, derived) = fresh_pool_and_stores().await;
        ingest_journal(
            &archive,
            &derived,
            &pool,
            bytes::Bytes::from_static(
                b"# Jul 26, 2026\n\nLeft shoulder aching again after climbing.\n",
            ),
            NarrativeIngestParams {
                source: "journal",
                original_filename: Some("2026-07-26.md"),
                archived_at: time::macros::datetime!(2026-07-26 21:00:00 UTC),
            },
            Some(&MockJournalExtractor),
        )
        .await
        .expect("live ingest");

        // Rebuild must reproduce everything from the two stores alone.
        let result = rebuild_index(&archive, &derived, &pool)
            .await
            .expect("rebuild");
        assert_eq!(result.blobs_found, 2);
        assert_eq!(result.journals_ingested, 1);
        assert_eq!(result.journal_extractions_applied, 1);
        assert_eq!(result.blobs_skipped, 0);

        let doc_row: (i64, Option<String>) =
            sqlx::query_as("SELECT id, document_date FROM source_documents WHERE kind = ?")
                .bind(JOURNAL_KIND)
                .fetch_one(&pool)
                .await
                .expect("doc row");
        assert_eq!(doc_row.1.as_deref(), Some("2026-07-26"));

        let obs = crate::index::list_observations_by_source_document(&pool, doc_row.0)
            .await
            .expect("observations");
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].coding_code, "M25.512");
        assert_eq!(obs[0].derivation, "inferred");

        let fts: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM narrative_texts_fts WHERE narrative_texts_fts MATCH 'climbing'",
        )
        .fetch_one(&pool)
        .await
        .expect("fts");
        assert_eq!(fts.0, 1);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core rebuild`
Expected: COMPILE FAIL — `journals_ingested` not found.

- [ ] **Step 3: Implement**

In `rebuild.rs`:

1. `RebuildResult` gains:

```rust
    /// Journal entries replayed (text re-indexed deterministically).
    pub journals_ingested: u64,
    /// Frozen journal extraction artifacts applied to their entries.
    pub journal_extractions_applied: u64,
```

2. `ReplayTally` gains `journals_ingested: u64` and `journal_artifacts: Vec<(OffsetDateTime, crate::extraction::JournalExtractionArtifact)>`; `BlobOutcome` gains:

```rust
    /// A journal entry's text was replayed.
    Journal,
    /// A frozen journal extraction artifact, deferred to phase two.
    JournalArtifact(OffsetDateTime, crate::extraction::JournalExtractionArtifact),
```

with the corresponding `record` arms.

3. In `replay_blob`'s match, add arms (before the catch-all):

```rust
        journal::JOURNAL_KIND => {
            match journal::replay_journal(pool, key, &content, &manifest).await {
                Ok(_) => Ok(BlobOutcome::Journal),
                Err(Error::JournalNotUtf8) => {
                    tracing::warn!(key = key.as_str(), "skipping non-utf8 journal blob");
                    Ok(BlobOutcome::Skipped)
                }
                Err(err) => Err(err),
            }
        }
        journal::JOURNAL_EXTRACTION_KIND => {
            Ok(parse_journal_artifact(key, &content, &manifest))
        }
```

Add `use super::journal;` next to `use super::narrative;`.

4. In `replay_derived_blob`, generalize the single-kind check to a match over `narrative::NARRATIVE_EXTRACTION_KIND` (existing behavior) and `journal::JOURNAL_EXTRACTION_KIND` (→ `parse_journal_artifact`), keeping the warn+skip fallback.

5. Add the parser (next to `parse_extraction_artifact`, same shape):

```rust
/// Parse a `journal-extraction` blob into its deferred phase-two outcome;
/// malformed JSON is skipped, not fatal.
fn parse_journal_artifact(
    key: &BlobKey,
    content: &bytes::Bytes,
    manifest: &Manifest,
) -> BlobOutcome {
    match serde_json::from_slice::<crate::extraction::JournalExtractionArtifact>(content) {
        Ok(artifact) => BlobOutcome::JournalArtifact(manifest.archived_at, artifact),
        Err(err) => {
            tracing::warn!(key = key.as_str(), %err, "skipping malformed journal artifact");
            BlobOutcome::Skipped
        }
    }
}
```

6. Phase two: after `apply_newest_extraction_artifacts`, add the journal twin (same newest-per-document dedup, calling `journal::apply_journal_extraction`):

```rust
/// Apply the newest journal artifact per referenced entry (a corrective
/// re-ingest can leave multiple artifacts pointing at the same blob).
/// Returns `(journal_extractions_applied, blobs_skipped)`.
async fn apply_newest_journal_artifacts(
    pool: &SqlitePool,
    artifacts: Vec<(OffsetDateTime, crate::extraction::JournalExtractionArtifact)>,
) -> Result<(u64, u64)> {
    let mut newest: std::collections::HashMap<
        String,
        (OffsetDateTime, crate::extraction::JournalExtractionArtifact),
    > = std::collections::HashMap::new();
    for (at, artifact) in artifacts {
        match newest.get(&artifact.document) {
            Some((existing_at, _)) if *existing_at >= at => {}
            _ => {
                newest.insert(artifact.document.clone(), (at, artifact));
            }
        }
    }
    let mut applied = 0u64;
    let mut skipped = 0u64;
    for (_at, artifact) in newest.into_values() {
        let Ok(key) = BlobKey::from_hex_str(&artifact.document) else {
            tracing::warn!(document = %artifact.document, "journal artifact references invalid blob key");
            skipped += 1;
            continue;
        };
        if let Some(doc) = index::fetch_source_document_by_archive_key(pool, &key).await? {
            journal::apply_journal_extraction(pool, doc.id, &artifact).await?;
            applied += 1;
        } else {
            tracing::warn!(document = %artifact.document, "journal artifact references missing document");
            skipped += 1;
        }
    }
    Ok((applied, skipped))
}
```

Wire both into `rebuild_index`'s result (add the journal skips into `blobs_skipped`), and update the `rebuild_index` doc comment to mention journal blobs/artifacts.

- [ ] **Step 4: Run tests to verify they pass**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core rebuild`
Expected: PASS — new test plus all pre-existing rebuild tests.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-core/src/ingestion/rebuild.rs
git -c user.email=sera@fhwang.net -c user.name="Francis Hwang" commit -m "Rebuild replays journal blobs and applies journal artifacts

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: Codings count in the narrative catalog

**Files:**
- Modify: `crates/chartpds-core/src/queries/search_narratives.rs`
- Modify: `.sqlx/` via `just prepare-sql`

**Interfaces:**
- Produces: `NarrativeSearchHit.codings_count: i64` — coded claims (problems + observations) attached to the document; `0` flags "entry produced no structured data".

- [ ] **Step 1: Write the failing test**

Add to `search_narratives.rs` tests:

```rust
    #[tokio::test]
    async fn catalog_reports_codings_count_including_zero() {
        let pool = pool().await;
        let coded = narrative(
            &pool,
            "5555555555555555555555555555555555555555555555555555555555555555",
            Some("2026-07-26"),
            "left shoulder aching after climbing",
        )
        .await;
        let uncoded = narrative(
            &pool,
            "6666666666666666666666666666666666666666666666666666666666666666",
            Some("2026-07-27"),
            "felt great, long run",
        )
        .await;
        crate::index::insert_observation(
            &pool,
            crate::index::InsertObservationParams {
                source_document_id: coded,
                coding_system: "http://hl7.org/fhir/sid/icd-10-cm",
                coding_code: "M25.512",
                coding_display: Some("Pain in left shoulder"),
                effective_start: time::macros::datetime!(2026-07-26 00:00:00 UTC),
                effective_end: None,
                value_quantity: None,
                value_string: None,
                value_unit: None,
                derivation: "inferred",
            },
        )
        .await
        .expect("insert obs");

        let hits = search_narratives(&pool, None, 10).await.expect("list");
        let count_for = |id: i64| {
            hits.iter()
                .find(|h| h.source_document_id == id)
                .expect("hit")
                .codings_count
        };
        assert_eq!(count_for(coded), 1);
        assert_eq!(count_for(uncoded), 0, "zero-coding entries stay visible");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core search_narratives`
Expected: COMPILE FAIL — no field `codings_count`.

- [ ] **Step 3: Implement**

- `NarrativeSearchHit` gains:
  ```rust
      /// Coded claims (problems + observations) attached to this document.
      /// `0` on a journal entry means it produced no structured data and is
      /// reachable only via full-text search.
      pub codings_count: i64,
  ```
- Both query branches gain this select expression (aliased `codings_count`):
  ```sql
  (SELECT COUNT(*) FROM problems p WHERE p.source_document_id = nt.source_document_id)
    + (SELECT COUNT(*) FROM observations o WHERE o.source_document_id = nt.source_document_id)
  ```
  - FTS branch: extend the `query_as` tuple type with a seventh `i64` and map it.
  - Catalog branch: add to the `query!` SELECT as `AS "codings_count!: i64"` and map it.
- Run `just prepare-sql` (the `query!` branch changed).

- [ ] **Step 4: Run tests to verify they pass**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-core search_narratives`
Expected: PASS (all three tests in the file).

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-core/src/queries/search_narratives.rs .sqlx/
git -c user.email=sera@fhwang.net -c user.name="Francis Hwang" commit -m "Narrative catalog reports per-document codings count

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: MCP tool surface + README

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs` (`RecordIngestArgs` docs, `record_ingest` description + branch, unsupported-kind message, tests)
- Modify: `README.md` (tool orientation map)

**Interfaces:**
- Consumes: `chartpds_core::ingestion::{ingest_journal, NarrativeIngestParams}`, `chartpds_core::extraction::ClaudeExtractor` (already implements `JournalExtractor`).
- Produces: `record_ingest` accepts `kind="journal"`; returns the serialized `JournalIngestOutcome`.

- [ ] **Step 1: Write the failing tests**

Add to `server.rs` tests (near the existing `record_ingest` tests, using the same server-construction helper the existing tests use):

```rust
    #[tokio::test]
    async fn record_ingest_journal_without_api_key_fails_actionably() {
        // Only meaningful in a key-less environment (CI). In a dev shell
        // where ANTHROPIC_API_KEY is set this test would build a REAL
        // extractor and hit the network — skip instead.
        if std::env::var("ANTHROPIC_API_KEY").is_ok_and(|k| !k.is_empty()) {
            return;
        }
        let server = test_server().await; // match the existing helper's name
        let err = server
            .record_ingest(Parameters(RecordIngestArgs {
                file_path: None,
                content: Some("# Jul 26, 2026\n\nShoulder aching.".to_owned()),
                kind: "journal".to_owned(),
                source: "journal".to_owned(),
                original_filename: Some("2026-07-26.md".to_owned()),
            }))
            .await
            .expect_err("no extractor configured in tests");
        assert!(err.to_string().contains("ANTHROPIC_API_KEY"));
    }

    #[tokio::test]
    async fn record_ingest_rejects_unknown_kind_naming_all_supported() {
        let server = test_server().await;
        let err = server
            .record_ingest(Parameters(RecordIngestArgs {
                file_path: None,
                content: Some("x".to_owned()),
                kind: "fax".to_owned(),
                source: "test".to_owned(),
                original_filename: None,
            }))
            .await
            .expect_err("unknown kind");
        let msg = err.to_string();
        assert!(msg.contains("ccda") && msg.contains("clinical-pdf") && msg.contains("journal"));
    }
```

(Adapt the helper name to whatever the existing tests use to construct the server — see `record_ingest_returns_source_document_id` around `server.rs:1491`. The env guard at the top of the first test makes it a no-op in key-bearing dev shells; it does its real work in CI, which has no key.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-mcp record_ingest`
Expected: FAIL — unknown kind `"journal"` error on the first test; second test fails on the missing `journal` mention.

- [ ] **Step 3: Implement**

1. `RecordIngestArgs.kind` doc becomes: `/// Document kind: "ccda", "clinical-pdf", or "journal".` and the `content` field doc gains: journal text MAY be passed inline via `content` (it is plain text, unlike PDFs).

2. Add a `"journal"` arm to the `match args.kind.as_str()` (modeled on the `"clinical-pdf"` arm):

```rust
            "journal" => {
                let extractor =
                    chartpds_core::extraction::ClaudeExtractor::from_env(self.http_client.clone());
                let outcome = chartpds_core::ingestion::ingest_journal(
                    &self.archive,
                    &self.derived,
                    &self.pool,
                    content,
                    NarrativeIngestParams {
                        source: &args.source,
                        original_filename: original_filename.as_deref(),
                        archived_at: time::OffsetDateTime::now_utc(),
                    },
                    extractor.as_ref(),
                )
                .await
                .map_err(|err| {
                    McpError::internal_error(format!("ingestion failed: {err}"), None)
                })?;
                let json = serde_json::to_string(&outcome)
                    .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
                Ok(CallToolResult::success(vec![Content::text(json)]))
            }
```

3. Update the unsupported-kind message to `"unsupported kind {other:?}; supported: \"ccda\", \"clinical-pdf\", \"journal\""`.

4. Update the `record_ingest` `#[tool(description = ...)]` — these strings are the canonical tool docs. Append after the clinical-pdf sentence:

> kind="journal": one personal health-journal entry (markdown/plain text, UTF-8, passable inline via content) written in free colloquial language — archives the text, indexes it for narrative_search, and maps described symptoms/complaints to coarse ICD-10-CM codes as observations rows with derivation "inferred" (severity captured as value_quantity only when the author stated a number; an entry with nothing to code succeeds with zero codings). One entry per file; the entry date comes from a YYYY-MM-DD in the filename or a dated markdown header (a year must appear somewhere), falling back to a verified date in the text. Returns the verified codings WITH their grounding quotes — echo each code+quote pair back to the user so the author can catch mis-mappings, since these codes are LLM-inferred, not quoted from a clinician.

5. Check the `observation_history`/`observation_latest`/`index_rebuild` tool descriptions: if they enumerate returned fields, add `derivation` (and the two new rebuild counters) to the enumeration; if they don't enumerate, leave them.

6. README: in the grouped tool-surface orientation map, update the `record_ingest` line to mention journal entries alongside CCDA/clinical-PDF ingestion. No configuration-table change (no new env vars).

- [ ] **Step 4: Run tests to verify they pass**

Run: `env -u RUSTUP_TOOLCHAIN cargo test -p chartpds-mcp`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-mcp/src/server.rs README.md
git -c user.email=sera@fhwang.net -c user.name="Francis Hwang" commit -m "record_ingest kind=journal: MCP surface and README

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 10: Docs routing + full verification

**Files:**
- Modify: `CLAUDE.md` (one line in "Where things are documented")
- Verify: whole workspace

- [ ] **Step 1: Update the CLAUDE.md routing line**

In CLAUDE.md's "Ingestion and the narrative/LLM pipeline" bullet, add `ingestion/journal.rs` and `extraction/journal.rs` to the file list (routing only — the real docs live in those files' module headers, which Tasks 3–6 wrote).

- [ ] **Step 2: Run the full check**

```bash
env -u RUSTUP_TOOLCHAIN just check
```

Expected: PASS end-to-end — fmt, clippy `-D warnings` (including `missing_docs` on every new pub item), typecheck, tests (workspace, including holdout), `cargo sqlx prepare --check`, `cargo deny`, `cargo machete`, `holdout-verify`.

Failure triage:
- `holdout-verify` fails → you touched a protected path; revert it, never regenerate the lock.
- A holdout test fails → STOP; report it; fix code in `crates/**` only.
- `prepare --check` fails → re-run `just prepare-sql`, amend the offending commit's `.sqlx/`.
- fmt → `just fmt` (or `cargo fmt --all`).

- [ ] **Step 3: Commit and verify a clean tree**

```bash
git add CLAUDE.md
git -c user.email=sera@fhwang.net -c user.name="Francis Hwang" commit -m "Route journal module docs from CLAUDE.md

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git status  # must be clean
git log --oneline main..HEAD
```

Then use the superpowers:finishing-a-development-branch skill to decide integration (PR etc.). PR body must not include personal health specifics; end it with the standard Claude Code attribution footer.

---

## Out of scope (deliberately — mirrors the spec's deferrals)

- Amendment/correction records; re-extraction command; audit sweeps of (quote, code) pairs.
- Multi-entry files; severity inferred from words; bucket-level derivation rollups in `observation_table`.
- Exposing artifact quotes through `narrative_get` (journal entries are short; the full text is the drill-down, and quotes are echoed at ingest).
- `derivation` on `current_problems`/`aligned_table` outputs.
- An explicit `"journal"` arm in `day_confidence` source matching (the `_ => Confirmed` default is correct: a submitted entry does not accrete).
