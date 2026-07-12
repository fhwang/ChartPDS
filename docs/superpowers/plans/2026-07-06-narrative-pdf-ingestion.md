# Narrative PDF Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ingest clinical narrative PDFs (e.g. pathology reports printed from a provider portal) into ChartPDS: archived as content-addressed blobs, full-text searchable via SQLite FTS5, with explicitly-quoted ICD-10 codes extracted by an in-process Claude API call, mechanically verified, frozen as an archived artifact, and indexed into `problems`.

**Architecture:** Two blobs per narrative — the raw PDF (`clinical-pdf`) and a JSON extraction artifact (`narrative-extraction`) that references the PDF's content hash. The LLM runs once at ingest; `rebuild_index` replays the frozen artifact and never calls a model. Text extraction (pure Rust) feeds a `narrative_texts` table + FTS5 index. New MCP read tools `search_narratives` / `get_narrative`; the existing `ingest_record` tool grows a `clinical-pdf` kind.

**Tech Stack:** Rust (stable, workspace pinned), sqlx offline mode + SQLite FTS5 (already compiled into the bundled libsqlite3-sys 0.30.1 — verified), `pdf-extract` 0.12 for PDF text, `reqwest` (already a workspace dep) for the Claude API, `rmcp` MCP server.

**Spec:** `docs/superpowers/specs/2026-07-06-narrative-pdf-ingestion-design.md`

## Global Constraints

- Run `just check` before declaring any task complete (fmt, clippy `-D warnings`, tests, cargo deny, machete, sqlx prepare check, holdout).
- After ANY change to `crates/chartpds-core/migrations/*.sql` or any `sqlx::query!`/`query_as!` invocation: run `just prepare-sql` and commit the `.sqlx/` cache updates in the same commit.
- Never bypass a lint. No `#[allow(...)]` without `reason = "..."`. Every `pub` item needs a doc comment (missing_docs is promoted to error by `-D warnings`).
- Default visibility inside `chartpds-core` is `pub(crate)`; only items the binary needs go through `lib.rs` re-exports as `pub`.
- **Never edit anything under `holdout/`, `holdout.lock`, `.github/allowed_signers`, `.github/workflows/holdout.yml`.** If a holdout test fails, stop and report; fix code in `crates/**` only.
- **Public repo: fixtures must be synthetic.** Never commit the user's real medical PDF or any text from it. The fixture uses invented patient/provider names and generic codes.
- Migrations are forward-only. No down migrations.
- Claude API: model pinned to `claude-opus-4-8`; `ANTHROPIC_API_KEY` from env; no sampling params, no `thinking` param (omitted = no thinking on Opus 4.8), structured output via `output_config.format` json_schema.
- Commit at the end of every task with a descriptive message ending in `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: Migration 0013 — `narrative_texts` + FTS5 + `problems.section_label`, and the `narrative_texts` index module

This task proves the load-bearing assumption (FTS5 works through sqlx offline mode) before anything is built on it.

**Files:**
- Create: `crates/chartpds-core/migrations/0013_narrative_texts.sql`
- Create: `crates/chartpds-core/src/index/narrative_texts.rs`
- Modify: `crates/chartpds-core/src/index/mod.rs` (add `mod narrative_texts;` + re-exports)

**Interfaces:**
- Consumes: existing `source_documents` table, `open_pool`, `insert_source_document`.
- Produces (used by Tasks 6–8):
  - `index::upsert_narrative_text(pool, UpsertNarrativeTextParams{source_document_id: i64, title: Option<&str>, text: &str}) -> Result<(), sqlx::Error>`
  - `index::get_narrative_text(pool, source_document_id: i64) -> Result<Option<NarrativeText>, sqlx::Error>` where `NarrativeText{source_document_id: i64, title: Option<String>, text: String}`
  - `index::set_narrative_title(pool, source_document_id: i64, title: &str) -> Result<(), sqlx::Error>`
  - The `narrative_texts_fts` FTS5 table, kept in sync by triggers (queried directly by Task 8).

- [ ] **Step 1: Write the migration**

`crates/chartpds-core/migrations/0013_narrative_texts.sql`:

```sql
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
```

- [ ] **Step 2: Write the failing tests + module**

`crates/chartpds-core/src/index/narrative_texts.rs`:

```rust
//! `narrative_texts` table: extracted plain text of narrative documents.
//!
//! One row per narrative `source_documents` row. A parallel FTS5 table
//! (`narrative_texts_fts`) is kept in sync by SQL triggers declared in the
//! migration — inserts, updates, and cascade deletes all propagate without
//! write-path code here.

use sqlx::SqlitePool;

/// A row from the `narrative_texts` table.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NarrativeText {
    /// Foreign key into `source_documents` (also the primary key here).
    pub source_document_id: i64,
    /// Short human-readable label from the extraction artifact, if any.
    pub title: Option<String>,
    /// Full extracted document text.
    pub text: String,
}

/// Parameters for [`upsert`].
pub struct UpsertParams<'a> {
    /// Foreign key into `source_documents`.
    pub source_document_id: i64,
    /// Optional title.
    pub title: Option<&'a str>,
    /// Full extracted text.
    pub text: &'a str,
}

/// Insert or replace the narrative text for a source document.
///
/// # Errors
///
/// Returns `sqlx::Error` if the statement fails (typically a foreign-key
/// violation on `source_document_id`).
pub async fn upsert(pool: &SqlitePool, params: UpsertParams<'_>) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO narrative_texts (source_document_id, title, text)
        VALUES (?, ?, ?)
        ON CONFLICT(source_document_id) DO UPDATE SET
            title = excluded.title,
            text = excluded.text
        "#,
        params.source_document_id,
        params.title,
        params.text,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Fetch the narrative text for a source document, if present.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn get_by_source_document(
    pool: &SqlitePool,
    source_document_id: i64,
) -> Result<Option<NarrativeText>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT source_document_id AS "source_document_id!: i64", title, text
        FROM narrative_texts
        WHERE source_document_id = ?
        "#,
        source_document_id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| NarrativeText {
        source_document_id: r.source_document_id,
        title: r.title,
        text: r.text,
    }))
}

/// Set the title on an existing narrative text row.
///
/// # Errors
///
/// Returns `sqlx::Error` if the statement fails.
pub async fn set_title(
    pool: &SqlitePool,
    source_document_id: i64,
    title: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "UPDATE narrative_texts SET title = ? WHERE source_document_id = ?",
        title,
        source_document_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::BlobKey;
    use crate::index::{insert_source_document, open_pool, InsertSourceDocumentParams};
    use time::OffsetDateTime;

    async fn fresh_pool() -> SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    async fn doc(pool: &SqlitePool, hex: &str) -> i64 {
        let key = BlobKey::from_hex_str(hex).expect("key");
        insert_source_document(
            pool,
            InsertSourceDocumentParams {
                archive_key: &key,
                kind: "clinical-pdf",
                source: "test",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
                document_date: None,
            },
        )
        .await
        .expect("doc")
    }

    async fn fts_match_count(pool: &SqlitePool, query: &str) -> i64 {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM narrative_texts_fts WHERE narrative_texts_fts MATCH ?",
        )
        .bind(query)
        .fetch_one(pool)
        .await
        .expect("fts query");
        row.0
    }

    #[tokio::test]
    async fn upsert_and_get_round_trips() {
        let pool = fresh_pool().await;
        let id = doc(
            &pool,
            "1111111111111111111111111111111111111111111111111111111111111111",
        )
        .await;
        upsert(
            &pool,
            UpsertParams {
                source_document_id: id,
                title: None,
                text: "RECTAL MUCOSA SHOWING NO SIGNIFICANT FINDINGS",
            },
        )
        .await
        .expect("upsert");

        let row = get_by_source_document(&pool, id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(row.title, None);
        assert!(row.text.contains("RECTAL MUCOSA"));

        set_title(&pool, id, "GI Pathology Report").await.expect("set title");
        let row = get_by_source_document(&pool, id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(row.title.as_deref(), Some("GI Pathology Report"));
    }

    #[tokio::test]
    async fn fts_index_matches_inserted_text() {
        let pool = fresh_pool().await;
        let id = doc(
            &pool,
            "2222222222222222222222222222222222222222222222222222222222222222",
        )
        .await;
        upsert(
            &pool,
            UpsertParams {
                source_document_id: id,
                title: None,
                text: "BIOPSY TAKEN TO RULE OUT PROCTITIS",
            },
        )
        .await
        .expect("upsert");

        assert_eq!(fts_match_count(&pool, "proctitis").await, 1);
        assert_eq!(fts_match_count(&pool, "cardiology").await, 0);
    }

    #[tokio::test]
    async fn fts_index_follows_update_and_cascade_delete() {
        let pool = fresh_pool().await;
        let id = doc(
            &pool,
            "3333333333333333333333333333333333333333333333333333333333333333",
        )
        .await;
        upsert(
            &pool,
            UpsertParams {
                source_document_id: id,
                title: None,
                text: "initial proctitis text",
            },
        )
        .await
        .expect("upsert");

        // Update replaces the indexed text.
        upsert(
            &pool,
            UpsertParams {
                source_document_id: id,
                title: None,
                text: "replacement colitis text",
            },
        )
        .await
        .expect("re-upsert");
        assert_eq!(fts_match_count(&pool, "proctitis").await, 0);
        assert_eq!(fts_match_count(&pool, "colitis").await, 1);

        // Deleting the parent source_documents row must cascade through
        // narrative_texts AND the FTS index (delete trigger fires on cascade).
        sqlx::query("DELETE FROM source_documents WHERE id = ?")
            .bind(id)
            .execute(&pool)
            .await
            .expect("delete doc");
        assert_eq!(fts_match_count(&pool, "colitis").await, 0);
    }
}
```

Add to `crates/chartpds-core/src/index/mod.rs` (alphabetical position among mods; re-exports in the existing style):

```rust
mod narrative_texts;
```

```rust
pub use narrative_texts::{
    get_by_source_document as get_narrative_text, set_title as set_narrative_title,
    upsert as upsert_narrative_text, NarrativeText,
    UpsertParams as UpsertNarrativeTextParams,
};
```

- [ ] **Step 3: Rebuild the sqlx offline cache**

Run: `just prepare-sql`
Expected: succeeds, new `query-*.json` files appear in `.sqlx/`. **If sqlx errors on any query touching the FTS5 virtual table**, convert only that query from the `sqlx::query!` macro to runtime `sqlx::query`/`sqlx::query_as` with `.bind(...)` (no offline cache entry needed) and add a comment: `// runtime query: sqlx cannot prepare against the FTS5 virtual table`. Do not weaken the non-FTS queries.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p chartpds-core narrative_texts`
Expected: 3 tests PASS. The `fts_index_follows_update_and_cascade_delete` test is the FTS5-availability and trigger-correctness gate. **If the cascade-delete assertion fails** (delete trigger not firing on FK cascade), change `clear_ingested_data` (`index/clear.rs`) and the re-ingest path (Task 6) to `DELETE FROM narrative_texts WHERE source_document_id = ?` explicitly *before* deleting the parent row, and update this test to match.

- [ ] **Step 5: Run the full gate and commit**

Run: `just check`
Expected: PASS.

```bash
git add crates/chartpds-core/migrations/0013_narrative_texts.sql \
        crates/chartpds-core/src/index/narrative_texts.rs \
        crates/chartpds-core/src/index/mod.rs .sqlx/
git commit -m "Add narrative_texts table with FTS5 index and problems.section_label"
```

---

### Task 2: Thread `section_label` through problems insert and `current_problems`

**Files:**
- Modify: `crates/chartpds-core/src/index/problems.rs`
- Modify: `crates/chartpds-core/src/ingestion/ingest.rs` (CCDA caller passes `None`)
- Modify: `crates/chartpds-core/src/queries/current_problems.rs`

**Interfaces:**
- Consumes: `problems.section_label` column from Task 1.
- Produces (used by Tasks 6, 8, 9):
  - `index::InsertProblemParams` gains `pub section_label: Option<&'a str>`
  - `index::Problem` gains `pub section_label: Option<String>`
  - `queries::CurrentProblem` gains `pub section_labels: Vec<String>` (distinct non-null labels across all documents mentioning the code; empty for CCDA-only codes)

- [ ] **Step 1: Extend `index/problems.rs`**

Add to the `Problem` struct (after `onset_date`):

```rust
    /// Verbatim section heading the coding appeared under in a narrative
    /// document (e.g. `"Pre-Op Diagnosis/Indications"`). `None` for CCDA rows.
    pub section_label: Option<String>,
```

Add to `InsertParams` (after `onset_date`):

```rust
    /// Verbatim narrative section heading, if any.
    pub section_label: Option<&'a str>,
```

Update `insert` to include the column:

```rust
    let row = sqlx::query!(
        r#"
        INSERT INTO problems (
            source_document_id, coding_system, coding_code, coding_display,
            status, onset_date, section_label
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        RETURNING id AS "id!: i64"
        "#,
        params.source_document_id,
        params.coding_system,
        params.coding_code,
        params.coding_display,
        params.status,
        params.onset_date,
        params.section_label,
    )
```

Update `list_by_source_document` to select and map it:

```rust
    let rows = sqlx::query!(
        r#"
        SELECT id AS "id!: i64", source_document_id AS "source_document_id!: i64",
               coding_system, coding_code, coding_display,
               status, onset_date, section_label
        FROM problems
        WHERE source_document_id = ?
        ORDER BY onset_date
        "#,
        source_document_id,
    )
```

and in the `.map(...)` add `section_label: r.section_label,`.

In the existing test `insert_and_list_for_source_document_round_trips`, add `section_label: None,` to the `InsertParams` literal and assert `assert_eq!(rows[0].section_label, None);`. Add a second test:

```rust
    #[tokio::test]
    async fn section_label_round_trips() {
        let (pool, doc_id) = fresh_pool_with_doc().await;
        insert(
            &pool,
            InsertParams {
                source_document_id: doc_id,
                coding_system: "http://hl7.org/fhir/sid/icd-10-cm",
                coding_code: "R10.9",
                coding_display: Some("Abdominal pain, unspecified"),
                status: "unknown",
                onset_date: Some("2026-04-21"),
                section_label: Some("Pre-Op Diagnosis/Indications"),
            },
        )
        .await
        .expect("insert problem");
        let rows = list_by_source_document(&pool, doc_id).await.expect("list");
        assert_eq!(
            rows[0].section_label.as_deref(),
            Some("Pre-Op Diagnosis/Indications")
        );
    }
```

- [ ] **Step 2: Fix the CCDA caller**

In `crates/chartpds-core/src/ingestion/ingest.rs`, the `insert_problem` call gains `section_label: None,` after `onset_date`.

Also fix every other `InsertProblemParams` literal the compiler flags (test code in `queries/current_problems.rs` etc.) by adding `section_label: None,`.

- [ ] **Step 3: Surface labels in `current_problems`**

In `crates/chartpds-core/src/queries/current_problems.rs`:

Add to `CurrentProblem` (after `last_seen`):

```rust
    /// Distinct verbatim section headings this code appeared under in
    /// narrative documents (e.g. `["Pre-Op Diagnosis/Indications"]`).
    /// Empty when the code only comes from CCDA problem sections.
    pub section_labels: Vec<String>,
```

In `current_problems`, after the main `rows` query, fetch labels with a second query and merge:

```rust
    let label_rows = sqlx::query!(
        r#"
        SELECT DISTINCT coding_system AS "coding_system!", coding_code AS "coding_code!",
               section_label AS "section_label!"
        FROM problems
        WHERE section_label IS NOT NULL
        ORDER BY coding_system, coding_code, section_label
        "#,
    )
    .fetch_all(pool)
    .await?;
    let mut labels: std::collections::HashMap<(String, String), Vec<String>> =
        std::collections::HashMap::new();
    for r in label_rows {
        labels
            .entry((r.coding_system, r.coding_code))
            .or_default()
            .push(r.section_label);
    }
```

and in the `items` map closure, compute the labels **before** the struct literal (the literal's first fields move `r.coding_system`/`r.coding_code`, so the lookup must happen first):

```rust
    let items = rows
        .into_iter()
        .map(|r| {
            let section_labels = labels
                .remove(&(r.coding_system.clone(), r.coding_code.clone()))
                .unwrap_or_default();
            CurrentProblem {
                coding_system: r.coding_system,
                coding_code: r.coding_code,
                coding_display: r.coding_display,
                status: r.status,
                onset_date: r.onset_date,
                document_count: r.document_count,
                first_seen: r.first_seen,
                last_seen: r.last_seen,
                section_labels,
            }
        })
        .collect();
```

Add a test:

```rust
    #[tokio::test]
    async fn section_labels_are_collected_distinct() {
        let pool = pool().await;
        let d = doc(
            &pool,
            "4444444444444444444444444444444444444444444444444444444444444444",
            "2026-04-21",
        )
        .await;
        for label in ["Pre-Op Diagnosis/Indications", "Post-Op Diagnosis/ICD Codes"] {
            insert_problem(
                &pool,
                InsertProblemParams {
                    source_document_id: d,
                    coding_system: "http://hl7.org/fhir/sid/icd-10-cm",
                    coding_code: "R10.9",
                    coding_display: Some("Abdominal pain, unspecified"),
                    status: "unknown",
                    onset_date: Some("2026-04-21"),
                    section_label: Some(label),
                },
            )
            .await
            .expect("problem");
        }
        let result = current_problems(&pool).await.expect("query");
        assert_eq!(result.items.len(), 1);
        assert_eq!(
            result.items[0].section_labels,
            vec![
                "Post-Op Diagnosis/ICD Codes".to_string(),
                "Pre-Op Diagnosis/Indications".to_string()
            ]
        );
    }
```

(Existing tests in this file gain `section_label: None,` in their `InsertProblemParams` and, where they construct expectations, nothing else changes — `section_labels` will be empty.)

- [ ] **Step 4: prepare-sql, test, commit**

Run: `just prepare-sql && cargo test -p chartpds-core problems`
Expected: PASS (both `index::problems` and `queries::current_problems` tests).

Run: `just check`
Expected: PASS. If the holdout suite asserts on `list_problems` JSON shape, the new `section_labels` field is additive; a holdout failure here means something else broke — stop and report.

```bash
git add -A crates/chartpds-core/src .sqlx/
git commit -m "Thread section_label through problems and surface in current_problems"
```

---

### Task 3: `extraction::pdf` — deterministic PDF text extraction + synthetic fixture

**Files:**
- Modify: `crates/chartpds-core/Cargo.toml` (add `pdf-extract`)
- Create: `crates/chartpds-core/src/extraction/mod.rs`
- Create: `crates/chartpds-core/src/extraction/error.rs`
- Create: `crates/chartpds-core/src/extraction/pdf.rs`
- Create: `crates/chartpds-core/src/extraction/fixtures/synthetic_pathology.pdf` (generated, binary)
- Modify: `crates/chartpds-core/src/lib.rs` (add `pub mod extraction;` + module-tree doc line)

**Interfaces:**
- Produces (used by Tasks 4–7, 9):
  - `extraction::Error` — `NoTextLayer`, `Pdf{reason}`, `Api{reason}`, `InvalidResponse{reason}`
  - `extraction::extract_pdf_text(bytes: &[u8]) -> Result<String, extraction::Error>`
  - Test fixture constant pattern: `include_bytes!("fixtures/synthetic_pathology.pdf")`

- [ ] **Step 1: Generate the synthetic fixture PDF**

The fixture is a pathology-report lookalike with invented data. Generate it with a throwaway Python venv (fpdf2), then copy it into the repo:

```bash
cd /tmp && python3 -m venv fixturegen && ./fixturegen/bin/pip install -q fpdf2
./fixturegen/bin/python - <<'EOF'
from fpdf import FPDF
LINES = [
    "FINAL RESULT",
    "DOE, JANE  DOB: 01/02/1980  Acc No. 55555",
    "Example Medical Group",
    "Order Date: 04/21/2026",
    "Collection Date: 04/21/2026 00:00:00",
    "BIOPSY, GI (1 JAR)",
    ".. GI PATHOLOGY REPORT ..",
    "SPECIMEN SOURCE: Colon",
    "CLINICAL DATA: Procedure: Colonoscopy",
    "Pre-Op Diagnosis/Indications: Abdominal pain, unspecified - R10.9",
    "Encounter for screening for malignant neoplasm of colon - Z12.11",
    "Post-Op Diagnosis/ICD Codes: Other hemorrhoids - K64.8.",
    "GROSS:",
    "  A. ONE PIECE OF SOFT TAN TISSUE MEASURING 0.4 X 0.2 X 0.1 CM.",
    "DIAGNOSIS:",
    "  A. - COLONIC MUCOSA SHOWING NO SIGNIFICANT HISTOPATHOLOGIC FINDINGS.",
    "     - NEGATIVE FOR DYSPLASIA OR MALIGNANCY.",
    "FINAL REPORT",
]
pdf = FPDF()
pdf.add_page()
pdf.set_font("Helvetica", size=10)
for line in LINES:
    pdf.cell(0, 6, line, new_x="LMARGIN", new_y="NEXT")
pdf.output("synthetic_pathology.pdf")
EOF
mkdir -p ~/Code/ChartPDS/crates/chartpds-core/src/extraction/fixtures
cp /tmp/synthetic_pathology.pdf ~/Code/ChartPDS/crates/chartpds-core/src/extraction/fixtures/
```

- [ ] **Step 2: Add the dependency**

In `crates/chartpds-core/Cargo.toml` under `[dependencies]` (it is crate-specific, not shared — goes in the crate manifest per CLAUDE.md):

```toml
pdf-extract = "0.12"
```

- [ ] **Step 3: Write error type, module skeleton, and failing test**

`crates/chartpds-core/src/extraction/error.rs`:

```rust
//! Extraction error types.

use thiserror::Error;

/// Errors from PDF text extraction or the LLM extraction call.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The PDF parsed but produced no extractable text (likely a scan).
    #[error("PDF has no extractable text layer; OCR is unsupported")]
    NoTextLayer,

    /// The bytes could not be parsed as a PDF.
    #[error("failed to parse PDF: {reason}")]
    Pdf {
        /// Human-readable parse failure description.
        reason: String,
    },

    /// The extraction API request failed (network, auth, refusal, ...).
    #[error("extraction API request failed: {reason}")]
    Api {
        /// Human-readable failure description.
        reason: String,
    },

    /// The extraction API responded, but the payload was not usable.
    #[error("extraction response invalid: {reason}")]
    InvalidResponse {
        /// Human-readable description of the malformation.
        reason: String,
    },
}
```

`crates/chartpds-core/src/extraction/pdf.rs`:

```rust
//! Deterministic PDF text extraction (no LLM, no network).

use super::error::Error;

/// Extract the embedded text layer from PDF bytes.
///
/// Purely mechanical: the same bytes always produce the same text, so
/// `rebuild_index` can re-derive it from the archive alone.
///
/// # Errors
///
/// Returns [`Error::Pdf`] if the bytes are not parseable as a PDF, and
/// [`Error::NoTextLayer`] if parsing succeeds but yields no text (a scanned
/// image; OCR is out of scope).
pub fn extract_pdf_text(bytes: &[u8]) -> Result<String, Error> {
    let text = pdf_extract::extract_text_from_mem(bytes).map_err(|err| Error::Pdf {
        reason: err.to_string(),
    })?;
    if text.chars().all(char::is_whitespace) {
        return Err(Error::NoTextLayer);
    }
    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &[u8] = include_bytes!("fixtures/synthetic_pathology.pdf");

    #[test]
    fn extracts_text_with_codes_from_fixture() {
        let text = extract_pdf_text(FIXTURE).expect("extract");
        for needle in ["R10.9", "Z12.11", "K64.8", "DIAGNOSIS", "04/21/2026"] {
            assert!(text.contains(needle), "missing {needle:?} in extracted text");
        }
    }

    #[test]
    fn non_pdf_bytes_return_pdf_error() {
        let err = extract_pdf_text(b"not a pdf").expect_err("should fail");
        assert!(matches!(err, Error::Pdf { .. }));
    }
}
```

`crates/chartpds-core/src/extraction/mod.rs`:

```rust
//! Narrative-document extraction: deterministic PDF text + one-time,
//! mechanically verified LLM extraction of explicitly quoted codings.
//!
//! Design invariant: the LLM runs once, at ingest. Its verified output is
//! frozen as an archived artifact; `rebuild_index` replays the artifact and
//! never calls a model.

mod error;
mod pdf;

pub use error::Error;
pub use pdf::extract_pdf_text;
```

In `crates/chartpds-core/src/lib.rs`, add `pub mod extraction;` (alphabetical, after `clinical`) and a module-tree doc line: `//! - [\`extraction\`] turns narrative PDFs into text + verified codings.`

- [ ] **Step 4: Run tests**

Run: `cargo test -p chartpds-core extraction::pdf`
Expected: 2 tests PASS. If `extracts_text_with_codes_from_fixture` fails on a needle, inspect what `pdf_extract` produced from the fpdf2 fixture (`cargo test ... -- --nocapture` with a debug print) and adjust the fixture generation (not the assertion semantics).

- [ ] **Step 5: Full gate (deny/machete validate the new dep) and commit**

Run: `just check`
Expected: PASS. If `cargo deny` rejects a transitive license from `pdf-extract`, STOP and report — do not edit `deny.toml` without verifying the license is actually acceptable.

```bash
git add crates/chartpds-core/Cargo.toml Cargo.lock crates/chartpds-core/src/extraction crates/chartpds-core/src/lib.rs
git commit -m "Add extraction module with deterministic PDF text extraction"
```

---

### Task 4: `extraction::artifact` + `extraction::verify` — artifact types and mechanical verification

**Files:**
- Create: `crates/chartpds-core/src/extraction/artifact.rs`
- Create: `crates/chartpds-core/src/extraction/verify.rs`
- Modify: `crates/chartpds-core/src/extraction/mod.rs` (declare + re-export)

**Interfaces:**
- Consumes: nothing new.
- Produces (used by Tasks 5–7):
  - `extraction::ICD10_CM_SYSTEM: &str`
  - `extraction::ExtractionArtifact { document: String, document_date: Option<String>, document_date_quote: Option<String>, title: Option<String>, codings: Vec<ExtractedCoding>, extractor: ExtractorInfo, extracted_at: OffsetDateTime }` (Serialize + Deserialize)
  - `extraction::ExtractedCoding { system: String, code: String, display: String, quote: String, section_label: Option<String> }`
  - `extraction::ExtractorInfo { model: String, prompt_version: u32 }`
  - `extraction::RawExtraction { document_date: Option<String>, document_date_quote: Option<String>, title: Option<String>, codings: Vec<RawCoding> }` (Deserialize + Clone — the pre-verification LLM output shape)
  - `extraction::RawCoding { code: String, display: String, quote: String, section_label: Option<String> }`
  - `extraction::VerifiedExtraction { document_date: Option<String>, document_date_quote: Option<String>, title: Option<String>, codings: Vec<ExtractedCoding>, rejected: Vec<String> }`
  - `extraction::verify_extraction(text: &str, raw: RawExtraction) -> VerifiedExtraction` (pure)

- [ ] **Step 1: Write `artifact.rs`**

```rust
//! The frozen extraction artifact: verified LLM output, archived as JSON.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Coding system URI for ICD-10-CM — the only system v1 extracts.
pub const ICD10_CM_SYSTEM: &str = "http://hl7.org/fhir/sid/icd-10-cm";

/// One verified coding: a code the document provably quotes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtractedCoding {
    /// Coding system URI (always [`ICD10_CM_SYSTEM`] in v1).
    pub system: String,
    /// The code exactly as written in the document.
    pub code: String,
    /// The diagnosis text the document pairs with the code.
    pub display: String,
    /// Verbatim text span containing the code; verified as a substring of
    /// the extracted document text (whitespace-normalized).
    pub quote: String,
    /// Verbatim section heading the quote appeared under, if any.
    pub section_label: Option<String>,
}

/// Identity of the extractor that produced an artifact, for auditability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtractorInfo {
    /// Claude model id (e.g. `"claude-opus-4-8"`).
    pub model: String,
    /// Version of the extraction prompt.
    pub prompt_version: u32,
}

/// The archived extraction artifact for one narrative PDF.
///
/// Contains only claims that passed mechanical verification against the
/// document text. Replayed verbatim on rebuild — never regenerated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtractionArtifact {
    /// SHA-256 hex of the PDF blob this artifact describes.
    pub document: String,
    /// The document's calendar date (ISO-8601), if verified.
    pub document_date: Option<String>,
    /// Verbatim text span supporting `document_date`.
    pub document_date_quote: Option<String>,
    /// Short human-readable label (extractor-authored, not verified).
    pub title: Option<String>,
    /// Verified codings.
    pub codings: Vec<ExtractedCoding>,
    /// Who produced this artifact.
    pub extractor: ExtractorInfo,
    /// When extraction ran (RFC 3339).
    #[serde(with = "time::serde::rfc3339")]
    pub extracted_at: OffsetDateTime,
}

/// Un-verified LLM output, as parsed from the structured-output response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RawExtraction {
    /// Claimed document date (ISO-8601), if any.
    pub document_date: Option<String>,
    /// Claimed verbatim span containing the date.
    pub document_date_quote: Option<String>,
    /// Proposed title.
    pub title: Option<String>,
    /// Proposed codings, pre-verification.
    pub codings: Vec<RawCoding>,
}

/// One un-verified proposed coding.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RawCoding {
    /// Proposed code.
    pub code: String,
    /// Proposed display text.
    pub display: String,
    /// Claimed verbatim span containing the code.
    pub quote: String,
    /// Claimed section heading.
    pub section_label: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn artifact_round_trips_through_json() {
        let a = ExtractionArtifact {
            document: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                .to_owned(),
            document_date: Some("2026-04-21".to_owned()),
            document_date_quote: Some("Order Date: 04/21/2026".to_owned()),
            title: Some("GI Pathology Report".to_owned()),
            codings: vec![ExtractedCoding {
                system: ICD10_CM_SYSTEM.to_owned(),
                code: "R10.9".to_owned(),
                display: "Abdominal pain, unspecified".to_owned(),
                quote: "Abdominal pain, unspecified - R10.9".to_owned(),
                section_label: Some("Pre-Op Diagnosis/Indications".to_owned()),
            }],
            extractor: ExtractorInfo {
                model: "claude-opus-4-8".to_owned(),
                prompt_version: 1,
            },
            extracted_at: datetime!(2026-07-06 12:00:00 UTC),
        };
        let bytes = serde_json::to_vec(&a).expect("serialize");
        let back: ExtractionArtifact = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(a, back);
    }
}
```

- [ ] **Step 2: Write `verify.rs`**

```rust
//! Mechanical verification of LLM extraction claims against document text.
//!
//! Pure functions, no async, no database, no network. An LLM claim only
//! reaches the archived artifact if it is provable against the text: every
//! quote must be a substring (whitespace-normalized — portal PDFs contain
//! non-breaking spaces), a coding's code must appear inside its quote, and
//! a claimed date must appear literally inside its quote.

use super::artifact::{ExtractedCoding, RawExtraction, ICD10_CM_SYSTEM};

/// Extraction output that survived verification, plus what was dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedExtraction {
    /// Verified document date (ISO-8601).
    pub document_date: Option<String>,
    /// The verbatim span supporting the date.
    pub document_date_quote: Option<String>,
    /// Title (passed through unverified — presentational only).
    pub title: Option<String>,
    /// Codings whose quote and code both verified.
    pub codings: Vec<ExtractedCoding>,
    /// Human-readable reasons for every dropped claim.
    pub rejected: Vec<String>,
}

/// Collapse every run of Unicode whitespace (including NBSP) to one space.
pub(crate) fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Render the plausible in-document spellings of an ISO date `YYYY-MM-DD`.
fn date_candidates(iso: &str) -> Option<Vec<String>> {
    let mut parts = iso.splitn(3, '-');
    let year: i32 = parts.next()?.parse().ok()?;
    let month: u32 = parts.next()?.parse().ok()?;
    let day: u32 = parts.next()?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    const MONTHS: [&str; 12] = [
        "January", "February", "March", "April", "May", "June", "July", "August",
        "September", "October", "November", "December",
    ];
    let name = MONTHS[(month - 1) as usize];
    let abbr = &name[..3];
    Some(vec![
        iso.to_owned(),
        format!("{month:02}/{day:02}/{year}"),
        format!("{month}/{day}/{year}"),
        format!("{name} {day}, {year}"),
        format!("{abbr} {day}, {year}"),
        format!("{day} {name} {year}"),
    ])
}

/// Verify raw LLM output against the extracted document text.
///
/// Claims that fail verification are dropped and reported in
/// [`VerifiedExtraction::rejected`]; they never reach the artifact.
#[must_use]
pub fn verify_extraction(text: &str, raw: RawExtraction) -> VerifiedExtraction {
    let norm_text = normalize_ws(text);
    let mut rejected = Vec::new();

    let mut codings = Vec::new();
    for c in raw.codings {
        let norm_quote = normalize_ws(&c.quote);
        if norm_quote.is_empty() {
            rejected.push(format!("coding {}: empty quote", c.code));
        } else if !norm_text.contains(&norm_quote) {
            rejected.push(format!(
                "coding {}: quote not found in document text: {:?}",
                c.code, c.quote
            ));
        } else if !norm_quote.contains(&c.code) {
            rejected.push(format!(
                "coding {}: code does not appear in its quote {:?}",
                c.code, c.quote
            ));
        } else {
            codings.push(ExtractedCoding {
                system: ICD10_CM_SYSTEM.to_owned(),
                code: c.code,
                display: c.display,
                quote: c.quote,
                section_label: c.section_label,
            });
        }
    }

    let (document_date, document_date_quote) = match (raw.document_date, raw.document_date_quote) {
        (Some(date), Some(quote)) => {
            let norm_quote = normalize_ws(&quote);
            let candidates = date_candidates(&date);
            match candidates {
                Some(cands)
                    if norm_text.contains(&norm_quote)
                        && cands.iter().any(|c| norm_quote.contains(c.as_str())) =>
                {
                    (Some(date), Some(quote))
                }
                _ => {
                    rejected.push(format!(
                        "document_date {date}: quote missing from text or date not in quote: {quote:?}"
                    ));
                    (None, None)
                }
            }
        }
        (Some(date), None) => {
            rejected.push(format!("document_date {date}: no supporting quote"));
            (None, None)
        }
        (None, _) => (None, None),
    };

    VerifiedExtraction {
        document_date,
        document_date_quote,
        title: raw.title,
        codings,
        rejected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extraction::artifact::RawCoding;

    // NBSP after the colon, mirroring real portal-printout extraction output.
    const TEXT: &str = "Order Date:\u{a0}04/21/2026\n\
        Pre-Op Diagnosis/Indications: Abdominal pain,\n unspecified - R10.9\n\
        DIAGNOSIS: NEGATIVE FOR DYSPLASIA";

    fn raw(codings: Vec<RawCoding>, date: Option<&str>, date_quote: Option<&str>) -> RawExtraction {
        RawExtraction {
            document_date: date.map(str::to_owned),
            document_date_quote: date_quote.map(str::to_owned),
            title: Some("GI Pathology Report".to_owned()),
            codings,
        }
    }

    fn coding(code: &str, quote: &str) -> RawCoding {
        RawCoding {
            code: code.to_owned(),
            display: "Abdominal pain, unspecified".to_owned(),
            quote: quote.to_owned(),
            section_label: Some("Pre-Op Diagnosis/Indications".to_owned()),
        }
    }

    #[test]
    fn accepts_quote_across_whitespace_differences() {
        // The quote uses a single space where the text has a newline, and the
        // date quote uses a plain space where the text has an NBSP.
        let v = verify_extraction(
            TEXT,
            raw(
                vec![coding("R10.9", "Abdominal pain, unspecified - R10.9")],
                Some("2026-04-21"),
                Some("Order Date: 04/21/2026"),
            ),
        );
        assert_eq!(v.codings.len(), 1);
        assert_eq!(v.codings[0].system, ICD10_CM_SYSTEM);
        assert_eq!(v.document_date.as_deref(), Some("2026-04-21"));
        assert!(v.rejected.is_empty());
    }

    #[test]
    fn rejects_quote_not_in_text() {
        let v = verify_extraction(
            TEXT,
            raw(vec![coding("K62.5", "Hemorrhage - K62.5")], None, None),
        );
        assert!(v.codings.is_empty());
        assert_eq!(v.rejected.len(), 1);
        assert!(v.rejected[0].contains("K62.5"));
    }

    #[test]
    fn rejects_code_missing_from_its_quote() {
        let v = verify_extraction(
            TEXT,
            raw(
                vec![coding("Z12.11", "Abdominal pain, unspecified - R10.9")],
                None,
                None,
            ),
        );
        assert!(v.codings.is_empty());
        assert_eq!(v.rejected.len(), 1);
    }

    #[test]
    fn rejects_date_whose_quote_lacks_the_date() {
        let v = verify_extraction(
            TEXT,
            raw(vec![], Some("2026-05-01"), Some("Order Date: 04/21/2026")),
        );
        assert_eq!(v.document_date, None);
        assert_eq!(v.rejected.len(), 1);
    }

    #[test]
    fn rejects_date_without_quote() {
        let v = verify_extraction(TEXT, raw(vec![], Some("2026-04-21"), None));
        assert_eq!(v.document_date, None);
        assert_eq!(v.rejected.len(), 1);
    }
}
```

- [ ] **Step 3: Wire into `mod.rs`**

`crates/chartpds-core/src/extraction/mod.rs` becomes:

```rust
//! Narrative-document extraction: deterministic PDF text + one-time,
//! mechanically verified LLM extraction of explicitly quoted codings.
//!
//! Design invariant: the LLM runs once, at ingest. Its verified output is
//! frozen as an archived artifact; `rebuild_index` replays the artifact and
//! never calls a model.

mod artifact;
mod error;
mod pdf;
mod verify;

pub use artifact::{
    ExtractedCoding, ExtractionArtifact, ExtractorInfo, RawCoding, RawExtraction,
    ICD10_CM_SYSTEM,
};
pub use error::Error;
pub use pdf::extract_pdf_text;
pub use verify::{verify_extraction, VerifiedExtraction};
```

- [ ] **Step 4: Run tests, gate, commit**

Run: `cargo test -p chartpds-core extraction`
Expected: pdf (2) + artifact (1) + verify (5) tests PASS.

Run: `just check`
Expected: PASS.

```bash
git add crates/chartpds-core/src/extraction
git commit -m "Add extraction artifact types and mechanical verification"
```

---

### Task 5: `extraction::llm` — Claude API extractor behind a trait

**Files:**
- Create: `crates/chartpds-core/src/extraction/llm.rs`
- Modify: `crates/chartpds-core/src/extraction/mod.rs`

**Interfaces:**
- Consumes: `RawExtraction`, `Error` from Task 4.
- Produces (used by Tasks 6, 9):
  - `extraction::LlmExtractor` trait: `fn extract(&self, text: &str) -> impl Future<Output = Result<RawExtraction, Error>> + Send`
  - `extraction::ClaudeExtractor::new(http: reqwest::Client, api_key: String) -> ClaudeExtractor`
  - `extraction::ClaudeExtractor::from_env(http: reqwest::Client) -> Option<ClaudeExtractor>` (reads `ANTHROPIC_API_KEY`; `None` if unset/empty)
  - `extraction::EXTRACTION_MODEL: &str = "claude-opus-4-8"`, `extraction::PROMPT_VERSION: u32 = 1`

- [ ] **Step 1: Write failing tests + implementation**

`crates/chartpds-core/src/extraction/llm.rs`:

```rust
//! One-shot LLM extraction via the Claude API.
//!
//! Rust has no official Anthropic SDK, so this is a plain `reqwest` call to
//! `POST /v1/messages` with a structured-output JSON schema
//! (`output_config.format`), which guarantees the response text is valid
//! JSON matching [`RawExtraction`]. The model id and prompt version are
//! pinned here and recorded in every archived artifact.

use std::future::Future;

use super::artifact::RawExtraction;
use super::error::Error;

/// Claude model used for extraction. Recorded in every artifact.
pub const EXTRACTION_MODEL: &str = "claude-opus-4-8";

/// Version of [`EXTRACTION_PROMPT`]. Bump when the prompt changes.
pub const PROMPT_VERSION: u32 = 1;

const API_URL: &str = "https://api.anthropic.com/v1/messages";
const API_VERSION: &str = "2023-06-01";

const EXTRACTION_PROMPT: &str = "You are extracting structured data from a clinical narrative \
document for a personal health record. Work ONLY from the document text below; never use \
outside knowledge to add codes that are not literally present.\n\
\n\
Extract:\n\
1. document_date: the single calendar date this document is about (order/collection date for \
a lab or pathology report; visit date for a note), formatted YYYY-MM-DD, with \
document_date_quote set to an exact text span (copied verbatim from the document) that \
contains that date. If no date is present, use null for both.\n\
2. title: a short human-readable label for the document, \
e.g. \"GI Pathology Report — colon biopsy\".\n\
3. codings: every ICD-10 code that appears VERBATIM in the text. For each: code exactly as \
written; display = the diagnosis text the document pairs with the code; quote = an exact \
text span (copied verbatim, including the code) where it appears; section_label = the \
section heading it appears under (e.g. \"Pre-Op Diagnosis/Indications\"), or null.\n\
\n\
Do not include codes that do not appear in the text. Copy quotes exactly — they are checked \
mechanically against the document, and any quote that is not a verbatim substring is \
discarded.";

/// Anything that can turn document text into a [`RawExtraction`].
///
/// Behind a trait so ingestion tests can use a canned extractor instead of
/// the network. Uses a native `impl Future` return (no `async_trait`),
/// matching the `sources::Source` convention.
pub trait LlmExtractor {
    /// Extract structured claims from the document text.
    fn extract(&self, text: &str) -> impl Future<Output = Result<RawExtraction, Error>> + Send;
}

/// The production extractor: calls the Claude API.
#[derive(Debug, Clone)]
pub struct ClaudeExtractor {
    http: reqwest::Client,
    api_key: String,
}

impl ClaudeExtractor {
    /// Build an extractor from an HTTP client and API key.
    #[must_use]
    pub fn new(http: reqwest::Client, api_key: String) -> Self {
        Self { http, api_key }
    }

    /// Build from the `ANTHROPIC_API_KEY` environment variable.
    ///
    /// Returns `None` when the variable is unset or empty — the caller
    /// degrades to text-only ingestion.
    #[must_use]
    pub fn from_env(http: reqwest::Client) -> Option<Self> {
        match std::env::var("ANTHROPIC_API_KEY") {
            Ok(key) if !key.is_empty() => Some(Self::new(http, key)),
            _ => None,
        }
    }
}

/// The JSON schema the API is constrained to (structured outputs).
fn output_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "document_date": {"type": ["string", "null"]},
            "document_date_quote": {"type": ["string", "null"]},
            "title": {"type": ["string", "null"]},
            "codings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string"},
                        "display": {"type": "string"},
                        "quote": {"type": "string"},
                        "section_label": {"type": ["string", "null"]}
                    },
                    "required": ["code", "display", "quote", "section_label"],
                    "additionalProperties": false
                }
            }
        },
        "required": ["document_date", "document_date_quote", "title", "codings"],
        "additionalProperties": false
    })
}

/// Build the `POST /v1/messages` request body for a document text.
fn build_request_body(text: &str) -> serde_json::Value {
    serde_json::json!({
        "model": EXTRACTION_MODEL,
        "max_tokens": 16000,
        "output_config": {"format": {"type": "json_schema", "schema": output_schema()}},
        "messages": [{
            "role": "user",
            "content": format!("{EXTRACTION_PROMPT}\n\n<document>\n{text}\n</document>"),
        }],
    })
}

/// Parse the Messages API response body into a [`RawExtraction`].
fn parse_response(body: &serde_json::Value) -> Result<RawExtraction, Error> {
    if let Some(stop) = body.get("stop_reason").and_then(|v| v.as_str()) {
        if stop == "refusal" {
            return Err(Error::Api {
                reason: "model refused the extraction request".to_owned(),
            });
        }
        if stop == "max_tokens" {
            return Err(Error::InvalidResponse {
                reason: "response truncated at max_tokens".to_owned(),
            });
        }
    }
    let text = body
        .get("content")
        .and_then(|c| c.as_array())
        .and_then(|blocks| {
            blocks
                .iter()
                .find(|b| b.get("type").and_then(|t| t.as_str()) == Some("text"))
        })
        .and_then(|b| b.get("text"))
        .and_then(|t| t.as_str())
        .ok_or_else(|| Error::InvalidResponse {
            reason: "no text content block in response".to_owned(),
        })?;
    serde_json::from_str(text).map_err(|err| Error::InvalidResponse {
        reason: format!("structured output did not parse as RawExtraction: {err}"),
    })
}

impl LlmExtractor for ClaudeExtractor {
    async fn extract(&self, text: &str) -> Result<RawExtraction, Error> {
        let response = self
            .http
            .post(API_URL)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", API_VERSION)
            .json(&build_request_body(text))
            .send()
            .await
            .map_err(|err| Error::Api {
                reason: format!("request failed: {err}"),
            })?;
        let status = response.status();
        let body: serde_json::Value = response.json().await.map_err(|err| Error::Api {
            reason: format!("reading response body: {err}"),
        })?;
        if !status.is_success() {
            return Err(Error::Api {
                reason: format!("HTTP {status}: {body}"),
            });
        }
        parse_response(&body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_body_pins_model_and_embeds_document() {
        let body = build_request_body("SAMPLE DOCUMENT TEXT");
        assert_eq!(body["model"], EXTRACTION_MODEL);
        assert_eq!(
            body["output_config"]["format"]["type"],
            "json_schema"
        );
        let content = body["messages"][0]["content"].as_str().expect("content");
        assert!(content.contains("SAMPLE DOCUMENT TEXT"));
        assert!(content.contains("ICD-10"));
        // No sampling params, no thinking config — both are 400s or noise here.
        assert!(body.get("temperature").is_none());
        assert!(body.get("thinking").is_none());
    }

    #[test]
    fn parses_a_successful_response() {
        let body = serde_json::json!({
            "stop_reason": "end_turn",
            "content": [{
                "type": "text",
                "text": r#"{"document_date":"2026-04-21","document_date_quote":"Order Date: 04/21/2026","title":"GI Pathology Report","codings":[{"code":"R10.9","display":"Abdominal pain, unspecified","quote":"Abdominal pain, unspecified - R10.9","section_label":"Pre-Op Diagnosis/Indications"}]}"#
            }]
        });
        let raw = parse_response(&body).expect("parse");
        assert_eq!(raw.document_date.as_deref(), Some("2026-04-21"));
        assert_eq!(raw.codings.len(), 1);
        assert_eq!(raw.codings[0].code, "R10.9");
    }

    #[test]
    fn refusal_maps_to_api_error() {
        let body = serde_json::json!({"stop_reason": "refusal", "content": []});
        let err = parse_response(&body).expect_err("should fail");
        assert!(matches!(err, Error::Api { .. }));
    }

    #[test]
    fn garbage_content_maps_to_invalid_response() {
        let body = serde_json::json!({
            "stop_reason": "end_turn",
            "content": [{"type": "text", "text": "not json"}]
        });
        let err = parse_response(&body).expect_err("should fail");
        assert!(matches!(err, Error::InvalidResponse { .. }));
    }
}
```

- [ ] **Step 2: Wire into `mod.rs`**

Add `mod llm;` and:

```rust
pub use llm::{ClaudeExtractor, LlmExtractor, EXTRACTION_MODEL, PROMPT_VERSION};
```

- [ ] **Step 3: Run tests, gate, commit**

Run: `cargo test -p chartpds-core extraction::llm`
Expected: 4 tests PASS (no network involved).

Run: `just check`
Expected: PASS.

```bash
git add crates/chartpds-core/src/extraction
git commit -m "Add Claude API extractor behind LlmExtractor trait"
```

---

### Task 6: `ingestion::narrative` — the live ingest orchestrator

**Files:**
- Create: `crates/chartpds-core/src/ingestion/narrative.rs`
- Modify: `crates/chartpds-core/src/ingestion/error.rs` (new variant)
- Modify: `crates/chartpds-core/src/ingestion/mod.rs` (declare + re-export)
- Modify: `crates/chartpds-core/src/index/source_documents.rs` (add `delete_by_id`, `set_document_date`)
- Modify: `crates/chartpds-core/src/index/mod.rs` (re-export the two new functions)

**Interfaces:**
- Consumes: Tasks 1–5 (`upsert_narrative_text`, `set_narrative_title`, `InsertProblemParams.section_label`, `extract_pdf_text`, `verify_extraction`, `LlmExtractor`, artifact types), `Archive::put_with_manifest`, `Manifest::new`, `insert_source_document`, `fetch_source_document_by_archive_key`, `insert_problem`.
- Produces (used by Tasks 7, 9):
  - `ingestion::NARRATIVE_PDF_KIND: &str = "clinical-pdf"`, `ingestion::NARRATIVE_EXTRACTION_KIND: &str = "narrative-extraction"`
  - `ingestion::ingest_narrative_pdf<E: LlmExtractor>(archive, pool, content: Bytes, source: &str, original_filename: Option<&str>, archived_at: OffsetDateTime, extractor: Option<&E>) -> Result<NarrativeIngestOutcome>`
  - `ingestion::NarrativeIngestOutcome { source_document_id: i64, title: Option<String>, document_date: Option<String>, codings_indexed: u64, extraction_status: String, extraction_error: Option<String>, rejected: Vec<String> }` (Serialize)
  - `pub(crate) narrative::apply_extraction(pool, source_document_id, &ExtractionArtifact) -> Result<u64>` (used by rebuild in Task 7)
  - `pub(crate) narrative::replay_pdf(pool, key: &BlobKey, content: &Bytes, manifest: &Manifest) -> Result<i64>` (used by rebuild in Task 7)
  - `index::delete_source_document(pool, id) -> Result<(), sqlx::Error>`, `index::set_source_document_date(pool, id, date: &str) -> Result<(), sqlx::Error>`
  - `ingestion::Error::Extraction(extraction::Error)` variant (with `From`)

- [ ] **Step 1: Index helpers**

In `crates/chartpds-core/src/index/source_documents.rs` add:

```rust
/// Delete a source document by id. FK CASCADE removes dependent rows
/// (observations, problems, medications, narrative_texts).
///
/// # Errors
///
/// Returns `sqlx::Error` if the statement fails.
pub async fn delete_by_id(pool: &SqlitePool, id: i64) -> Result<(), sqlx::Error> {
    sqlx::query!("DELETE FROM source_documents WHERE id = ?", id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Set the `document_date` of an existing source document.
///
/// # Errors
///
/// Returns `sqlx::Error` if the statement fails.
pub async fn set_document_date(
    pool: &SqlitePool,
    id: i64,
    document_date: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "UPDATE source_documents SET document_date = ? WHERE id = ?",
        document_date,
        id,
    )
    .execute(pool)
    .await?;
    Ok(())
}
```

In `index/mod.rs` extend the `source_documents` re-export:

```rust
pub use source_documents::{
    delete_by_id as delete_source_document,
    fetch_by_archive_key as fetch_source_document_by_archive_key,
    get_by_id as get_source_document_by_id, insert as insert_source_document,
    insert_superseding as insert_source_document_superseding,
    set_document_date as set_source_document_date,
    InsertParams as InsertSourceDocumentParams, SourceDocument, SupersedeOutcome,
};
```

- [ ] **Step 2: Error variant**

In `crates/chartpds-core/src/ingestion/error.rs` add to the enum:

```rust
    /// PDF text extraction or LLM extraction failed fatally.
    #[error("narrative extraction failed")]
    Extraction(#[source] crate::extraction::Error),
```

and the conversion:

```rust
impl From<crate::extraction::Error> for Error {
    fn from(err: crate::extraction::Error) -> Self {
        Self::Extraction(err)
    }
}
```

- [ ] **Step 3: Write `ingestion/narrative.rs`**

```rust
//! Narrative PDF ingestion: archive → deterministic text → one-time verified
//! LLM extraction (frozen as an archived artifact) → index rows.

use bytes::Bytes;
use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::archive::{Archive, BlobKey, Manifest};
use crate::extraction::{
    extract_pdf_text, verify_extraction, ExtractionArtifact, ExtractorInfo, LlmExtractor,
    VerifiedExtraction, EXTRACTION_MODEL, PROMPT_VERSION,
};
use crate::index::{
    delete_source_document, fetch_source_document_by_archive_key, insert_problem,
    insert_source_document, set_narrative_title, set_source_document_date,
    upsert_narrative_text, InsertProblemParams, InsertSourceDocumentParams,
    UpsertNarrativeTextParams,
};
use crate::ingestion::{Error, Result};

/// `source_documents.kind` / manifest `type` for a narrative PDF blob.
pub const NARRATIVE_PDF_KIND: &str = "clinical-pdf";
/// Manifest `type` for the frozen extraction artifact blob.
pub const NARRATIVE_EXTRACTION_KIND: &str = "narrative-extraction";

/// What `ingest_narrative_pdf` did, reported in-band to the tool caller.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NarrativeIngestOutcome {
    /// The `source_documents.id` of the ingested narrative.
    pub source_document_id: i64,
    /// Extractor-authored title, if extraction ran.
    pub title: Option<String>,
    /// Verified document date, if extraction ran and the date verified.
    pub document_date: Option<String>,
    /// Number of verified codings indexed into `problems`.
    pub codings_indexed: u64,
    /// `"applied"`, `"skipped_no_extractor"`, or `"failed"`.
    pub extraction_status: String,
    /// Failure detail when `extraction_status == "failed"`.
    pub extraction_error: Option<String>,
    /// Human-readable reasons for LLM claims dropped by verification.
    pub rejected: Vec<String>,
}

/// Ingest one narrative PDF.
///
/// Steps: extract text (fail fast on scans — nothing archived) → optional
/// LLM extraction + mechanical verification → archive the PDF blob (manifest
/// `subject` = verified date) → archive the verified extraction as its own
/// JSON artifact blob → upsert index rows (document, narrative text, coded
/// problems).
///
/// LLM failure and absence of an extractor degrade gracefully: the PDF is
/// still archived and text-indexed, and the degradation is reported in the
/// returned outcome rather than as an error.
///
/// # Errors
///
/// Returns [`Error::Extraction`] when the PDF has no text layer or cannot be
/// parsed, and [`Error::Archive`]/[`Error::Database`] on storage failures.
pub async fn ingest_narrative_pdf<E: LlmExtractor>(
    archive: &Archive,
    pool: &SqlitePool,
    content: Bytes,
    source: &str,
    original_filename: Option<&str>,
    archived_at: OffsetDateTime,
    extractor: Option<&E>,
) -> Result<NarrativeIngestOutcome> {
    // 1. Deterministic text extraction. A scan/no-text PDF is a hard error
    //    before anything is archived — the caller should hear "OCR
    //    unsupported" rather than accumulate unusable blobs.
    let text = extract_pdf_text(&content)?;

    // 2. LLM extraction + verification (soft-fail).
    let (verified, extraction_status, extraction_error): (
        Option<VerifiedExtraction>,
        &str,
        Option<String>,
    ) = match extractor {
        None => (None, "skipped_no_extractor", None),
        Some(e) => match e.extract(&text).await {
            Ok(raw) => (Some(verify_extraction(&text, raw)), "applied", None),
            Err(err) => (None, "failed", Some(err.to_string())),
        },
    };

    // 3. Archive the PDF blob. `subject` carries the verified document date
    //    so the blob is self-describing for text-only replay.
    let pdf_manifest = Manifest::new(
        source,
        NARRATIVE_PDF_KIND,
        "application/pdf",
        verified.as_ref().and_then(|v| v.document_date.clone()),
        archived_at,
        original_filename.map(str::to_owned),
    );
    let pdf_key = archive.put_with_manifest(content.clone(), pdf_manifest).await?;

    // 4. Freeze the verified extraction as its own archived artifact.
    let artifact = verified.as_ref().map(|v| ExtractionArtifact {
        document: pdf_key.to_string(),
        document_date: v.document_date.clone(),
        document_date_quote: v.document_date_quote.clone(),
        title: v.title.clone(),
        codings: v.codings.clone(),
        extractor: ExtractorInfo {
            model: EXTRACTION_MODEL.to_owned(),
            prompt_version: PROMPT_VERSION,
        },
        extracted_at: archived_at,
    });
    if let Some(a) = &artifact {
        let bytes = serde_json::to_vec(a).map_err(|err| {
            Error::Extraction(crate::extraction::Error::InvalidResponse {
                reason: format!("serializing artifact: {err}"),
            })
        })?;
        let manifest = Manifest::new(
            "chartpds",
            NARRATIVE_EXTRACTION_KIND,
            "application/json",
            Some(pdf_key.to_string()),
            archived_at,
            None,
        );
        archive.put_with_manifest(Bytes::from(bytes), manifest).await?;
    }

    // 5. Upsert the document row: re-ingest of the same bytes replaces the
    //    prior rows (cascade cleans narrative_texts + problems, and the FTS
    //    delete trigger fires on the cascade).
    if let Some(existing) = fetch_source_document_by_archive_key(pool, &pdf_key).await? {
        delete_source_document(pool, existing.id).await?;
    }
    let source_document_id = insert_source_document(
        pool,
        InsertSourceDocumentParams {
            archive_key: &pdf_key,
            kind: NARRATIVE_PDF_KIND,
            source,
            original_filename,
            archived_at,
            document_date: None, // applied from the artifact below
        },
    )
    .await?;

    // 6. Index the text (FTS via triggers).
    upsert_narrative_text(
        pool,
        UpsertNarrativeTextParams {
            source_document_id,
            title: None, // applied from the artifact below
            text: &text,
        },
    )
    .await?;

    // 7. Apply the artifact (date, title, coded problems).
    let mut codings_indexed = 0;
    if let Some(a) = &artifact {
        codings_indexed = apply_extraction(pool, source_document_id, a).await?;
    }

    Ok(NarrativeIngestOutcome {
        source_document_id,
        title: artifact.as_ref().and_then(|a| a.title.clone()),
        document_date: artifact.as_ref().and_then(|a| a.document_date.clone()),
        codings_indexed,
        extraction_status: extraction_status.to_owned(),
        extraction_error,
        rejected: verified.map(|v| v.rejected).unwrap_or_default(),
    })
}

/// Apply a frozen extraction artifact to an indexed narrative document:
/// set the document date and title, insert one `problems` row per coding.
///
/// Shared by live ingestion and `rebuild_index` — this is the ONLY code path
/// that turns an artifact into index rows.
pub(crate) async fn apply_extraction(
    pool: &SqlitePool,
    source_document_id: i64,
    artifact: &ExtractionArtifact,
) -> Result<u64> {
    if let Some(date) = &artifact.document_date {
        set_source_document_date(pool, source_document_id, date).await?;
    }
    if let Some(title) = &artifact.title {
        set_narrative_title(pool, source_document_id, title).await?;
    }
    let mut count = 0u64;
    for c in &artifact.codings {
        insert_problem(
            pool,
            InsertProblemParams {
                source_document_id,
                coding_system: &c.system,
                coding_code: &c.code,
                coding_display: Some(&c.display),
                status: "unknown",
                onset_date: artifact.document_date.as_deref(),
                section_label: c.section_label.as_deref(),
            },
        )
        .await?;
        count += 1;
    }
    Ok(count)
}

/// Text-only replay of an archived narrative PDF blob during rebuild.
///
/// Re-derives the text deterministically and rebuilds the document +
/// narrative rows. The document date comes from the manifest `subject`
/// (also re-applied by the artifact pass, when an artifact exists).
/// Returns the new `source_documents.id`.
pub(crate) async fn replay_pdf(
    pool: &SqlitePool,
    key: &BlobKey,
    content: &Bytes,
    manifest: &Manifest,
) -> Result<i64> {
    let text = extract_pdf_text(content)?;
    let source_document_id = insert_source_document(
        pool,
        InsertSourceDocumentParams {
            archive_key: key,
            kind: NARRATIVE_PDF_KIND,
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
            text: &text,
        },
    )
    .await?;
    Ok(source_document_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extraction::{RawCoding, RawExtraction};
    use crate::index::{list_problems_by_source_document, open_pool};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    const FIXTURE: &[u8] = include_bytes!("../extraction/fixtures/synthetic_pathology.pdf");

    async fn fresh_pool_and_archive() -> (SqlitePool, Archive) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        (pool, Archive::new(backend))
    }

    /// Canned extractor: returns a fixed RawExtraction without any network.
    struct MockExtractor(RawExtraction);
    impl LlmExtractor for MockExtractor {
        async fn extract(&self, _text: &str) -> std::result::Result<RawExtraction, crate::extraction::Error> {
            Ok(self.0.clone())
        }
    }

    /// Canned failing extractor.
    struct FailingExtractor;
    impl LlmExtractor for FailingExtractor {
        async fn extract(&self, _text: &str) -> std::result::Result<RawExtraction, crate::extraction::Error> {
            Err(crate::extraction::Error::Api {
                reason: "simulated outage".to_owned(),
            })
        }
    }

    fn fixture_extraction() -> RawExtraction {
        RawExtraction {
            document_date: Some("2026-04-21".to_owned()),
            document_date_quote: Some("Order Date: 04/21/2026".to_owned()),
            title: Some("GI Pathology Report — colon biopsy".to_owned()),
            codings: vec![
                RawCoding {
                    code: "R10.9".to_owned(),
                    display: "Abdominal pain, unspecified".to_owned(),
                    quote: "Abdominal pain, unspecified - R10.9".to_owned(),
                    section_label: Some("Pre-Op Diagnosis/Indications".to_owned()),
                },
                // A hallucinated code the fixture does not contain: must be
                // rejected by verification and never reach the index.
                RawCoding {
                    code: "K62.5".to_owned(),
                    display: "Hemorrhage of anus and rectum".to_owned(),
                    quote: "Hemorrhage of anus and rectum - K62.5".to_owned(),
                    section_label: None,
                },
            ],
        }
    }

    #[tokio::test]
    async fn ingests_pdf_with_verified_extraction() {
        let (pool, archive) = fresh_pool_and_archive().await;
        let extractor = MockExtractor(fixture_extraction());

        let outcome = ingest_narrative_pdf(
            &archive,
            &pool,
            Bytes::from_static(FIXTURE),
            "manual-upload",
            Some("synthetic_pathology.pdf"),
            OffsetDateTime::now_utc(),
            Some(&extractor),
        )
        .await
        .expect("ingest");

        assert_eq!(outcome.extraction_status, "applied");
        assert_eq!(outcome.codings_indexed, 1);
        assert_eq!(outcome.rejected.len(), 1, "hallucinated coding rejected");
        assert_eq!(outcome.document_date.as_deref(), Some("2026-04-21"));

        // Two blobs: PDF + artifact.
        assert_eq!(archive.list_keys().await.expect("keys").len(), 2);

        // Problems row landed with section label and unknown status.
        let problems = list_problems_by_source_document(&pool, outcome.source_document_id)
            .await
            .expect("problems");
        assert_eq!(problems.len(), 1);
        assert_eq!(problems[0].coding_code, "R10.9");
        assert_eq!(problems[0].status, "unknown");
        assert_eq!(
            problems[0].section_label.as_deref(),
            Some("Pre-Op Diagnosis/Indications")
        );

        // Document row has the verified date.
        let doc = crate::index::get_source_document_by_id(&pool, outcome.source_document_id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(doc.kind, NARRATIVE_PDF_KIND);
        assert_eq!(doc.document_date.as_deref(), Some("2026-04-21"));
    }

    #[tokio::test]
    async fn degrades_to_text_only_without_extractor() {
        let (pool, archive) = fresh_pool_and_archive().await;
        let outcome = ingest_narrative_pdf(
            &archive,
            &pool,
            Bytes::from_static(FIXTURE),
            "manual-upload",
            None,
            OffsetDateTime::now_utc(),
            None::<&crate::extraction::ClaudeExtractor>,
        )
        .await
        .expect("ingest");
        assert_eq!(outcome.extraction_status, "skipped_no_extractor");
        assert_eq!(outcome.codings_indexed, 0);
        // Only the PDF blob — no artifact.
        assert_eq!(archive.list_keys().await.expect("keys").len(), 1);
        // Text is still indexed.
        let text = crate::index::get_narrative_text(&pool, outcome.source_document_id)
            .await
            .expect("get")
            .expect("present");
        assert!(text.text.contains("DIAGNOSIS"));
    }

    #[tokio::test]
    async fn degrades_to_text_only_on_llm_failure() {
        let (pool, archive) = fresh_pool_and_archive().await;
        let outcome = ingest_narrative_pdf(
            &archive,
            &pool,
            Bytes::from_static(FIXTURE),
            "manual-upload",
            None,
            OffsetDateTime::now_utc(),
            Some(&FailingExtractor),
        )
        .await
        .expect("ingest");
        assert_eq!(outcome.extraction_status, "failed");
        assert!(outcome.extraction_error.as_deref().unwrap_or("").contains("outage"));
        assert_eq!(archive.list_keys().await.expect("keys").len(), 1);
        let _ = pool; // document + text rows exist; covered above
    }

    #[tokio::test]
    async fn re_ingest_of_same_pdf_upserts_without_duplicates() {
        let (pool, archive) = fresh_pool_and_archive().await;
        let extractor = MockExtractor(fixture_extraction());
        let first = ingest_narrative_pdf(
            &archive,
            &pool,
            Bytes::from_static(FIXTURE),
            "manual-upload",
            None,
            OffsetDateTime::now_utc(),
            Some(&extractor),
        )
        .await
        .expect("first");
        let second = ingest_narrative_pdf(
            &archive,
            &pool,
            Bytes::from_static(FIXTURE),
            "manual-upload",
            None,
            OffsetDateTime::now_utc(),
            Some(&extractor),
        )
        .await
        .expect("second");
        assert_ne!(first.source_document_id, second.source_document_id);

        let doc_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM source_documents WHERE kind = 'clinical-pdf'",
        )
        .fetch_one(&pool)
        .await
        .expect("count");
        assert_eq!(doc_count.0, 1, "same bytes must not duplicate");
        let prob_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM problems")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(prob_count.0, 1, "problems must not accumulate");
    }

    #[tokio::test]
    async fn non_pdf_bytes_fail_before_archiving() {
        let (pool, archive) = fresh_pool_and_archive().await;
        let err = ingest_narrative_pdf(
            &archive,
            &pool,
            Bytes::from_static(b"plain text, not a pdf"),
            "manual-upload",
            None,
            OffsetDateTime::now_utc(),
            None::<&crate::extraction::ClaudeExtractor>,
        )
        .await
        .expect_err("must fail");
        assert!(matches!(err, Error::Extraction(_)));
        assert!(archive.list_keys().await.expect("keys").is_empty());
    }
}
```

Note: the `use Rfc3339 as _` shim exists only if the compiler complains about the unused import — **if `Rfc3339` is genuinely unused, delete the import and the shim entirely** (prefer deletion over suppression).

- [ ] **Step 4: Wire into `ingestion/mod.rs`**

```rust
mod narrative;
```

```rust
pub use narrative::{
    ingest_narrative_pdf, NarrativeIngestOutcome, NARRATIVE_EXTRACTION_KIND, NARRATIVE_PDF_KIND,
};
```

Update the module doc comment to mention narrative ingestion.

- [ ] **Step 5: prepare-sql, test, gate, commit**

Run: `just prepare-sql && cargo test -p chartpds-core narrative`
Expected: 5 narrative tests + narrative_texts tests PASS.

Run: `just check`
Expected: PASS.

```bash
git add -A crates/chartpds-core/src .sqlx/
git commit -m "Add narrative PDF ingest orchestrator with verified extraction artifacts"
```

---

### Task 7: Rebuild — replay `clinical-pdf` and apply `narrative-extraction` artifacts (two-phase)

**Files:**
- Modify: `crates/chartpds-core/src/ingestion/rebuild.rs`

**Interfaces:**
- Consumes: `narrative::replay_pdf`, `narrative::apply_extraction`, `NARRATIVE_PDF_KIND`, `NARRATIVE_EXTRACTION_KIND`, `ExtractionArtifact`, `fetch_source_document_by_archive_key`, `BlobKey::from_hex_str`.
- Produces: `RebuildResult` gains `pub narratives_ingested: u64` and `pub extractions_applied: u64` (additive; existing consumers unaffected).

- [ ] **Step 1: Extend `RebuildResult`**

Add to the struct with doc comments:

```rust
    /// Narrative PDF documents replayed (text re-extracted deterministically).
    pub narratives_ingested: u64,
    /// Frozen extraction artifacts applied to their narrative documents.
    pub extractions_applied: u64,
```

- [ ] **Step 2: Two-phase replay in `rebuild_index`**

In the manifest-kind `match`, add two arms (using the constants from `narrative`; add `use crate::ingestion::narrative;` and `use crate::extraction::ExtractionArtifact;` imports as needed):

```rust
                narrative::NARRATIVE_PDF_KIND => {
                    match narrative::replay_pdf(pool, key, &content, &manifest).await {
                        Ok(_) => narratives_ingested += 1,
                        Err(Error::Extraction(err)) => {
                            tracing::warn!(key = key.as_str(), %err, "skipping unreadable narrative pdf");
                            blobs_skipped += 1;
                        }
                        Err(err) => return Err(err),
                    }
                }
                narrative::NARRATIVE_EXTRACTION_KIND => {
                    match serde_json::from_slice::<ExtractionArtifact>(&content) {
                        Ok(artifact) => extraction_artifacts.push((manifest.archived_at, artifact)),
                        Err(err) => {
                            tracing::warn!(key = key.as_str(), %err, "skipping malformed extraction artifact");
                            blobs_skipped += 1;
                        }
                    }
                }
```

Declare `let mut narratives_ingested = 0u64;`, `let mut extractions_applied = 0u64;`, and `let mut extraction_artifacts: Vec<(time::OffsetDateTime, ExtractionArtifact)> = Vec::new();` alongside the existing counters.

After the blob loop, add the artifact pass (phase two — runs after every document exists, and collapses multiple artifacts per document to the newest):

```rust
    // Phase two: apply extraction artifacts. When the same PDF was re-ingested
    // (retried extraction), multiple artifacts reference it — apply only the
    // newest by archived_at.
    let mut newest: std::collections::HashMap<String, (time::OffsetDateTime, ExtractionArtifact)> =
        std::collections::HashMap::new();
    for (at, artifact) in extraction_artifacts {
        match newest.get(&artifact.document) {
            Some((existing_at, _)) if *existing_at >= at => {}
            _ => {
                newest.insert(artifact.document.clone(), (at, artifact));
            }
        }
    }
    for (_at, artifact) in newest.into_values() {
        let Ok(pdf_key) = crate::archive::BlobKey::from_hex_str(&artifact.document) else {
            tracing::warn!(document = %artifact.document, "artifact references invalid blob key");
            blobs_skipped += 1;
            continue;
        };
        match index::fetch_source_document_by_archive_key(pool, &pdf_key).await? {
            Some(doc) => {
                narrative::apply_extraction(pool, doc.id, &artifact).await?;
                extractions_applied += 1;
            }
            None => {
                tracing::warn!(document = %artifact.document, "artifact references missing document");
                blobs_skipped += 1;
            }
        }
    }
```

(`rebuild.rs` is in the same `ingestion` module family, so `narrative::apply_extraction`/`replay_pdf` being `pub(crate)` in `narrative.rs` are reachable via `crate::ingestion::narrative::...` — declare `mod narrative;` visibility accordingly: keep `mod narrative;` private in `mod.rs`; `rebuild.rs` refers to it as `super::narrative` or `crate::ingestion::narrative`. Use `use super::narrative;`.)

Include the two new counters in the returned `RebuildResult`.

- [ ] **Step 3: Write the failing test**

Add to `rebuild.rs` tests:

```rust
    #[tokio::test]
    async fn rebuild_replays_narrative_pdf_and_applies_artifact_without_llm() {
        use crate::extraction::{RawCoding, RawExtraction};
        use crate::ingestion::{ingest_narrative_pdf, NARRATIVE_PDF_KIND};

        const FIXTURE: &[u8] =
            include_bytes!("../extraction/fixtures/synthetic_pathology.pdf");

        struct MockExtractor;
        impl crate::extraction::LlmExtractor for MockExtractor {
            async fn extract(
                &self,
                _text: &str,
            ) -> Result<RawExtraction, crate::extraction::Error> {
                Ok(RawExtraction {
                    document_date: Some("2026-04-21".to_owned()),
                    document_date_quote: Some("Order Date: 04/21/2026".to_owned()),
                    title: Some("GI Pathology Report".to_owned()),
                    codings: vec![RawCoding {
                        code: "R10.9".to_owned(),
                        display: "Abdominal pain, unspecified".to_owned(),
                        quote: "Abdominal pain, unspecified - R10.9".to_owned(),
                        section_label: Some("Pre-Op Diagnosis/Indications".to_owned()),
                    }],
                })
            }
        }

        let (pool, archive) = fresh_pool_and_archive().await;
        ingest_narrative_pdf(
            &archive,
            &pool,
            Bytes::from_static(FIXTURE),
            "manual-upload",
            Some("synthetic_pathology.pdf"),
            time::macros::datetime!(2026-07-06 12:00:00 UTC),
            Some(&MockExtractor),
        )
        .await
        .expect("live ingest");

        // Rebuild must reproduce everything from the archive alone — the
        // extractor is NOT provided anywhere in the rebuild path.
        let result = rebuild_index(&archive, &pool).await.expect("rebuild");
        assert_eq!(result.blobs_found, 2);
        assert_eq!(result.narratives_ingested, 1);
        assert_eq!(result.extractions_applied, 1);
        assert_eq!(result.blobs_skipped, 0);

        // The coded problem, title, date, and FTS text all survived.
        let doc_row: (i64, Option<String>) = sqlx::query_as(
            "SELECT id, document_date FROM source_documents WHERE kind = ?",
        )
        .bind(NARRATIVE_PDF_KIND)
        .fetch_one(&pool)
        .await
        .expect("doc row");
        assert_eq!(doc_row.1.as_deref(), Some("2026-04-21"));

        let text = crate::index::get_narrative_text(&pool, doc_row.0)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(text.title.as_deref(), Some("GI Pathology Report"));

        let problems = crate::index::list_problems_by_source_document(&pool, doc_row.0)
            .await
            .expect("problems");
        assert_eq!(problems.len(), 1);
        assert_eq!(problems[0].coding_code, "R10.9");

        let fts: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM narrative_texts_fts WHERE narrative_texts_fts MATCH 'dysplasia'",
        )
        .fetch_one(&pool)
        .await
        .expect("fts");
        assert_eq!(fts.0, 1);
    }
```

- [ ] **Step 4: Run tests, gate, commit**

Run: `cargo test -p chartpds-core rebuild`
Expected: all existing rebuild tests + the new one PASS. (Existing tests construct `RebuildResult` implicitly, so the new fields only require updating any exhaustive struct literals the compiler flags.)

Run: `just check`
Expected: PASS.

```bash
git add crates/chartpds-core/src/ingestion .sqlx/
git commit -m "Replay narrative PDFs and apply frozen extraction artifacts on rebuild"
```

---

### Task 8: Queries — `search_narratives` and `get_narrative`

**Files:**
- Create: `crates/chartpds-core/src/queries/search_narratives.rs`
- Create: `crates/chartpds-core/src/queries/get_narrative.rs`
- Modify: `crates/chartpds-core/src/queries/mod.rs`

**Interfaces:**
- Consumes: `narrative_texts` + `narrative_texts_fts` (Task 1), `problems.section_label` (Task 2), `get_source_document_by_id`, `get_narrative_text`, `list_problems_by_source_document`.
- Produces (used by Task 9):
  - `queries::search_narratives(pool, query: Option<&str>, limit: i64) -> Result<Vec<NarrativeSearchHit>, sqlx::Error>` — `NarrativeSearchHit { source_document_id: i64, title: Option<String>, kind: String, source: String, document_date: Option<String>, snippet: String }`. With a query: BM25-ranked FTS matches with `snippet(...)`. Without: full catalog, newest `document_date` first, snippet = first 200 chars.
  - `queries::get_narrative(pool, source_document_id: i64) -> Result<Option<NarrativeDetail>, sqlx::Error>` — `NarrativeDetail { source_document_id: i64, kind: String, source: String, title: Option<String>, document_date: Option<String>, original_filename: Option<String>, text: String, codings: Vec<NarrativeCoding> }`, `NarrativeCoding { coding_system: String, coding_code: String, coding_display: Option<String>, section_label: Option<String> }`

- [ ] **Step 1: Write `search_narratives.rs`**

```rust
//! Free-form search over narrative document text (FTS5/BM25), with a
//! catalog-listing mode when no query is given.

use sqlx::SqlitePool;

/// One search hit (or catalog entry).
#[derive(Debug, Clone, serde::Serialize)]
pub struct NarrativeSearchHit {
    /// The narrative's `source_documents.id` — pass to `get_narrative`.
    pub source_document_id: i64,
    /// Extractor-authored title, if any.
    pub title: Option<String>,
    /// Document kind (e.g. `"clinical-pdf"`).
    pub kind: String,
    /// Ingest source (e.g. `"manual-upload"`).
    pub source: String,
    /// The document's calendar date, if known.
    pub document_date: Option<String>,
    /// Matching excerpt (FTS snippet) or the document's opening text.
    pub snippet: String,
}

/// Search narrative texts, or list them newest-first when `query` is `None`.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails — including FTS5 `MATCH` syntax
/// errors from a malformed query string (surface those to the caller as an
/// invalid parameter).
pub async fn search_narratives(
    pool: &SqlitePool,
    query: Option<&str>,
    limit: i64,
) -> Result<Vec<NarrativeSearchHit>, sqlx::Error> {
    match query {
        Some(q) => {
            let rows = sqlx::query!(
                r#"
                SELECT nt.source_document_id AS "source_document_id!: i64",
                       nt.title,
                       sd.kind AS "kind!", sd.source AS "source!",
                       sd.document_date,
                       snippet(narrative_texts_fts, 0, '[', ']', ' … ', 16)
                           AS "snippet!: String"
                FROM narrative_texts_fts
                JOIN narrative_texts nt ON nt.source_document_id = narrative_texts_fts.rowid
                JOIN source_documents sd ON sd.id = nt.source_document_id
                WHERE narrative_texts_fts MATCH ?
                ORDER BY bm25(narrative_texts_fts)
                LIMIT ?
                "#,
                q,
                limit,
            )
            .fetch_all(pool)
            .await?;
            Ok(rows
                .into_iter()
                .map(|r| NarrativeSearchHit {
                    source_document_id: r.source_document_id,
                    title: r.title,
                    kind: r.kind,
                    source: r.source,
                    document_date: r.document_date,
                    snippet: r.snippet,
                })
                .collect())
        }
        None => {
            let rows = sqlx::query!(
                r#"
                SELECT nt.source_document_id AS "source_document_id!: i64",
                       nt.title,
                       sd.kind AS "kind!", sd.source AS "source!",
                       sd.document_date,
                       substr(nt.text, 1, 200) AS "snippet!: String"
                FROM narrative_texts nt
                JOIN source_documents sd ON sd.id = nt.source_document_id
                ORDER BY sd.document_date DESC, sd.id DESC
                LIMIT ?
                "#,
                limit,
            )
            .fetch_all(pool)
            .await?;
            Ok(rows
                .into_iter()
                .map(|r| NarrativeSearchHit {
                    source_document_id: r.source_document_id,
                    title: r.title,
                    kind: r.kind,
                    source: r.source,
                    document_date: r.document_date,
                    snippet: r.snippet,
                })
                .collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::BlobKey;
    use crate::index::{
        insert_source_document, open_pool, upsert_narrative_text, InsertSourceDocumentParams,
        UpsertNarrativeTextParams,
    };
    use time::OffsetDateTime;

    async fn pool() -> SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    async fn narrative(pool: &SqlitePool, hex: &str, date: Option<&str>, text: &str) -> i64 {
        let key = BlobKey::from_hex_str(hex).expect("key");
        let id = insert_source_document(
            pool,
            InsertSourceDocumentParams {
                archive_key: &key,
                kind: "clinical-pdf",
                source: "manual-upload",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
                document_date: date,
            },
        )
        .await
        .expect("doc");
        upsert_narrative_text(
            pool,
            UpsertNarrativeTextParams {
                source_document_id: id,
                title: None,
                text,
            },
        )
        .await
        .expect("text");
        id
    }

    #[tokio::test]
    async fn query_matches_and_ranks_with_snippet() {
        let pool = pool().await;
        narrative(
            &pool,
            "1111111111111111111111111111111111111111111111111111111111111111",
            Some("2026-04-21"),
            "GI PATHOLOGY REPORT: BIOPSY TAKEN TO RULE OUT PROCTITIS",
        )
        .await;
        narrative(
            &pool,
            "2222222222222222222222222222222222222222222222222222222222222222",
            Some("2026-05-01"),
            "CARDIOLOGY VISIT NOTE: NORMAL SINUS RHYTHM",
        )
        .await;

        let hits = search_narratives(&pool, Some("proctitis"), 10)
            .await
            .expect("search");
        assert_eq!(hits.len(), 1);
        assert!(hits[0].snippet.contains("[PROCTITIS]"));
        assert_eq!(hits[0].document_date.as_deref(), Some("2026-04-21"));
    }

    #[tokio::test]
    async fn no_query_lists_catalog_newest_first() {
        let pool = pool().await;
        narrative(
            &pool,
            "3333333333333333333333333333333333333333333333333333333333333333",
            Some("2026-01-01"),
            "older document",
        )
        .await;
        let newer = narrative(
            &pool,
            "4444444444444444444444444444444444444444444444444444444444444444",
            Some("2026-06-01"),
            "newer document",
        )
        .await;

        let hits = search_narratives(&pool, None, 10).await.expect("list");
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].source_document_id, newer);
        assert_eq!(hits[1].snippet, "older document");
    }
}
```

- [ ] **Step 2: Write `get_narrative.rs`**

```rust
//! Full narrative document read: metadata + extracted text + codings.

use sqlx::SqlitePool;

use crate::index::{
    get_narrative_text, get_source_document_by_id, list_problems_by_source_document,
};

/// One coding extracted from this narrative.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NarrativeCoding {
    /// Coding system URI.
    pub coding_system: String,
    /// Code within the system.
    pub coding_code: String,
    /// Display text paired with the code in the document.
    pub coding_display: Option<String>,
    /// Verbatim section heading the code appeared under.
    pub section_label: Option<String>,
}

/// A narrative document with its full text and extracted codings.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NarrativeDetail {
    /// The `source_documents.id`.
    pub source_document_id: i64,
    /// Document kind.
    pub kind: String,
    /// Ingest source.
    pub source: String,
    /// Extractor-authored title, if any.
    pub title: Option<String>,
    /// Document date, if known.
    pub document_date: Option<String>,
    /// Original upload filename, if known.
    pub original_filename: Option<String>,
    /// Full extracted document text.
    pub text: String,
    /// Codings extracted (and verified) from this document.
    pub codings: Vec<NarrativeCoding>,
}

/// Fetch a narrative by `source_documents.id`.
///
/// Returns `None` when the id does not exist or is not a narrative (has no
/// `narrative_texts` row).
///
/// # Errors
///
/// Returns `sqlx::Error` if a query fails.
pub async fn get_narrative(
    pool: &SqlitePool,
    source_document_id: i64,
) -> Result<Option<NarrativeDetail>, sqlx::Error> {
    let Some(doc) = get_source_document_by_id(pool, source_document_id).await? else {
        return Ok(None);
    };
    let Some(nt) = get_narrative_text(pool, source_document_id).await? else {
        return Ok(None);
    };
    let codings = list_problems_by_source_document(pool, source_document_id)
        .await?
        .into_iter()
        .map(|p| NarrativeCoding {
            coding_system: p.coding_system,
            coding_code: p.coding_code,
            coding_display: p.coding_display,
            section_label: p.section_label,
        })
        .collect();
    Ok(Some(NarrativeDetail {
        source_document_id,
        kind: doc.kind,
        source: doc.source,
        title: nt.title,
        document_date: doc.document_date,
        original_filename: doc.original_filename,
        text: nt.text,
        codings,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::BlobKey;
    use crate::index::{
        insert_problem, insert_source_document, open_pool, upsert_narrative_text,
        InsertProblemParams, InsertSourceDocumentParams, UpsertNarrativeTextParams,
    };
    use time::OffsetDateTime;

    #[tokio::test]
    async fn returns_metadata_text_and_codings() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");

        let key = BlobKey::from_hex_str(
            "5555555555555555555555555555555555555555555555555555555555555555",
        )
        .expect("key");
        let id = insert_source_document(
            &pool,
            InsertSourceDocumentParams {
                archive_key: &key,
                kind: "clinical-pdf",
                source: "manual-upload",
                original_filename: Some("report.pdf"),
                archived_at: OffsetDateTime::now_utc(),
                document_date: Some("2026-04-21"),
            },
        )
        .await
        .expect("doc");
        upsert_narrative_text(
            &pool,
            UpsertNarrativeTextParams {
                source_document_id: id,
                title: Some("GI Pathology Report"),
                text: "full document text here",
            },
        )
        .await
        .expect("text");
        insert_problem(
            &pool,
            InsertProblemParams {
                source_document_id: id,
                coding_system: "http://hl7.org/fhir/sid/icd-10-cm",
                coding_code: "R10.9",
                coding_display: Some("Abdominal pain, unspecified"),
                status: "unknown",
                onset_date: Some("2026-04-21"),
                section_label: Some("Pre-Op Diagnosis/Indications"),
            },
        )
        .await
        .expect("problem");

        let detail = get_narrative(&pool, id)
            .await
            .expect("query")
            .expect("present");
        assert_eq!(detail.title.as_deref(), Some("GI Pathology Report"));
        assert_eq!(detail.text, "full document text here");
        assert_eq!(detail.codings.len(), 1);
        assert_eq!(detail.codings[0].coding_code, "R10.9");
        assert_eq!(
            detail.codings[0].section_label.as_deref(),
            Some("Pre-Op Diagnosis/Indications")
        );

        assert!(get_narrative(&pool, id + 999).await.expect("query").is_none());
    }
}
```

- [ ] **Step 3: Wire into `queries/mod.rs`**

```rust
mod get_narrative;
mod search_narratives;
```

```rust
pub use get_narrative::{get_narrative, NarrativeCoding, NarrativeDetail};
pub use search_narratives::{search_narratives, NarrativeSearchHit};
```

- [ ] **Step 4: prepare-sql, test, gate, commit**

Run: `just prepare-sql && cargo test -p chartpds-core narratives && cargo test -p chartpds-core get_narrative`
Expected: PASS. Same FTS-macro contingency as Task 1 Step 3 applies to the MATCH query.

Run: `just check`
Expected: PASS.

```bash
git add crates/chartpds-core/src/queries .sqlx/
git commit -m "Add search_narratives and get_narrative query primitives"
```

---

### Task 9: MCP surface — extend `ingest_record`, add `search_narratives` + `get_narrative` tools, update CLAUDE.md

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs`
- Modify: `CLAUDE.md`

**Interfaces:**
- Consumes: `ingest_narrative_pdf`, `NarrativeIngestOutcome`, `ClaudeExtractor::from_env`, `queries::{search_narratives, get_narrative}`, `NARRATIVE_PDF_KIND`.
- Produces: MCP tool surface goes 13 → 15 tools; `ingest_record` accepts `kind = "clinical-pdf"`.

- [ ] **Step 1: Args structs**

Add near the other args structs in `server.rs`:

```rust
/// Arguments for the `search_narratives` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct SearchNarrativesArgs {
    /// FTS5 full-text query (e.g. `"biopsy proctitis"`). Omit to list the
    /// full narrative catalog, newest first.
    pub(crate) query: Option<String>,
    /// Maximum results (default 20).
    pub(crate) limit: Option<i64>,
}

/// Arguments for the `get_narrative` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct GetNarrativeArgs {
    /// The narrative's `source_document_id` (from `search_narratives`).
    pub(crate) document_id: i64,
}
```

Update `IngestRecordArgs.kind`'s doc comment to: `/// Document kind: "ccda" or "clinical-pdf".`

- [ ] **Step 2: Route `ingest_record` on kind**

Replace the `if args.kind != "ccda"` guard at the top of `ingest_record` with nothing (delete it), and after the existing `(content, original_filename)` resolution block, replace the unconditional CCDA ingest call with a match:

```rust
        match args.kind.as_str() {
            "ccda" => {
                let source_document_id = chartpds_core::ingestion::ingest(
                    &self.archive,
                    &self.pool,
                    content,
                    &args.kind,
                    &args.source,
                    original_filename.as_deref(),
                    time::OffsetDateTime::now_utc(),
                )
                .await
                .map_err(|err| {
                    McpError::internal_error(format!("ingestion failed: {err}"), None)
                })?;
                let result = serde_json::json!({ "source_document_id": source_document_id });
                let json = serde_json::to_string(&result).map_err(|err| {
                    McpError::internal_error(format!("serializing: {err}"), None)
                })?;
                Ok(CallToolResult::success(vec![Content::text(json)]))
            }
            "clinical-pdf" => {
                let extractor =
                    chartpds_core::extraction::ClaudeExtractor::from_env(self.http_client.clone());
                let outcome = chartpds_core::ingestion::ingest_narrative_pdf(
                    &self.archive,
                    &self.pool,
                    content,
                    &args.source,
                    original_filename.as_deref(),
                    time::OffsetDateTime::now_utc(),
                    extractor.as_ref(),
                )
                .await
                .map_err(|err| {
                    McpError::internal_error(format!("ingestion failed: {err}"), None)
                })?;
                let json = serde_json::to_string(&outcome).map_err(|err| {
                    McpError::internal_error(format!("serializing: {err}"), None)
                })?;
                Ok(CallToolResult::success(vec![Content::text(json)]))
            }
            other => Err(McpError::invalid_params(
                format!("unsupported kind {other:?}; supported: \"ccda\", \"clinical-pdf\""),
                None,
            )),
        }
```

Update the `#[tool(description = ...)]` on `ingest_record` to mention both kinds and the degradation behavior, e.g.:

```
Ingest a medical record document. kind="ccda": CCDA XML (observations, problems, medications). kind="clinical-pdf": a narrative clinical PDF (pathology/imaging report, visit note) — archives the PDF, indexes its text for search_narratives, and (when ANTHROPIC_API_KEY is set) extracts explicitly-quoted ICD-10 codes into problems via a one-time verified LLM pass; without a key it ingests text-only. Returns what was extracted, verified, and dropped.
```

- [ ] **Step 3: Add the two read tools inside the `#[tool_router]` block**

```rust
    #[tool(
        description = "Full-text search over narrative clinical documents (FTS5, BM25-ranked). Args: query? (FTS5 syntax; omit to list the whole narrative catalog newest-first), limit? (default 20). Returns [{source_document_id, title, kind, source, document_date, snippet}]. Pass source_document_id to get_narrative for the full text."
    )]
    async fn search_narratives(
        &self,
        Parameters(args): Parameters<SearchNarrativesArgs>,
    ) -> Result<CallToolResult, McpError> {
        let limit = args.limit.unwrap_or(20);
        let hits = chartpds_core::queries::search_narratives(
            &self.pool,
            args.query.as_deref(),
            limit,
        )
        .await
        .map_err(|err| McpError::invalid_params(format!("search failed: {err}"), None))?;
        let json = serde_json::to_string(&hits)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }

    #[tool(
        description = "Fetch one narrative clinical document: metadata, full extracted text, and the verified codings extracted from it (with their section labels). Args: document_id (a source_document_id from search_narratives)."
    )]
    async fn get_narrative(
        &self,
        Parameters(args): Parameters<GetNarrativeArgs>,
    ) -> Result<CallToolResult, McpError> {
        let detail = chartpds_core::queries::get_narrative(&self.pool, args.document_id)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&detail)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }
```

(`search_narratives` maps DB errors to `invalid_params` because the dominant failure is FTS5 MATCH syntax in the user-supplied query.)

- [ ] **Step 4: Server tests**

Add to the `server.rs` test module, following the existing direct-construction pattern (`fresh_server...` helpers). The PDF fixture is reached from the sibling crate via a relative `include_bytes!` path:

```rust
    const PDF_FIXTURE: &[u8] = include_bytes!(
        "../../chartpds-core/src/extraction/fixtures/synthetic_pathology.pdf"
    );

    #[tokio::test]
    async fn ingest_record_clinical_pdf_ingests_text_only_without_key() {
        // No ANTHROPIC_API_KEY manipulation: from_env may or may not find a
        // key in the developer's environment, so this test writes the PDF to
        // a temp file and only asserts on fields that hold in BOTH the
        // text-only and extraction paths.
        let server = fresh_server().await;
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("synthetic_pathology.pdf");
        std::fs::write(&path, PDF_FIXTURE).expect("write fixture");

        let result = server
            .ingest_record(Parameters(IngestRecordArgs {
                file_path: Some(path.to_string_lossy().into_owned()),
                content: None,
                kind: "clinical-pdf".to_string(),
                source: "manual-upload".to_string(),
                original_filename: None,
            }))
            .await
            .expect("tool call succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert!(value["source_document_id"].as_i64().is_some());
        assert!(value["extraction_status"].is_string());

        // The text is now searchable.
        let search = server
            .search_narratives(Parameters(SearchNarrativesArgs {
                query: Some("proctitis OR dysplasia".to_string()),
                limit: None,
            }))
            .await
            .expect("search");
        let text = match &search.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let hits: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(hits.as_array().expect("array").len(), 1);

        // And retrievable in full.
        let doc_id = value["source_document_id"].as_i64().expect("id");
        let get = server
            .get_narrative(Parameters(GetNarrativeArgs { document_id: doc_id }))
            .await
            .expect("get");
        let text = match &get.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let detail: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert!(detail["text"].as_str().expect("text").contains("DIAGNOSIS"));
    }

    #[tokio::test]
    async fn ingest_record_rejects_unknown_kind() {
        let server = fresh_server().await;
        let err = server
            .ingest_record(Parameters(IngestRecordArgs {
                file_path: None,
                content: Some("whatever".to_string()),
                kind: "hl7v2".to_string(),
                source: "test".to_string(),
                original_filename: None,
            }))
            .await
            .expect_err("unknown kind must be rejected");
        assert!(err.to_string().contains("hl7v2"));
    }
```

**Important caveat for the implementer:** the text-only test above still constructs `ClaudeExtractor::from_env`, and if the developer's shell exports `ANTHROPIC_API_KEY` the tool would attempt a real network call. To keep the test hermetic, gate the extractor construction in `ingest_record` behind an env var check is NOT enough — instead make the test deterministic by clearing the variable for the process under test: at the top of the test add

```rust
        // Hermetic: never let this test reach the network.
        std::env::remove_var("ANTHROPIC_API_KEY");
```

and mark the test `#[tokio::test(flavor = "current_thread")]` — env mutation is process-global, so if other tests race on this variable, serialize them with a `std::sync::Mutex<()>` test lock (define `static ENV_LOCK: Mutex<()> = Mutex::new(());` and hold it in every test that touches the env var). Only this one test touches it today, so the lock can be added lazily if a second one appears. Adjust the surviving assertions to the text-only expectations: `extraction_status == "skipped_no_extractor"`.

If `fresh_server()` does not exist under that name, reuse whatever existing helper constructs a `ChartPdsServer` against a temp pool + in-memory archive (see `fresh_server_with_one_weight` in the existing tests and factor the server-construction part out if needed).

- [ ] **Step 5: Update CLAUDE.md**

- Table count: "The index currently has 9 tables" → "10 tables", adding `narrative_texts` to the ingestion-populated list and noting its FTS5 companion index (`narrative_texts_fts`, trigger-maintained).
- MCP server section: "serves 13 tools" → "serves 15 tools"; extend the `ingest_record` bullet for `clinical-pdf`; add bullets for `search_narratives` and `get_narrative`.
- New top-level section **"Narrative documents"** after "Ingestion", covering: the `clinical-pdf` kind; deterministic text extraction via `pdf-extract`; the one-time Claude extraction (`ANTHROPIC_API_KEY`, model pinned in `extraction/llm.rs`) with mechanical quote-verification; the frozen `narrative-extraction` artifact and why rebuild never calls an LLM; `problems.section_label`; graceful text-only degradation; scanned PDFs unsupported (no OCR).
- "Rebuild index" section: mention `clinical-pdf` replay + artifact application pass, and the new `RebuildResult` fields.
- Module-tree line for `extraction` under the core library description if a module list exists there.

- [ ] **Step 6: Test, gate, commit**

Run: `cargo test -p chartpds-mcp`
Expected: PASS, including the two new tests.

Run: `just check`
Expected: PASS (including holdout — additive tool changes must not break it; if it fails, stop and report).

```bash
git add crates/chartpds-mcp/src/server.rs CLAUDE.md
git commit -m "Serve clinical-pdf ingestion and narrative search/get MCP tools"
```

---

### Task 10: End-to-end verification against the real document (manual, local only)

No repository files change in this task (except possibly bug fixes it uncovers). **Nothing from the real PDF may be committed.**

- [ ] **Step 1: Full gate**

Run: `just check`
Expected: PASS.

- [ ] **Step 2: Live run**

With `ANTHROPIC_API_KEY` exported and a scratch data dir:

```bash
export CHARTPDS_DATA_DIR=$(mktemp -d)
export CHARTPDS_SYNC_INTERVAL_SECS=0
cargo build -p chartpds-mcp
```

Drive the built server over stdio (or via the user's MCP client) and call, in order:

1. `ingest_record` with `{"kind": "clinical-pdf", "source": "manual-upload", "file_path": "/Users/fhwang/Desktop/GI biopsy Apr 21 2026.pdf"}` — expect `extraction_status: "applied"`, `document_date: "2026-04-21"`, `codings_indexed: 3` (K62.5, Z12.11, K64.8), `rejected: []`.
2. `search_narratives` with `{"query": "proctitis"}` — expect 1 hit with a snippet.
3. `get_narrative` with the returned id — expect full text + 3 codings with section labels.
4. `list_problems` — expect the three ICD-10 codes present with `section_labels` populated.
5. `rebuild_index` — expect `narratives_ingested: 1, extractions_applied: 1`; re-run steps 2–4 and confirm identical results (proves the no-LLM replay).

- [ ] **Step 3: Report**

Report the verification transcript (statuses and counts only — no document content) to the user. If extraction quality problems appear (missed codes, rejected-but-valid quotes), fix the prompt in `extraction/llm.rs` (bump `PROMPT_VERSION`) rather than loosening verification.

---

## Self-Review Notes

- **Spec coverage:** archive manifests (Task 6), artifact format + verification rules incl. NBSP normalization (Task 4), index schema + FTS triggers + `section_label` (Tasks 1–2), tool surface 13→15 with `ingest_record` extension (Task 9), two-phase rebuild with newest-artifact supersession (Task 7), LLM client + `ANTHROPIC_API_KEY` + graceful degradation (Tasks 5–6, 9), scanned-PDF error (Tasks 3, 6), re-ingest upsert semantics (Task 6), synthetic-fixture privacy rule (Task 3), FTS5 verification gate (Task 1). Deferred spec items (prose-only findings, embeddings, OCR, normalized section enum) intentionally have no tasks.
- **Type consistency:** `NarrativeIngestOutcome`, `ExtractionArtifact`, `RawExtraction`, `VerifiedExtraction`, `NarrativeSearchHit`, `NarrativeDetail` field names match across producing and consuming tasks; `section_label: Option<&str>` on `InsertProblemParams` everywhere.
- **Known judgment calls an implementer may hit:** exact positions of existing code in `server.rs` may have drifted — the match-on-kind structure is what matters, not line numbers; the `Rfc3339` import in Task 6 is only needed if used — delete if not.
