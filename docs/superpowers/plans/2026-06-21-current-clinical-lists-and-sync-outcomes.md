# P1: Deduped clinical lists + structured sync outcomes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `list_problems`/`list_medications` with deduped, provenance-bearing views, and make `sync_source` return structured per-source outcomes (incl. a new `no_credentials` reason).

**Architecture:** Add a `document_date` column to `source_documents` (populated for all source types) so dedup queries can report real per-document recency. Two new `queries/` primitives dedupe by `(coding_system, coding_code)` and attach provenance; the MCP tools serialize the new envelope. For sync, add a `NoCredentials` error variant and a `reason_code()` method, then have the MCP tool emit a uniform `{results:[…]}` array instead of raising on sync failure.

**Tech Stack:** Rust, sqlx (SQLite, offline/compile-time-checked queries), rmcp (MCP server), `time`, `serde`/`serde_json`, `roxmltree` (CCDA), `just` task runner.

## Global Constraints

- **Lint policy (hard):** Never bypass a lint. No `#[allow(...)]` without a `reason = "…"`. `just lint` runs clippy with `-D warnings`, and `missing_docs` is `warn` → **every `pub` item needs a doc comment**.
- **sqlx offline cache:** After ANY change to `migrations/*.sql` or any `sqlx::query!`/`query_as!`, run `just prepare-sql` and commit the `.sqlx/` changes in the same commit. `just check` runs `cargo sqlx prepare --check`.
- **Migrations are forward-only.** No down migrations, no `*.down.sql`.
- **Module boundaries:** core internals are `pub(crate)`; items the binary calls go through `crates/chartpds-core/src/lib.rs` / module `pub use` re-exports.
- **Completion gate:** `just check` (chains fmt-check, lint, typecheck, test, cargo deny, cargo machete) must pass before any task is "done".
- **Commits:** author as `Francis Hwang <sera@fhwang.net>`. End commit messages with:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
- **Public repo:** keep personal/private-harness specifics out of code and commit messages. Describe the consumer generically ("an external MCP client").
- **sqlx column overrides:** rowid/`REFERENCES` columns infer as `Option`; force with `AS "id!: i64"`. Nullable timestamp/text needs `?:`. CTE/window/JOIN columns often infer as nullable — add `!` overrides for columns known non-null (see each query task).

---

## Task 1: Add `document_date` to `source_documents` (schema + index layer)

Adds the column and threads it through the index CRUD layer. All existing call sites pass `None` for now; Tasks 2–3 fill in real values. This task must end compiling and green.

**Files:**
- Create: `crates/chartpds-core/migrations/0011_source_document_date.sql`
- Modify: `crates/chartpds-core/src/index/source_documents.rs` (struct, params, insert SQL, fetch SQL)
- Modify (add `document_date: None` to each `InsertSourceDocumentParams { … }` literal):
  - `crates/chartpds-core/src/ingestion/ingest.rs:89`
  - `crates/chartpds-core/src/sources/fitbit/storage.rs:129`
  - `crates/chartpds-core/src/sources/oura/storage.rs:108`
  - `crates/chartpds-core/src/index/observations.rs:147`
  - `crates/chartpds-core/src/index/clear.rs:54`
  - `crates/chartpds-core/src/index/medications.rs:149`
  - `crates/chartpds-core/src/index/problems.rs:125`
  - `crates/chartpds-core/src/queries/list_medications.rs:70`
  - `crates/chartpds-core/src/queries/list_problems.rs:66`
  - `crates/chartpds-core/src/queries/test_support.rs:51` and `:117`
  - `crates/chartpds-mcp/src/server.rs:686`, `:1028`, `:1117`

**Interfaces:**
- Produces: `InsertSourceDocumentParams` gains `pub document_date: Option<&'a str>`; `SourceDocument` gains `pub document_date: Option<String>`.

- [ ] **Step 1: Write the migration**

Create `crates/chartpds-core/migrations/0011_source_document_date.sql`:

```sql
-- Add source_documents.document_date: the calendar date the document pertains
-- to (CCDA authored effectiveTime; Fitbit day; Oura sleep day). Distinct from
-- archived_at (when the bytes entered the archive). Nullable: a CCDA may omit
-- effectiveTime. Populated for all source types by ingestion/replay.
--
-- Forward-only per the migration policy; no down migration.
ALTER TABLE source_documents ADD COLUMN document_date TEXT;
```

- [ ] **Step 2: Update the `SourceDocument` struct and `InsertParams`**

In `crates/chartpds-core/src/index/source_documents.rs`, add the field to the struct (after `archived_at`):

```rust
    /// Wall-clock time the blob's bytes first entered the archive. Immutable;
    /// preserved across index rebuilds (sourced from the blob's sidecar
    /// manifest), not stamped at projection time.
    pub archived_at: OffsetDateTime,
    /// The calendar date this document pertains to (`YYYY-MM-DD`): CCDA authored
    /// date, Fitbit day, or Oura sleep day. `None` when unknown. Distinct from
    /// `archived_at`.
    pub document_date: Option<String>,
```

And to `InsertParams` (after `archived_at`):

```rust
    /// Wall-clock time the blob entered the archive (archive-entry time).
    pub archived_at: OffsetDateTime,
    /// The calendar date this document pertains to (`YYYY-MM-DD`), if known.
    pub document_date: Option<&'a str>,
```

- [ ] **Step 3: Update the insert + fetch SQL**

In the same file, change `insert`'s query to include the new column:

```rust
    let row = sqlx::query!(
        r#"
        INSERT INTO source_documents (archive_key, kind, source, original_filename, archived_at, document_date)
        VALUES (?, ?, ?, ?, ?, ?)
        RETURNING id AS "id!: i64"
        "#,
        archive_key,
        params.kind,
        params.source,
        params.original_filename,
        params.archived_at,
        params.document_date,
    )
```

And `fetch_by_archive_key`'s query + mapping:

```rust
    let row = sqlx::query!(
        r#"
        SELECT id AS "id!: i64", archive_key, kind, source, original_filename, archived_at AS "archived_at: OffsetDateTime", document_date
        FROM source_documents
        WHERE archive_key = ?
        "#,
        key_str,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| SourceDocument {
        id: r.id,
        archive_key: BlobKey::from_hex_str(&r.archive_key)
            .expect("archive_key column always contains a valid BlobKey hex"),
        kind: r.kind,
        source: r.source,
        original_filename: r.original_filename,
        archived_at: r.archived_at,
        document_date: r.document_date,
    }))
```

- [ ] **Step 4: Add `document_date: None` to every `InsertSourceDocumentParams`/`InsertParams` literal**

For each call site listed under **Files**, add `document_date: None,` after the `archived_at: …,` line. Example (`ingest.rs:89`):

```rust
        InsertSourceDocumentParams {
            archive_key: &archive_key,
            kind,
            source,
            original_filename,
            archived_at,
            document_date: None,
        },
```

Note `index/source_documents.rs`'s own tests construct `InsertParams { … }` (not the re-exported alias) — add `document_date: None,` there too.

- [ ] **Step 5: Add a round-trip test for the new column**

In `crates/chartpds-core/src/index/source_documents.rs`, add a test that inserts with a date and reads it back. Add near the other tests:

```rust
    #[tokio::test]
    async fn insert_persists_document_date() {
        let pool = fresh_pool().await;
        let archive_key = BlobKey::from_hex_str(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .expect("valid key");
        insert(
            &pool,
            InsertParams {
                archive_key: &archive_key,
                kind: "ccda",
                source: "test",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
                document_date: Some("2026-01-01"),
            },
        )
        .await
        .expect("insert");
        let row = fetch_by_archive_key(&pool, &archive_key)
            .await
            .expect("fetch")
            .expect("row");
        assert_eq!(row.document_date.as_deref(), Some("2026-01-01"));
    }
```

- [ ] **Step 6: Regenerate the sqlx cache**

Run: `just prepare-sql`
Expected: `.sqlx/` updated with the new insert/fetch query hashes; no errors.

- [ ] **Step 7: Run the check gate**

Run: `just check`
Expected: PASS (all crates compile, tests green, lint clean).

- [ ] **Step 8: Commit**

```bash
git add crates/chartpds-core/migrations/0011_source_document_date.sql \
        crates/chartpds-core/src crates/chartpds-mcp/src .sqlx
git commit -m "Add source_documents.document_date column and thread through index layer"
```

---

## Task 2: Populate `document_date` from CCDA `effectiveTime` on ingest

**Files:**
- Modify: `crates/chartpds-core/src/ingestion/ccda/parse.rs` (add `extract_document_date`)
- Modify: `crates/chartpds-core/src/ingestion/ingest.rs` (call it; pass to insert)
- Test: `crates/chartpds-core/src/ingestion/ingest.rs` (assert the date is stored)

**Interfaces:**
- Produces: `pub(crate) fn extract_document_date(doc: &roxmltree::Document<'_>) -> Option<String>` in `ccda::parse`, returning a `YYYY-MM-DD` UTC date string from `ClinicalDocument/effectiveTime@value`, or `None` if absent/unparseable.
- Consumes: `crate::ingestion::ccda::time::parse_hl7_timestamp` (already exists).

- [ ] **Step 1: Write the failing test**

In `crates/chartpds-core/src/ingestion/ingest.rs` tests, add (the `VALID` fixture has `<effectiveTime value="20260101120000+0000"/>` at the `ClinicalDocument` root):

```rust
    #[tokio::test]
    async fn ingest_stores_ccda_document_date() {
        let (pool, archive) = fresh_pool_and_archive().await;
        let id = ingest(
            &archive,
            &pool,
            Bytes::from_static(VALID),
            "ccda",
            "test",
            None,
            OffsetDateTime::now_utc(),
        )
        .await
        .expect("ingest");

        let key = crate::index::fetch_source_document_by_archive_key;
        // Fetch the row via the public index API by listing — simplest is a direct query:
        let row: (Option<String>,) = sqlx::query_as("SELECT document_date FROM source_documents WHERE id = ?")
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("row");
        let _ = key; // silence unused import if not needed
        assert_eq!(row.0.as_deref(), Some("2026-01-01"));
    }
```

> Note: `query_as` with a runtime string is not compile-time checked and needs no `.sqlx` entry, so it is safe for a test assertion here. (Remove the `key`/`_` lines if your editor flags them; they are only a hint that `fetch_source_document_by_archive_key` also works.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p chartpds-core ingest_stores_ccda_document_date -- --nocapture`
Expected: FAIL — `document_date` is `None` (ingest still passes `None`).

- [ ] **Step 3: Add `extract_document_date` to `ccda/parse.rs`**

Append to `crates/chartpds-core/src/ingestion/ccda/parse.rs`:

```rust
use super::time::parse_hl7_timestamp;

/// Extract the document's authored date from `ClinicalDocument/effectiveTime`.
///
/// Returns the UTC calendar date as `YYYY-MM-DD`, or `None` when the element
/// is absent, has no `value`, or the value does not parse as an HL7 timestamp.
/// Used to populate `source_documents.document_date`.
pub(crate) fn extract_document_date(doc: &roxmltree::Document<'_>) -> Option<String> {
    let value = doc
        .root_element()
        .children()
        .find(|n| n.is_element() && n.tag_name().name() == "effectiveTime")?
        .attribute("value")?;
    let ts = parse_hl7_timestamp(value).ok()?;
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    ts.to_offset(time::UtcOffset::UTC).date().format(&fmt).ok()
}
```

If `parse.rs` lacks a `roxmltree`/`time` import path that compiles, mirror the import style already used in the file (the module already parses with `roxmltree`).

- [ ] **Step 4: Call it in `ingest()` and pass to the insert**

In `crates/chartpds-core/src/ingestion/ingest.rs`, after `self_check(&doc)?;` (around line 77) compute the date, and update the import + insert:

Change the import line:

```rust
use crate::ingestion::ccda::parse::{extract_document_date, parse_xml};
```

After `self_check(&doc)?;`:

```rust
    let document_date = extract_document_date(&doc);
```

And in the `InsertSourceDocumentParams { … }` literal, replace `document_date: None,` with:

```rust
            document_date: document_date.as_deref(),
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p chartpds-core ingest_stores_ccda_document_date`
Expected: PASS.

- [ ] **Step 6: Regenerate sqlx cache (test-only query is runtime, but re-run to be safe) and check**

Run: `just prepare-sql && just check`
Expected: PASS. (Rebuild already re-ingests CCDA through `ingest()`, so `document_date` repopulates on rebuild with no extra work.)

- [ ] **Step 7: Commit**

```bash
git add crates/chartpds-core/src .sqlx
git commit -m "Extract and store CCDA document_date from ClinicalDocument/effectiveTime"
```

---

## Task 3: Populate `document_date` for Fitbit and Oura (live + replay)

The per-document day is already in hand in both adapters' shared index-write tail. Pass it through. Replay uses the same tail, so rebuild repopulates automatically.

**Files:**
- Modify: `crates/chartpds-core/src/sources/fitbit/storage.rs` (`index_intraday_day`)
- Modify: `crates/chartpds-core/src/sources/oura/storage.rs` (`index_sleep_session`)
- Test: same two files

**Interfaces:**
- Consumes: `InsertSourceDocumentParams.document_date` (Task 1).

- [ ] **Step 1: Write failing tests**

In `crates/chartpds-core/src/sources/fitbit/storage.rs` tests, extend `ingest_day_archives_and_inserts_observations` (or add a new test) to assert the date. Add a new test:

```rust
    #[tokio::test]
    async fn ingest_day_stores_document_date() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        let result = IntradayResult { samples: vec![], raw_pages: vec![] };
        let id = ingest_day(&archive, &pool, "2026-01-01", &result)
            .await
            .expect("ingest_day");
        let row: (Option<String>,) =
            sqlx::query_as("SELECT document_date FROM source_documents WHERE id = ?")
                .bind(id)
                .fetch_one(&pool)
                .await
                .expect("row");
        assert_eq!(row.0.as_deref(), Some("2026-01-01"));
    }
```

In `crates/chartpds-core/src/sources/oura/storage.rs` tests, add:

```rust
    #[tokio::test]
    async fn ingest_session_stores_document_date() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        let session = OuraSleepSession {
            id: "doc-date-1".to_owned(),
            day: "2026-01-15".to_owned(),
            bedtime_start: "2026-01-14T22:00:00Z".to_owned(),
            bedtime_end: "2026-01-15T06:00:00Z".to_owned(),
            session_type: "long_sleep".to_owned(),
            sleep_phase_5_min: "12".to_owned(),
            total_sleep_duration: None,
            rem_sleep_duration: None,
            deep_sleep_duration: None,
            light_sleep_duration: None,
        };
        let raw = serde_json::json!({"id": "doc-date-1"});
        let id = ingest_session(&archive, &pool, &session, &raw)
            .await
            .expect("ingest");
        let row: (Option<String>,) =
            sqlx::query_as("SELECT document_date FROM source_documents WHERE id = ?")
                .bind(id)
                .fetch_one(&pool)
                .await
                .expect("row");
        assert_eq!(row.0.as_deref(), Some("2026-01-15"));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p chartpds-core ingest_day_stores_document_date ingest_session_stores_document_date`
Expected: FAIL — `document_date` is `None`.

- [ ] **Step 3: Pass the day through in Fitbit `index_intraday_day`**

In `crates/chartpds-core/src/sources/fitbit/storage.rs`, in the `InsertSourceDocumentParams { … }` literal inside `index_intraday_day`, replace `document_date: None,` with:

```rust
            document_date: Some(date),
```

(`date: &str` is already a parameter of `index_intraday_day`.)

- [ ] **Step 4: Pass the day through in Oura `index_sleep_session`**

In `crates/chartpds-core/src/sources/oura/storage.rs`, in the `InsertSourceDocumentParams { … }` literal inside `index_sleep_session`, replace `document_date: None,` with:

```rust
            document_date: Some(&session.day),
```

(`session: &OuraSleepSession` is a parameter; `session.day` is the sleep day.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p chartpds-core ingest_day_stores_document_date ingest_session_stores_document_date`
Expected: PASS.

- [ ] **Step 6: Check gate**

Run: `just check`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/chartpds-core/src
git commit -m "Populate document_date for Fitbit and Oura source documents"
```

---

## Task 4: `current_problems` query + `list_problems` tool (vertical slice)

**Files:**
- Create: `crates/chartpds-core/src/queries/current_problems.rs`
- Modify: `crates/chartpds-core/src/queries/mod.rs` (mod + re-export)
- Modify: `crates/chartpds-mcp/src/server.rs` (rewire `list_problems`, update test)

**Interfaces:**
- Produces:
  - `pub struct CurrentProblem { coding_system: String, coding_code: String, coding_display: Option<String>, status: String, onset_date: Option<String>, document_count: i64, first_seen: Option<String>, last_seen: Option<String> }`
  - `pub struct CurrentProblems { latest_document_date: Option<String>, items: Vec<CurrentProblem> }`
  - `pub async fn current_problems(pool: &SqlitePool) -> Result<CurrentProblems, sqlx::Error>`

- [ ] **Step 1: Write the failing query test**

Create `crates/chartpds-core/src/queries/current_problems.rs` with the test first (and a stub so it compiles after Step 3). For now write the full file including the test; the implementation goes in Step 3. Test body:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::BlobKey;
    use crate::index::{
        insert_problem, insert_source_document, open_pool, InsertProblemParams,
        InsertSourceDocumentParams,
    };
    use time::OffsetDateTime;

    async fn pool() -> sqlx::SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    async fn doc(pool: &sqlx::SqlitePool, hex: &str, date: &str) -> i64 {
        let key = BlobKey::from_hex_str(hex).expect("key");
        insert_source_document(
            pool,
            InsertSourceDocumentParams {
                archive_key: &key,
                kind: "ccda",
                source: "test",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
                document_date: Some(date),
            },
        )
        .await
        .expect("doc")
    }

    #[tokio::test]
    async fn dedupes_and_takes_newest_doc_with_provenance() {
        let pool = pool().await;
        // Older doc: status "active". Newer doc: status "resolved".
        let old = doc(
            &pool,
            "1111111111111111111111111111111111111111111111111111111111111111",
            "2021-01-01",
        )
        .await;
        let new = doc(
            &pool,
            "2222222222222222222222222222222222222222222222222222222222222222",
            "2024-06-01",
        )
        .await;
        for (id, status) in [(old, "active"), (new, "resolved")] {
            insert_problem(
                &pool,
                InsertProblemParams {
                    source_document_id: id,
                    coding_system: "http://snomed.info/sct",
                    coding_code: "44054006",
                    coding_display: Some("Type 2 diabetes mellitus"),
                    status,
                    onset_date: Some("2020-03-15"),
                },
            )
            .await
            .expect("problem");
        }

        let result = current_problems(&pool).await.expect("query");
        assert_eq!(result.latest_document_date.as_deref(), Some("2024-06-01"));
        assert_eq!(result.items.len(), 1);
        let p = &result.items[0];
        assert_eq!(p.coding_code, "44054006");
        assert_eq!(p.status, "resolved"); // winning row = newest document
        assert_eq!(p.document_count, 2);
        assert_eq!(p.first_seen.as_deref(), Some("2021-01-01"));
        assert_eq!(p.last_seen.as_deref(), Some("2024-06-01"));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile/find the fn**

Run: `cargo test -p chartpds-core current_problems`
Expected: FAIL — `current_problems` / types not defined (after Step 3 the module exists).

- [ ] **Step 3: Write the implementation (top of the same file)**

Prepend to `crates/chartpds-core/src/queries/current_problems.rs`:

```rust
//! Deduped "current" problems with provenance.
//!
//! Collapses `problems` rows to one per `(coding_system, coding_code)`, taking
//! source-asserted fields from the most-recent document (max `document_date`,
//! ties broken by max `source_documents.id`; a null date sorts oldest) and
//! attaching provenance (`document_count`, `first_seen`, `last_seen`). The
//! base layer does NOT judge active/resolved — `status` is the raw, unreliable
//! source value; the caller derives currency from provenance.

use sqlx::SqlitePool;

/// One deduped problem with provenance facts.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CurrentProblem {
    /// Coding system URI.
    pub coding_system: String,
    /// Code within the system.
    pub coding_code: String,
    /// Human-readable label from the newest document, if any.
    pub coding_display: Option<String>,
    /// Source-asserted CCDA status from the newest document. UNRELIABLE — do
    /// not treat as active/resolved truth; use provenance to judge currency.
    pub status: String,
    /// Onset date from the newest document, if any.
    pub onset_date: Option<String>,
    /// Number of documents that mention this code.
    pub document_count: i64,
    /// Earliest `document_date` mentioning this code.
    pub first_seen: Option<String>,
    /// Latest `document_date` mentioning this code.
    pub last_seen: Option<String>,
}

/// Deduped current problems plus the newest document date in the archive.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CurrentProblems {
    /// Newest `document_date` across all source documents — the caller's
    /// reference point for judging recency. `None` if the archive is empty.
    pub latest_document_date: Option<String>,
    /// One entry per `(coding_system, coding_code)`.
    pub items: Vec<CurrentProblem>,
}

/// Dedupe problems to a current snapshot with provenance.
///
/// # Errors
///
/// Returns `sqlx::Error` if a query fails.
pub async fn current_problems(pool: &SqlitePool) -> Result<CurrentProblems, sqlx::Error> {
    let latest = sqlx::query!(
        r#"SELECT MAX(document_date) AS "latest?: String" FROM source_documents"#
    )
    .fetch_one(pool)
    .await?
    .latest;

    let rows = sqlx::query!(
        r#"
        WITH ranked AS (
            SELECT p.coding_system, p.coding_code, p.coding_display, p.status,
                   p.onset_date, sd.document_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY p.coding_system, p.coding_code
                       ORDER BY sd.document_date DESC, sd.id DESC
                   ) AS rn
            FROM problems p
            JOIN source_documents sd ON sd.id = p.source_document_id
        ),
        agg AS (
            SELECT p.coding_system, p.coding_code,
                   COUNT(*) AS document_count,
                   MIN(sd.document_date) AS first_seen,
                   MAX(sd.document_date) AS last_seen
            FROM problems p
            JOIN source_documents sd ON sd.id = p.source_document_id
            GROUP BY p.coding_system, p.coding_code
        )
        SELECT r.coding_system AS "coding_system!",
               r.coding_code AS "coding_code!",
               r.coding_display,
               r.status AS "status!",
               r.onset_date,
               a.document_count AS "document_count!: i64",
               a.first_seen AS "first_seen?: String",
               a.last_seen AS "last_seen?: String"
        FROM ranked r
        JOIN agg a ON a.coding_system = r.coding_system AND a.coding_code = r.coding_code
        WHERE r.rn = 1
        ORDER BY r.coding_code
        "#,
    )
    .fetch_all(pool)
    .await?;

    let items = rows
        .into_iter()
        .map(|r| CurrentProblem {
            coding_system: r.coding_system,
            coding_code: r.coding_code,
            coding_display: r.coding_display,
            status: r.status,
            onset_date: r.onset_date,
            document_count: r.document_count,
            first_seen: r.first_seen,
            last_seen: r.last_seen,
        })
        .collect();

    Ok(CurrentProblems {
        latest_document_date: latest,
        items,
    })
}
```

> If `just prepare-sql` reports a column inferred differently than annotated (e.g. `coding_system` already non-null, or `document_count` typed `i32`), adjust the `!`/`?:` overrides per the Global Constraints rule and re-run.

- [ ] **Step 4: Register the module and re-export**

In `crates/chartpds-core/src/queries/mod.rs`, add the `mod` line (alphabetical, after `counts_per_code`):

```rust
mod current_medications;
mod current_problems;
```

(Add `current_medications` now too; Task 5 fills it in — but to avoid a failing `mod` reference, add `current_medications` only in Task 5. For THIS task add only `mod current_problems;`.)

And the re-export:

```rust
pub use current_problems::{current_problems, CurrentProblem, CurrentProblems};
```

- [ ] **Step 5: Regenerate sqlx cache and run the query test**

Run: `just prepare-sql && cargo test -p chartpds-core current_problems`
Expected: PASS.

- [ ] **Step 6: Rewire the `list_problems` MCP tool**

In `crates/chartpds-mcp/src/server.rs`, replace the `list_problems` tool body:

```rust
    #[tool(
        description = "Current problems (diagnoses), deduped to one entry per code. Returns {latest_document_date, items:[{coding_system, coding_code, coding_display, status, onset_date, document_count, first_seen, last_seen}]}. NOTE: `status` is the raw source-asserted value and is UNRELIABLE. To judge whether a problem is current, compare its `last_seen` against `latest_document_date` (a code absent from the newest document is likely resolved)."
    )]
    async fn list_problems(
        &self,
        Parameters(_args): Parameters<ObservationCountsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let result = chartpds_core::queries::current_problems(&self.pool)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&result)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }
```

- [ ] **Step 7: Update the server test for the new shape**

In `crates/chartpds-mcp/src/server.rs` tests, replace the body of `list_problems_returns_ingested_problems` assertions (after fetching `value`):

```rust
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert!(value["latest_document_date"].is_string());
        let arr = value["items"].as_array().expect("expected items array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["coding_code"], "44054006");
        assert_eq!(arr[0]["document_count"], 1);
```

- [ ] **Step 8: Check gate**

Run: `just check`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/chartpds-core/src crates/chartpds-mcp/src .sqlx
git commit -m "Add current_problems query and rewire list_problems tool to deduped+provenance shape"
```

---

## Task 5: `current_medications` query + `list_medications` tool (vertical slice)

Mirrors Task 4, adding `dose`, `route`, `start_date`, `end_date`.

**Files:**
- Create: `crates/chartpds-core/src/queries/current_medications.rs`
- Modify: `crates/chartpds-core/src/queries/mod.rs` (mod + re-export)
- Modify: `crates/chartpds-mcp/src/server.rs` (rewire `list_medications`, update test)

**Interfaces:**
- Produces:
  - `pub struct CurrentMedication { coding_system: String, coding_code: String, coding_display: Option<String>, status: String, dose: Option<String>, route: Option<String>, start_date: Option<String>, end_date: Option<String>, document_count: i64, first_seen: Option<String>, last_seen: Option<String> }`
  - `pub struct CurrentMedications { latest_document_date: Option<String>, items: Vec<CurrentMedication> }`
  - `pub async fn current_medications(pool: &SqlitePool) -> Result<CurrentMedications, sqlx::Error>`

- [ ] **Step 1: Write the failing query test**

Create `crates/chartpds-core/src/queries/current_medications.rs` and add at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::BlobKey;
    use crate::index::{
        insert_medication, insert_source_document, open_pool, InsertMedicationParams,
        InsertSourceDocumentParams,
    };
    use time::OffsetDateTime;

    async fn pool() -> sqlx::SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    async fn doc(pool: &sqlx::SqlitePool, hex: &str, date: &str) -> i64 {
        let key = BlobKey::from_hex_str(hex).expect("key");
        insert_source_document(
            pool,
            InsertSourceDocumentParams {
                archive_key: &key,
                kind: "ccda",
                source: "test",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
                document_date: Some(date),
            },
        )
        .await
        .expect("doc")
    }

    #[tokio::test]
    async fn dedupes_meds_with_provenance_and_newest_fields() {
        let pool = pool().await;
        let old = doc(
            &pool,
            "1111111111111111111111111111111111111111111111111111111111111111",
            "2021-01-01",
        )
        .await;
        let new = doc(
            &pool,
            "2222222222222222222222222222222222222222222222222222222222222222",
            "2024-06-01",
        )
        .await;
        for (id, dose) in [(old, "10 mg"), (new, "20 mg")] {
            insert_medication(
                &pool,
                InsertMedicationParams {
                    source_document_id: id,
                    coding_system: "http://www.nlm.nih.gov/research/umls/rxnorm",
                    coding_code: "617314",
                    coding_display: Some("Atorvastatin 20 MG Oral Tablet"),
                    status: "active",
                    dose: Some(dose),
                    route: Some("oral"),
                    frequency: None,
                    start_date: Some("2021-01-01"),
                    end_date: None,
                },
            )
            .await
            .expect("med");
        }

        let result = current_medications(&pool).await.expect("query");
        assert_eq!(result.latest_document_date.as_deref(), Some("2024-06-01"));
        assert_eq!(result.items.len(), 1);
        let m = &result.items[0];
        assert_eq!(m.coding_code, "617314");
        assert_eq!(m.dose.as_deref(), Some("20 mg")); // newest doc wins
        assert_eq!(m.document_count, 2);
        assert_eq!(m.first_seen.as_deref(), Some("2021-01-01"));
        assert_eq!(m.last_seen.as_deref(), Some("2024-06-01"));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p chartpds-core current_medications`
Expected: FAIL — not defined.

- [ ] **Step 3: Write the implementation (top of the file)**

Prepend:

```rust
//! Deduped "current" medications with provenance.
//!
//! Same model as `current_problems`: one entry per `(coding_system,
//! coding_code)`, source-asserted fields from the newest document, provenance
//! attached. `status` is the raw, unreliable source value.

use sqlx::SqlitePool;

/// One deduped medication with provenance facts.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CurrentMedication {
    /// Coding system URI.
    pub coding_system: String,
    /// Code within the system.
    pub coding_code: String,
    /// Human-readable label from the newest document, if any.
    pub coding_display: Option<String>,
    /// Source-asserted status from the newest document. UNRELIABLE.
    pub status: String,
    /// Dose from the newest document, if any.
    pub dose: Option<String>,
    /// Route from the newest document, if any.
    pub route: Option<String>,
    /// Start date from the newest document, if any.
    pub start_date: Option<String>,
    /// End date from the newest document, if any (a past end_date is a strong
    /// discontinuation signal).
    pub end_date: Option<String>,
    /// Number of documents that mention this code.
    pub document_count: i64,
    /// Earliest `document_date` mentioning this code.
    pub first_seen: Option<String>,
    /// Latest `document_date` mentioning this code.
    pub last_seen: Option<String>,
}

/// Deduped current medications plus the newest document date in the archive.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CurrentMedications {
    /// Newest `document_date` across all source documents. `None` if empty.
    pub latest_document_date: Option<String>,
    /// One entry per `(coding_system, coding_code)`.
    pub items: Vec<CurrentMedication>,
}

/// Dedupe medications to a current snapshot with provenance.
///
/// # Errors
///
/// Returns `sqlx::Error` if a query fails.
pub async fn current_medications(pool: &SqlitePool) -> Result<CurrentMedications, sqlx::Error> {
    let latest = sqlx::query!(
        r#"SELECT MAX(document_date) AS "latest?: String" FROM source_documents"#
    )
    .fetch_one(pool)
    .await?
    .latest;

    let rows = sqlx::query!(
        r#"
        WITH ranked AS (
            SELECT m.coding_system, m.coding_code, m.coding_display, m.status,
                   m.dose, m.route, m.start_date, m.end_date, sd.document_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY m.coding_system, m.coding_code
                       ORDER BY sd.document_date DESC, sd.id DESC
                   ) AS rn
            FROM medications m
            JOIN source_documents sd ON sd.id = m.source_document_id
        ),
        agg AS (
            SELECT m.coding_system, m.coding_code,
                   COUNT(*) AS document_count,
                   MIN(sd.document_date) AS first_seen,
                   MAX(sd.document_date) AS last_seen
            FROM medications m
            JOIN source_documents sd ON sd.id = m.source_document_id
            GROUP BY m.coding_system, m.coding_code
        )
        SELECT r.coding_system AS "coding_system!",
               r.coding_code AS "coding_code!",
               r.coding_display,
               r.status AS "status!",
               r.dose, r.route, r.start_date, r.end_date,
               a.document_count AS "document_count!: i64",
               a.first_seen AS "first_seen?: String",
               a.last_seen AS "last_seen?: String"
        FROM ranked r
        JOIN agg a ON a.coding_system = r.coding_system AND a.coding_code = r.coding_code
        WHERE r.rn = 1
        ORDER BY r.coding_code
        "#,
    )
    .fetch_all(pool)
    .await?;

    let items = rows
        .into_iter()
        .map(|r| CurrentMedication {
            coding_system: r.coding_system,
            coding_code: r.coding_code,
            coding_display: r.coding_display,
            status: r.status,
            dose: r.dose,
            route: r.route,
            start_date: r.start_date,
            end_date: r.end_date,
            document_count: r.document_count,
            first_seen: r.first_seen,
            last_seen: r.last_seen,
        })
        .collect();

    Ok(CurrentMedications {
        latest_document_date: latest,
        items,
    })
}
```

- [ ] **Step 4: Register module + re-export**

In `crates/chartpds-core/src/queries/mod.rs`, add `mod current_medications;` (alphabetical) and:

```rust
pub use current_medications::{current_medications, CurrentMedication, CurrentMedications};
```

- [ ] **Step 5: Regenerate sqlx cache and run the test**

Run: `just prepare-sql && cargo test -p chartpds-core current_medications`
Expected: PASS.

- [ ] **Step 6: Rewire the `list_medications` tool**

In `crates/chartpds-mcp/src/server.rs`, replace the `list_medications` tool body:

```rust
    #[tool(
        description = "Current medications, deduped to one entry per code. Returns {latest_document_date, items:[{coding_system, coding_code, coding_display, status, dose, route, start_date, end_date, document_count, first_seen, last_seen}]}. NOTE: `status` is the raw source-asserted value and is UNRELIABLE. To judge whether a medication is current, compare its `last_seen` against `latest_document_date` and treat a past `end_date` as discontinued."
    )]
    async fn list_medications(
        &self,
        Parameters(_args): Parameters<ObservationCountsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let result = chartpds_core::queries::current_medications(&self.pool)
            .await
            .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
        let json = serde_json::to_string(&result)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }
```

- [ ] **Step 7: Update the server test for the new shape**

In `list_medications_returns_ingested_medications`, replace the assertions after `value`:

```rust
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert!(value["latest_document_date"].is_string());
        let arr = value["items"].as_array().expect("expected items array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["coding_code"], "860975");
        assert_eq!(arr[0]["document_count"], 1);
```

- [ ] **Step 8: Check gate**

Run: `just check`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/chartpds-core/src crates/chartpds-mcp/src .sqlx
git commit -m "Add current_medications query and rewire list_medications tool to deduped+provenance shape"
```

---

## Task 6: `NoCredentials` error variant + `Error::reason_code()`

**Files:**
- Modify: `crates/chartpds-core/src/sources/error.rs` (variant + method + test)
- Modify: `crates/chartpds-core/src/sync/tick.rs` (delegate `error_reason_code` to the method)
- Modify: `crates/chartpds-core/src/sources/fitbit/sync.rs` (missing-creds → `NoCredentials`)

**Interfaces:**
- Produces: `Error::NoCredentials { reason: String }` and `pub fn reason_code(&self) -> &'static str` on `sources::Error`. `reason_code` returns one of `reauth_required | no_credentials | transient | parse_error | archive_error | database_error`.

- [ ] **Step 1: Write the failing test**

In `crates/chartpds-core/src/sources/error.rs`, add a test module at the end:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reason_code_maps_each_variant() {
        assert_eq!(
            Error::ReauthRequired { reason: "x".into() }.reason_code(),
            "reauth_required"
        );
        assert_eq!(
            Error::NoCredentials { reason: "x".into() }.reason_code(),
            "no_credentials"
        );
        assert_eq!(
            Error::Transient { reason: "x".into() }.reason_code(),
            "transient"
        );
        assert_eq!(Error::Parse { reason: "x".into() }.reason_code(), "parse_error");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p chartpds-core reason_code_maps_each_variant`
Expected: FAIL — `NoCredentials` and `reason_code` don't exist.

- [ ] **Step 3: Add the variant and method**

In `crates/chartpds-core/src/sources/error.rs`, add the variant after `ReauthRequired`:

```rust
    /// No credentials are configured for this adapter; the user must connect
    /// it before syncing. Distinct from [`Error::ReauthRequired`], which means
    /// a previously-connected adapter's token expired.
    #[error("no credentials configured: {reason}")]
    NoCredentials {
        /// Human-readable explanation.
        reason: String,
    },
```

And add an impl block (after the enum, before the `From` impls):

```rust
impl Error {
    /// Machine-readable reason code for structured reporting (MCP results and
    /// `source_state.last_error_reason`).
    #[must_use]
    pub fn reason_code(&self) -> &'static str {
        match self {
            Error::ReauthRequired { .. } => "reauth_required",
            Error::NoCredentials { .. } => "no_credentials",
            Error::Transient { .. } => "transient",
            Error::Parse { .. } => "parse_error",
            Error::Archive(_) => "archive_error",
            Error::Database(_) => "database_error",
        }
    }
}
```

- [ ] **Step 4: Delegate `error_reason_code` in `sync/tick.rs`**

In `crates/chartpds-core/src/sync/tick.rs`, replace the body of the private `error_reason_code` (lines ~181–189) with delegation so existing call sites/tests stay valid:

```rust
/// Map an adapter error variant to a machine-readable reason code.
fn error_reason_code(err: &sources::Error) -> &'static str {
    err.reason_code()
}
```

- [ ] **Step 5: Switch Fitbit missing-creds to `NoCredentials`**

In `crates/chartpds-core/src/sources/fitbit/sync.rs`, in `refresh_access_token` change the `ok_or_else` (lines ~31–33):

```rust
        .ok_or_else(|| sources::Error::NoCredentials {
            reason: "no credentials found for fitbit — run the connect flow first".to_owned(),
        })?;
```

- [ ] **Step 6: Run the tests**

Run: `cargo test -p chartpds-core reason_code_maps_each_variant && cargo test -p chartpds-core -- sync::tick`
Expected: PASS. (The daemon still records `reauth_required` for genuine expiry; notifications/`auth_failed` keying is unchanged.)

- [ ] **Step 7: Check gate**

Run: `just check`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add crates/chartpds-core/src
git commit -m "Add NoCredentials error variant and Error::reason_code(); distinguish missing creds from reauth"
```

---

## Task 7: `sync_source` returns a structured `results` array

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs` (`sync_source`, replace `sync_fitbit_inner`/`sync_oura_inner`, drop `result_text`, add tests)

**Interfaces:**
- Consumes: `sources::Error::reason_code()` (Task 6); `sources::{fitbit,oura}::sync::sync_recent_days`.
- Behavior: returns `{ "results": [ {source, ok, …} ] }`. Success: `{source, ok:true, days_synced, total_samples}`. Failure: `{source, ok:false, reason, message}`. Unconfigured single source → one `no_credentials` result. `sync` of all configured sources skips unconfigured ones (empty array if none). An unknown source *name* is still an `McpError` (bad argument, not a sync outcome).

- [ ] **Step 1: Write the failing tests**

In `crates/chartpds-mcp/src/server.rs` tests, add (the empty-db server has `oauth_config: None` and no Oura creds):

```rust
    #[tokio::test]
    async fn sync_source_fitbit_without_credentials_reports_no_credentials() {
        let server = fresh_server_with_empty_db().await;
        let result = server
            .sync_source(Parameters(SyncSourceArgs {
                source: Some("fitbit".to_owned()),
                window_days: None,
            }))
            .await
            .expect("tool call succeeds (failure is in-band)");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        let arr = value["results"].as_array().expect("results array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["source"], "fitbit");
        assert_eq!(arr[0]["ok"], false);
        assert_eq!(arr[0]["reason"], "no_credentials");
    }

    #[tokio::test]
    async fn sync_source_oura_without_credentials_reports_no_credentials() {
        let server = fresh_server_with_empty_db().await;
        let result = server
            .sync_source(Parameters(SyncSourceArgs {
                source: Some("oura".to_owned()),
                window_days: None,
            }))
            .await
            .expect("tool call succeeds (failure is in-band)");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        let arr = value["results"].as_array().expect("results array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["source"], "oura");
        assert_eq!(arr[0]["reason"], "no_credentials");
    }

    #[tokio::test]
    async fn sync_source_all_with_nothing_configured_returns_empty_results() {
        let server = fresh_server_with_empty_db().await;
        let result = server
            .sync_source(Parameters(SyncSourceArgs {
                source: None,
                window_days: None,
            }))
            .await
            .expect("tool call succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["results"].as_array().expect("array").len(), 0);
    }
```

> Note: `OURA_PERSONAL_ACCESS_TOKEN` in the test environment would make the Oura test resolve a token. If CI sets it, the test would attempt a network sync. Guard by clearing it at the top of the Oura test: `std::env::remove_var("OURA_PERSONAL_ACCESS_TOKEN");` (tests run single-threaded per process by default for env safety only if needed — if flakiness appears, gate this test behind serialization).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p chartpds-mcp sync_source_`
Expected: FAIL — current `sync_source` returns `McpError` / old text shape.

- [ ] **Step 3: Replace `sync_fitbit_inner`/`sync_oura_inner` with structured variants**

In `crates/chartpds-mcp/src/server.rs` (the private helpers `impl` block), replace `sync_fitbit_inner` and `sync_oura_inner` with:

```rust
    /// Sync Fitbit and return a per-source structured result object.
    async fn sync_fitbit_structured(&self, window_days: i64) -> serde_json::Value {
        let Some(oauth_config) = self.oauth_config.as_ref() else {
            return serde_json::json!({
                "source": "fitbit",
                "ok": false,
                "reason": "no_credentials",
                "message": "GOOGLE_HEALTH_CLIENT_ID and GOOGLE_HEALTH_CLIENT_SECRET must be set"
            });
        };
        match chartpds_core::sources::fitbit::sync::sync_recent_days(
            &self.archive,
            &self.pool,
            &self.http_client,
            oauth_config,
            window_days,
        )
        .await
        {
            Ok(r) => serde_json::json!({
                "source": "fitbit",
                "ok": true,
                "days_synced": r.days_synced,
                "total_samples": r.total_samples
            }),
            Err(e) => serde_json::json!({
                "source": "fitbit",
                "ok": false,
                "reason": e.reason_code(),
                "message": e.to_string()
            }),
        }
    }

    /// Sync Oura and return a per-source structured result object.
    async fn sync_oura_structured(&self, window_days: i64) -> serde_json::Value {
        let access_token = match self.resolve_oura_token().await {
            Ok(t) => t,
            Err(_) => {
                return serde_json::json!({
                    "source": "oura",
                    "ok": false,
                    "reason": "no_credentials",
                    "message": "No Oura PAT found. Call connect_source with source=\"oura\" first or set OURA_PERSONAL_ACCESS_TOKEN."
                });
            }
        };
        match chartpds_core::sources::oura::sync::sync_recent_days(
            &self.archive,
            &self.pool,
            &self.http_client,
            &access_token,
            window_days,
        )
        .await
        {
            Ok(r) => serde_json::json!({
                "source": "oura",
                "ok": true,
                "days_synced": r.days_synced,
                "total_samples": r.total_samples
            }),
            Err(e) => serde_json::json!({
                "source": "oura",
                "ok": false,
                "reason": e.reason_code(),
                "message": e.to_string()
            }),
        }
    }
```

Delete the now-unused `result_text` helper (cargo machete / clippy `dead_code` will otherwise fail).

- [ ] **Step 4: Rewrite the `sync_source` tool body**

Replace the whole `sync_source` method body:

```rust
    #[tool(
        description = "Sync a data source (or all configured sources). Returns {results:[{source, ok, days_synced?, total_samples?, reason?, message?}]}. A sync failure is reported in-band as ok:false with a reason in {reauth_required, no_credentials, transient, parse_error, archive_error, database_error}; the tool call itself still succeeds so the caller can render against stale data. Syncing all sources skips unconfigured ones."
    )]
    async fn sync_source(
        &self,
        Parameters(args): Parameters<SyncSourceArgs>,
    ) -> Result<CallToolResult, McpError> {
        let window_days = args.window_days.unwrap_or(8);

        let results: Vec<serde_json::Value> = match args.source.as_deref() {
            Some("fitbit") => vec![self.sync_fitbit_structured(window_days).await],
            Some("oura") => vec![self.sync_oura_structured(window_days).await],
            Some(other) => {
                return Err(McpError::invalid_params(
                    format!("unknown source {other:?}; known sources: fitbit, oura"),
                    None,
                ))
            }
            None => {
                let mut out = Vec::new();
                if self.oauth_config.is_some() {
                    out.push(self.sync_fitbit_structured(window_days).await);
                }
                if self.resolve_oura_token().await.is_ok() {
                    out.push(self.sync_oura_structured(window_days).await);
                }
                out
            }
        };

        let payload = serde_json::json!({ "results": results });
        let text = serde_json::to_string(&payload)
            .map_err(|err| McpError::internal_error(format!("serializing result: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }
```

- [ ] **Step 5: Run the new tests**

Run: `cargo test -p chartpds-mcp sync_source_`
Expected: PASS. Also confirm the pre-existing `sync_source_unknown_returns_error` still passes (unknown source name still raises `McpError`).

- [ ] **Step 6: Check gate**

Run: `just check`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/chartpds-mcp/src
git commit -m "Return structured per-source results from sync_source tool"
```

---

## Task 8: Update CLAUDE.md tool descriptions

Keep the architecture doc accurate (it enumerates the tools and their contracts).

**Files:**
- Modify: `CLAUDE.md` (MCP server tool list)

- [ ] **Step 1: Update the tool descriptions**

In `CLAUDE.md`, under "MCP server", update the three bullets to reflect new behavior:

```markdown
- `list_problems` — current problems (diagnoses), deduped to one entry per
  code with provenance (`document_count`, `first_seen`, `last_seen`) and the
  archive's `latest_document_date`; `status` is the raw source value and is
  unreliable for active/resolved
- `list_medications` — current medications, deduped per code with the same
  provenance shape
- `sync_source` — sync one or all sources; returns `{results:[{source, ok,
  days_synced?, total_samples?, reason?, message?}]}` with failures reported
  in-band (reason ∈ reauth_required, no_credentials, transient, parse_error,
  archive_error, database_error)
```

Also update the "Confidence tracking" / sources prose only if needed (no behavior change there). Add a line under "Queries" noting `current_problems` / `current_medications` were added.

- [ ] **Step 2: Check gate**

Run: `just check`
Expected: PASS (docs change only).

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "Document deduped clinical-list tools and structured sync_source results"
```

---

## Self-Review (completed by plan author)

**Spec coverage:**
- Part 1 §1.1 `document_date` column → Task 1. §1.2 populate (CCDA) → Task 2; (Fitbit/Oura + replay) → Task 3. §1.3 query primitives → Tasks 4, 5. §1.4 tool surface → Tasks 4, 5. §1.5 sqlx/verify → each task runs `just prepare-sql`/`just check`.
- Part 2 §2.1 `NoCredentials` + `reason_code` + fitbit/oura/tick → Task 6 (fitbit + tick) and Task 7 (oura no-creds handled at the server boundary, where the token is resolved; the in-adapter Oura path has no creds lookup). §2.2 structured `sync_source` → Task 7. §2.3 verify → Task 7 `just check`.
- Tie-break / null-date ordering from the spec → encoded in the `ORDER BY sd.document_date DESC, sd.id DESC` (SQLite sorts NULL last under DESC) in Tasks 4/5.
- Docs → Task 8.

**Placeholder scan:** none — every code step contains full content.

**Type consistency:** `CurrentProblem(s)` / `CurrentMedication(s)` field names match between query structs (Tasks 4/5) and server serialization (which serializes the struct directly). `Error::reason_code()` defined in Task 6 is consumed in Task 7. `InsertSourceDocumentParams.document_date` field name consistent across Tasks 1–5.

**Note on Oura `no_credentials`:** Oura's adapter `sync_recent_days` takes the token as an argument and has no internal credentials lookup, so the "not configured" case is detected at the server (`resolve_oura_token`) and mapped to `no_credentials` there (Task 7) — not via a `sources::Error`. This is intentional and matches the existing code structure; no in-adapter Oura change is needed.
