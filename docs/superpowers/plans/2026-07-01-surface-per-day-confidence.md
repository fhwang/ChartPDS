# Surface per-day confidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface each day's `confirmed`/`provisional` confidence in ChartPDS's four day-oriented query/tool responses so consumers can tell settled data from data that may still change.

**Architecture:** A new pure-ish core resolver (`queries/day_confidence.rs`) maps `(source, replay-day)` keys to `DayConfidence` by reading `source_state`/`source_day_state` and dispatching to the existing per-adapter confidence functions. The four day-oriented query functions gain a `now` parameter and fold confidence into their results — observations via a flattened `confidence` field, buckets via a conservative roll-up (`provisional` if any contributing source-day is provisional). CCDA / non-wearable / null-replay-day data is `confirmed` by policy.

**Tech Stack:** Rust, sqlx (SQLite, offline mode), `time`, serde, `rmcp` (MCP), tokio.

## Global Constraints

- Run `just check` before declaring any change complete (fmt-check, lint, typecheck, test, cargo deny, cargo machete, sqlx prepare --check, holdout-verify).
- Never bypass a lint. Every `pub` item needs a doc comment (`missing_docs` is promoted to error). `#[allow(...)]` requires a `reason = "..."`.
- After any change to `sqlx::query!` / migrations, run `just prepare-sql` and commit `.sqlx/` in the same commit.
- Default cross-module visibility is `pub(crate)`; items the binary calls go through `lib.rs` / module `pub use` re-exports.
- Confidence is additive at the JSON level: existing fields keep their names/positions.
- Protected paths (`holdout/**`, `holdout.lock`, `.github/allowed_signers`, `.github/workflows/holdout.yml`): a NEW holdout test may be drafted and left staged-but-uncommitted for a human `just holdout-bless`. Never run `just holdout-bless`; never weaken an existing holdout test.
- `DayConfidence` serializes lowercase: `"confirmed"` / `"provisional"`.
- Confidence day-key is `source_documents.document_date` (the source's replay day), NOT `observation.effective_start`.

---

### Task 1: Confidence resolver + lowercase serialization

**Files:**
- Modify: `crates/chartpds-core/src/sources/confidence.rs` (add serde rename to `DayConfidence`)
- Create: `crates/chartpds-core/src/queries/day_confidence.rs`
- Modify: `crates/chartpds-core/src/queries/mod.rs` (mod + re-export)

**Interfaces:**
- Consumes: `crate::sources::DayConfidence`; `crate::sources::fitbit::confidence::fitbit_day_confidence`; `crate::sources::oura::confidence::oura_day_confidence`; `crate::index::{get_source_state, get_source_day_state, upsert_source_state, upsert_source_day_state, UpsertSourceStateParams, UpsertSourceDayStateParams, open_pool}`.
- Produces: `pub async fn resolve_source_day_confidence(pool: &SqlitePool, now: OffsetDateTime, keys: &[(String, String)]) -> Result<HashMap<(String, String), DayConfidence>, sqlx::Error>` — each key is `(source, YYYY-MM-DD replay day)`.

- [ ] **Step 1: Make `DayConfidence` serialize lowercase**

In `crates/chartpds-core/src/sources/confidence.rs`, change the derive line above `pub enum DayConfidence`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DayConfidence {
```

- [ ] **Step 2: Write the failing resolver tests**

Create `crates/chartpds-core/src/queries/day_confidence.rs` with only the tests first (module + imports will be filled in Step 4). Paste this whole file:

```rust
//! Resolve per-`(source, replay-day)` data confidence from the index.
//!
//! This is the single place that reads `source_state` / `source_day_state`
//! and dispatches to the pure per-adapter confidence functions. It has no
//! wall clock of its own — `now` is always injected so callers (and tests)
//! stay deterministic. The day key is the source's replay day
//! (`source_documents.document_date`), never the observation timestamp.

use std::collections::HashMap;

use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::index::{get_source_day_state, get_source_state};
use crate::sources::fitbit::confidence::fitbit_day_confidence;
use crate::sources::oura::confidence::oura_day_confidence;
use crate::sources::DayConfidence;

/// Format an `OffsetDateTime`'s calendar date as `YYYY-MM-DD`.
fn ymd(dt: OffsetDateTime) -> String {
    format!("{:04}-{:02}-{:02}", dt.year(), u8::from(dt.month()), dt.day())
}

/// Resolve confidence for a set of `(source, replay-day)` keys.
///
/// Dispatches per source: `fitbit` uses the stability-based rule (frontier +
/// day-state), `oura` uses the time-based rule, and any other source is
/// `Confirmed` by policy (a finalized clinical document does not accrete
/// data). Keys for sources with no meaningful confidence model should simply
/// not be passed; if they are, they resolve to `Confirmed`.
///
/// # Errors
///
/// Returns `sqlx::Error` if reading `source_state` / `source_day_state` fails.
pub async fn resolve_source_day_confidence(
    pool: &SqlitePool,
    now: OffsetDateTime,
    keys: &[(String, String)],
) -> Result<HashMap<(String, String), DayConfidence>, sqlx::Error> {
    let today = ymd(now);
    let mut frontier_cache: HashMap<String, Option<String>> = HashMap::new();
    let mut out = HashMap::new();

    for (source, date) in keys {
        let confidence = match source.as_str() {
            "fitbit" => {
                if !frontier_cache.contains_key(source) {
                    let frontier = get_source_state(pool, source)
                        .await?
                        .and_then(|s| s.freshness_frontier_at);
                    frontier_cache.insert(source.clone(), frontier);
                }
                let frontier = frontier_cache
                    .get(source)
                    .and_then(std::option::Option::as_deref);
                let day_state = get_source_day_state(pool, source, date).await?;
                fitbit_day_confidence(&today, date, frontier, day_state.as_ref())
            }
            "oura" => oura_day_confidence(now, date),
            _ => DayConfidence::Confirmed,
        };
        out.insert((source.clone(), date.clone()), confidence);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{
        open_pool, upsert_source_day_state, upsert_source_state, UpsertSourceDayStateParams,
        UpsertSourceStateParams,
    };
    use time::macros::datetime;

    async fn pool() -> SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    async fn set_frontier(pool: &SqlitePool, source: &str, frontier: &str) {
        upsert_source_state(
            pool,
            UpsertSourceStateParams {
                source_name: source,
                last_sync_at: None,
                last_sync_status: None,
                last_error_message: None,
                last_error_reason: None,
                last_synced_window_end: None,
                freshness_frontier_at: Some(frontier),
                frontier_last_advanced_at: None,
                consecutive_sync_failures: 0,
            },
        )
        .await
        .expect("upsert source_state");
    }

    async fn set_day_state(pool: &SqlitePool, source: &str, date: &str, count: i64, prev: Option<i64>) {
        upsert_source_day_state(
            pool,
            UpsertSourceDayStateParams {
                source_name: source,
                date,
                samples_count: count,
                samples_count_prev: prev,
                last_pulled_at: "2026-01-11T00:00:00Z",
            },
        )
        .await
        .expect("upsert source_day_state");
    }

    #[tokio::test]
    async fn fitbit_old_stable_frontier_past_is_confirmed() {
        let pool = pool().await;
        set_frontier(&pool, "fitbit", "2026-01-12T12:00:00Z").await;
        set_day_state(&pool, "fitbit", "2026-01-10", 100, Some(100)).await;
        let keys = [("fitbit".to_owned(), "2026-01-10".to_owned())];
        let out = resolve_source_day_confidence(&pool, datetime!(2026-01-20 00:00:00 UTC), &keys)
            .await
            .expect("resolve");
        assert_eq!(
            out[&("fitbit".to_owned(), "2026-01-10".to_owned())],
            DayConfidence::Confirmed
        );
    }

    #[tokio::test]
    async fn fitbit_no_frontier_is_provisional() {
        let pool = pool().await;
        set_day_state(&pool, "fitbit", "2026-01-10", 100, Some(100)).await;
        let keys = [("fitbit".to_owned(), "2026-01-10".to_owned())];
        let out = resolve_source_day_confidence(&pool, datetime!(2026-01-20 00:00:00 UTC), &keys)
            .await
            .expect("resolve");
        assert_eq!(
            out[&("fitbit".to_owned(), "2026-01-10".to_owned())],
            DayConfidence::Provisional
        );
    }

    #[tokio::test]
    async fn oura_old_day_is_confirmed_recent_is_provisional() {
        let pool = pool().await;
        let keys = [
            ("oura".to_owned(), "2026-01-10".to_owned()),
            ("oura".to_owned(), "2026-01-20".to_owned()),
        ];
        let out = resolve_source_day_confidence(&pool, datetime!(2026-01-20 12:00:00 UTC), &keys)
            .await
            .expect("resolve");
        assert_eq!(
            out[&("oura".to_owned(), "2026-01-10".to_owned())],
            DayConfidence::Confirmed
        );
        assert_eq!(
            out[&("oura".to_owned(), "2026-01-20".to_owned())],
            DayConfidence::Provisional
        );
    }

    #[tokio::test]
    async fn unknown_source_is_confirmed_by_policy() {
        let pool = pool().await;
        let keys = [("epic".to_owned(), "2026-01-10".to_owned())];
        let out = resolve_source_day_confidence(&pool, datetime!(2026-01-20 00:00:00 UTC), &keys)
            .await
            .expect("resolve");
        assert_eq!(
            out[&("epic".to_owned(), "2026-01-10".to_owned())],
            DayConfidence::Confirmed
        );
    }
}
```

- [ ] **Step 3: Wire the module**

In `crates/chartpds-core/src/queries/mod.rs`, add the `mod` line in alphabetical position (after `mod current_problems;`):

```rust
mod day_confidence;
```

and add the re-export (after the `pub use current_problems::...` line):

```rust
pub use day_confidence::resolve_source_day_confidence;
```

- [ ] **Step 4: Run the resolver tests**

Run: `cargo test -p chartpds-core --lib queries::day_confidence`
Expected: PASS (4 tests). The implementation is already in the file from Step 2.

- [ ] **Step 5: Full gate + commit**

Run: `just check`
Expected: PASS.

```bash
git add crates/chartpds-core/src/sources/confidence.rs crates/chartpds-core/src/queries/day_confidence.rs crates/chartpds-core/src/queries/mod.rs
git commit -m "Add per-day confidence resolver over the index (#15)"
```

---

### Task 2: Source-document lookup + observation/bucket annotation helpers

**Files:**
- Modify: `crates/chartpds-core/src/index/source_documents.rs` (add `get_by_id` + tests)
- Modify: `crates/chartpds-core/src/index/mod.rs` (re-export `get_by_id`)
- Modify: `crates/chartpds-core/src/queries/day_confidence.rs` (add `ObservationWithConfidence`, `annotate_observations`, `roll_up_bucket_confidence`)
- Modify: `crates/chartpds-core/src/queries/mod.rs` (re-export the new items)

**Interfaces:**
- Consumes: `resolve_source_day_confidence` (Task 1); `crate::index::{Observation, SourceDocument}`.
- Produces:
  - `crate::index::get_source_document_by_id(pool, id: i64) -> Result<Option<SourceDocument>, sqlx::Error>`
  - `pub struct ObservationWithConfidence { #[serde(flatten)] pub observation: Observation, pub confidence: DayConfidence }`
  - `pub async fn annotate_observations(pool, now: OffsetDateTime, observations: Vec<Observation>) -> Result<Vec<ObservationWithConfidence>, sqlx::Error>`
  - `pub async fn roll_up_bucket_confidence(pool, now: OffsetDateTime, contributions: &[(String, String, Option<String>)]) -> Result<HashMap<String, DayConfidence>, sqlx::Error>` — each contribution is `(bucket_day, source, document_date)`; returns `bucket_day -> confidence`.

- [ ] **Step 1: Write the failing `get_by_id` test**

In `crates/chartpds-core/src/index/source_documents.rs`, inside the existing `#[cfg(test)] mod tests { ... }`, add:

```rust
#[tokio::test]
async fn get_by_id_returns_inserted_document() {
    let pool = crate::index::test_pool().await;
    let archive_key =
        BlobKey::from_hex_str("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .expect("valid key");
    let id = insert(
        &pool,
        InsertParams {
            archive_key: &archive_key,
            kind: "fitbit",
            source: "fitbit",
            original_filename: None,
            archived_at: OffsetDateTime::now_utc(),
            document_date: Some("2026-01-10"),
        },
    )
    .await
    .expect("insert");

    let doc = get_by_id(&pool, id).await.expect("query").expect("row present");
    assert_eq!(doc.source, "fitbit");
    assert_eq!(doc.document_date.as_deref(), Some("2026-01-10"));
    assert!(get_by_id(&pool, id + 999).await.expect("query").is_none());
}
```

If the existing tests in this file use a different pool constructor than `crate::index::test_pool()`, mirror whatever the sibling tests in this same file already use (open a look at the top of the `mod tests` block); use that exact constructor instead.

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p chartpds-core --lib index::source_documents::tests::get_by_id_returns_inserted_document`
Expected: FAIL — `cannot find function get_by_id`.

- [ ] **Step 3: Implement `get_by_id`**

In `crates/chartpds-core/src/index/source_documents.rs`, after `fetch_by_archive_key`, add:

```rust
/// Fetch a single `source_documents` row by its auto-increment id.
///
/// Returns `Ok(None)` if no row has that id.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails for any reason other than the row
/// being absent.
///
/// # Panics
///
/// Panics if the stored `archive_key` is not valid `BlobKey` hex — an
/// invariant of the table, so a panic indicates schema corruption.
pub async fn get_by_id(
    pool: &SqlitePool,
    id: i64,
) -> Result<Option<SourceDocument>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT id AS "id!: i64", archive_key, kind, source, original_filename, archived_at AS "archived_at: OffsetDateTime", document_date
        FROM source_documents
        WHERE id = ?
        "#,
        id,
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
}
```

- [ ] **Step 4: Re-export `get_by_id`**

In `crates/chartpds-core/src/index/mod.rs`, extend the `source_documents` re-export block to include the new function under a clear alias:

```rust
pub use source_documents::{
    fetch_by_archive_key as fetch_source_document_by_archive_key,
    get_by_id as get_source_document_by_id, insert as insert_source_document,
    insert_superseding as insert_source_document_superseding,
    InsertParams as InsertSourceDocumentParams, SourceDocument, SupersedeOutcome,
};
```

- [ ] **Step 5: Regenerate the SQL cache**

Run: `just prepare-sql`
Expected: writes/updates a `.sqlx/query-*.json` for the new `get_by_id` query.

- [ ] **Step 6: Run the `get_by_id` test**

Run: `cargo test -p chartpds-core --lib index::source_documents::tests::get_by_id_returns_inserted_document`
Expected: PASS.

- [ ] **Step 7: Write failing annotation-helper tests**

In `crates/chartpds-core/src/queries/day_confidence.rs`, add to the `#[cfg(test)] mod tests`:

```rust
use crate::index::{insert_observation, insert_source_document, InsertObservationParams, InsertSourceDocumentParams};
use crate::archive::BlobKey;

async fn seed_doc(pool: &SqlitePool, source: &str, document_date: Option<&str>, hex: &str) -> i64 {
    let key = BlobKey::from_hex_str(hex).expect("valid key");
    insert_source_document(
        pool,
        InsertSourceDocumentParams {
            archive_key: &key,
            kind: "test",
            source,
            original_filename: None,
            archived_at: OffsetDateTime::now_utc(),
            document_date,
        },
    )
    .await
    .expect("insert doc")
}

async fn seed_obs(pool: &SqlitePool, doc_id: i64, start: OffsetDateTime) -> crate::index::Observation {
    let id = insert_observation(
        pool,
        InsertObservationParams {
            source_document_id: doc_id,
            coding_system: "http://loinc.org",
            coding_code: "8867-4",
            coding_display: None,
            effective_start: start,
            effective_end: None,
            value_quantity: Some(72.0),
            value_string: None,
            value_unit: None,
        },
    )
    .await
    .expect("insert obs");
    crate::index::Observation {
        id,
        source_document_id: doc_id,
        coding_system: "http://loinc.org".to_owned(),
        coding_code: "8867-4".to_owned(),
        coding_display: None,
        effective_start: start,
        effective_end: None,
        value_quantity: Some(72.0),
        value_string: None,
        value_unit: None,
    }
}

#[tokio::test]
async fn annotate_marks_fitbit_no_frontier_provisional_ccda_confirmed() {
    let pool = pool().await;
    let fitbit_doc = seed_doc(&pool, "fitbit", Some("2026-01-10"),
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").await;
    let ccda_doc = seed_doc(&pool, "epic", None,
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").await;
    let o1 = seed_obs(&pool, fitbit_doc, datetime!(2026-01-10 08:00:00 UTC)).await;
    let o2 = seed_obs(&pool, ccda_doc, datetime!(2026-01-10 09:00:00 UTC)).await;

    let annotated = annotate_observations(&pool, datetime!(2026-01-20 00:00:00 UTC), vec![o1, o2])
        .await
        .expect("annotate");

    assert_eq!(annotated[0].confidence, DayConfidence::Provisional);
    assert_eq!(annotated[1].confidence, DayConfidence::Confirmed);
}

#[tokio::test]
async fn roll_up_bucket_is_provisional_if_any_contributor_provisional() {
    let pool = pool().await;
    // No frontier for fitbit → its day is provisional; ccda day is confirmed.
    let contributions = vec![
        ("2026-01-10".to_owned(), "epic".to_owned(), None),
        ("2026-01-10".to_owned(), "fitbit".to_owned(), Some("2026-01-10".to_owned())),
        ("2026-01-11".to_owned(), "epic".to_owned(), None),
    ];
    let map = roll_up_bucket_confidence(&pool, datetime!(2026-01-20 00:00:00 UTC), &contributions)
        .await
        .expect("roll up");
    assert_eq!(map["2026-01-10"], DayConfidence::Provisional);
    assert_eq!(map["2026-01-11"], DayConfidence::Confirmed);
}
```

- [ ] **Step 8: Run to confirm they fail**

Run: `cargo test -p chartpds-core --lib queries::day_confidence`
Expected: FAIL — `ObservationWithConfidence`, `annotate_observations`, `roll_up_bucket_confidence` not found.

- [ ] **Step 9: Implement the annotation helpers**

In `crates/chartpds-core/src/queries/day_confidence.rs`, add to the top-level imports:

```rust
use crate::index::{get_source_document_by_id, Observation};
```

and add these items above the `#[cfg(test)]` block:

```rust
/// An observation paired with its day's confidence, serialized flat.
///
/// `#[serde(flatten)]` keeps every existing `Observation` field at the top
/// level and adds `confidence` as a sibling key, so the JSON stays additive.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ObservationWithConfidence {
    /// The underlying observation.
    #[serde(flatten)]
    pub observation: Observation,
    /// Confidence of the observation's source-day.
    pub confidence: DayConfidence,
}

/// Confidence for a document's `(source, document_date)`, applying policy:
/// a missing replay day or a non-wearable source is `Confirmed`.
fn confidence_for_doc(
    source: &str,
    document_date: Option<&str>,
    resolved: &HashMap<(String, String), DayConfidence>,
) -> DayConfidence {
    match document_date {
        Some(date) if source == "fitbit" || source == "oura" => resolved
            .get(&(source.to_owned(), date.to_owned()))
            .copied()
            .unwrap_or(DayConfidence::Confirmed),
        _ => DayConfidence::Confirmed,
    }
}

/// Collect the distinct wearable `(source, replay-day)` keys from a set of
/// `(source, document_date)` pairs (skipping `None` dates and non-wearables).
fn wearable_keys<'a, I>(pairs: I) -> Vec<(String, String)>
where
    I: IntoIterator<Item = (&'a str, Option<&'a str>)>,
{
    let mut keys: Vec<(String, String)> = pairs
        .into_iter()
        .filter_map(|(source, date)| match date {
            Some(d) if source == "fitbit" || source == "oura" => {
                Some((source.to_owned(), d.to_owned()))
            }
            _ => None,
        })
        .collect();
    keys.sort();
    keys.dedup();
    keys
}

/// Attach per-day confidence to a list of observations.
///
/// Looks up each observation's document `(source, document_date)`, resolves
/// the wearable source-days once, and maps every observation to its
/// confidence (CCDA / non-wearable / null replay-day → `Confirmed`).
///
/// # Errors
///
/// Returns `sqlx::Error` if any index read fails.
pub async fn annotate_observations(
    pool: &SqlitePool,
    now: OffsetDateTime,
    observations: Vec<Observation>,
) -> Result<Vec<ObservationWithConfidence>, sqlx::Error> {
    let mut doc_meta: HashMap<i64, (String, Option<String>)> = HashMap::new();
    for obs in &observations {
        if !doc_meta.contains_key(&obs.source_document_id) {
            if let Some(doc) = get_source_document_by_id(pool, obs.source_document_id).await? {
                doc_meta.insert(obs.source_document_id, (doc.source, doc.document_date));
            }
        }
    }

    let keys = wearable_keys(
        doc_meta
            .values()
            .map(|(source, date)| (source.as_str(), date.as_deref())),
    );
    let resolved = resolve_source_day_confidence(pool, now, &keys).await?;

    Ok(observations
        .into_iter()
        .map(|obs| {
            let confidence = match doc_meta.get(&obs.source_document_id) {
                Some((source, date)) => confidence_for_doc(source, date.as_deref(), &resolved),
                None => DayConfidence::Confirmed,
            };
            ObservationWithConfidence {
                observation: obs,
                confidence,
            }
        })
        .collect())
}

/// Roll up per-bucket confidence from `(bucket_day, source, document_date)`
/// contributions.
///
/// A bucket is `Provisional` if ANY contributing source-day is provisional;
/// otherwise `Confirmed`. The confidence day-key is the source's replay day
/// (`document_date`), while the bucket key is the observation's UTC calendar
/// day — for a midnight-crossing run these can differ, so the roll-up may
/// flag a bucket based on a neighboring source-day. This is conservative
/// (over-flags toward `provisional`, never under-flags) and intentional.
///
/// # Errors
///
/// Returns `sqlx::Error` if resolving source-day confidence fails.
pub async fn roll_up_bucket_confidence(
    pool: &SqlitePool,
    now: OffsetDateTime,
    contributions: &[(String, String, Option<String>)],
) -> Result<HashMap<String, DayConfidence>, sqlx::Error> {
    let keys = wearable_keys(
        contributions
            .iter()
            .map(|(_, source, date)| (source.as_str(), date.as_deref())),
    );
    let resolved = resolve_source_day_confidence(pool, now, &keys).await?;

    let mut out: HashMap<String, DayConfidence> = HashMap::new();
    for (bucket, source, date) in contributions {
        let confidence = confidence_for_doc(source, date.as_deref(), &resolved);
        let entry = out.entry(bucket.clone()).or_insert(DayConfidence::Confirmed);
        if confidence == DayConfidence::Provisional {
            *entry = DayConfidence::Provisional;
        }
    }
    Ok(out)
}
```

- [ ] **Step 10: Re-export the new items**

In `crates/chartpds-core/src/queries/mod.rs`, replace the Task-1 re-export line with:

```rust
pub use day_confidence::{
    annotate_observations, resolve_source_day_confidence, roll_up_bucket_confidence,
    ObservationWithConfidence,
};
```

- [ ] **Step 11: Run the annotation tests**

Run: `cargo test -p chartpds-core --lib queries::day_confidence`
Expected: PASS (all resolver + annotation tests).

- [ ] **Step 12: Full gate + commit**

Run: `just check`
Expected: PASS.

```bash
git add crates/chartpds-core/src/index/source_documents.rs crates/chartpds-core/src/index/mod.rs crates/chartpds-core/src/queries/day_confidence.rs crates/chartpds-core/src/queries/mod.rs .sqlx
git commit -m "Add observation and bucket confidence annotation helpers (#15)"
```

---

### Task 3: Fold confidence into `observation_history` + `latest_by_code`

**Files:**
- Modify: `crates/chartpds-core/src/queries/observation_history.rs` (add `now`, return annotated, update tests)
- Modify: `crates/chartpds-core/src/queries/latest_by_code.rs` (add `now`, return annotated, update tests)
- Modify: `crates/chartpds-mcp/src/server.rs` (pass `now`, at lines ~165 and ~206)

**Interfaces:**
- Consumes: `annotate_observations`, `ObservationWithConfidence` (Task 2).
- Produces:
  - `observation_history(pool, now: OffsetDateTime, codings: &[CodingKey], since, until) -> Result<Vec<ObservationWithConfidence>, sqlx::Error>`
  - `latest_by_code(pool, now: OffsetDateTime, code: &str) -> Result<Option<ObservationWithConfidence>, sqlx::Error>`

- [ ] **Step 1: Update `observation_history` signature + body**

In `crates/chartpds-core/src/queries/observation_history.rs`:

Add the import near the top:

```rust
use crate::queries::{annotate_observations, ObservationWithConfidence};
```

Change the signature to insert `now` after `pool` and change the return type:

```rust
pub async fn observation_history(
    pool: &SqlitePool,
    now: OffsetDateTime,
    codings: &[CodingKey<'_>],
    since: Option<OffsetDateTime>,
    until: Option<OffsetDateTime>,
) -> Result<Vec<ObservationWithConfidence>, sqlx::Error> {
```

At the end of the function, after the `out.sort_by(...)` call, replace `Ok(out)` with:

```rust
    annotate_observations(pool, now, out).await
}
```

Update the doc comment's final `# Errors` paragraph is unchanged; add one sentence to the top doc comment: `Each observation carries its source-day \`confidence\`.`

- [ ] **Step 2: Update `observation_history` tests**

In the same file's `#[cfg(test)] mod tests`, each call now passes `now` and reads through `.observation`. Update the four tests:

- `empty_codings_returns_empty`: change the call to
  `observation_history(&pool, datetime!(2026-06-01 00:00:00 UTC), &[], None, None)`.
- `multi_coding_full_history_ordered_by_system_code_time`: pass `datetime!(2026-06-01 00:00:00 UTC)` as the second arg; change assertions `rows[0].coding_system` → `rows[0].observation.coding_system`, `rows[0].effective_start` → `rows[0].observation.effective_start`, and likewise for indices 1 and 2.
- `since_only_is_open_ended_upper` and `until_only_is_open_ended_lower_and_exclusive`: pass `datetime!(2026-06-01 00:00:00 UTC)` as the second arg; change `rows[0].effective_start` → `rows[0].observation.effective_start`.

The seed uses `source: "test"`, `document_date: None`, so every row's `confidence` is `Confirmed`; no test needs to assert on it here (covered in Task 6).

- [ ] **Step 3: Update `latest_by_code` signature + body**

In `crates/chartpds-core/src/queries/latest_by_code.rs`:

Add the import:

```rust
use crate::queries::{annotate_observations, ObservationWithConfidence};
```

Change the signature:

```rust
pub async fn latest_by_code(
    pool: &SqlitePool,
    now: OffsetDateTime,
    code: &str,
) -> Result<Option<ObservationWithConfidence>, sqlx::Error> {
```

Replace the final `Ok(row.map(|r| Observation { ... }))` block with:

```rust
    let Some(r) = row else {
        return Ok(None);
    };
    let observation = Observation {
        id: r.id,
        source_document_id: r.source_document_id,
        coding_system: r.coding_system,
        coding_code: r.coding_code,
        coding_display: r.coding_display,
        effective_start: r.effective_start,
        effective_end: r.effective_end,
        value_quantity: r.value_quantity,
        value_string: r.value_string,
        value_unit: r.value_unit,
    };
    let mut annotated = annotate_observations(pool, now, vec![observation]).await?;
    Ok(annotated.pop())
}
```

- [ ] **Step 4: Update `latest_by_code` tests**

In its `#[cfg(test)] mod tests`, add `use time::macros::datetime;` if not present, pass `now` and read through `.observation`:

- `returns_none_when_no_observations_match_the_code`: call `latest_by_code(&pool, datetime!(2026-06-01 00:00:00 UTC), "8302-2")`.
- `returns_the_only_observation_when_one_matches`: pass the datetime; `obs.value_quantity` → `obs.observation.value_quantity`, `obs.effective_start` → `obs.observation.effective_start`.
- `returns_the_most_recent_when_multiple_match`: pass the datetime; `obs.value_quantity` → `obs.observation.value_quantity`, `obs.effective_start` → `obs.observation.effective_start`.
- `does_not_cross_codes`: pass the datetime in both calls; `weight.unwrap().value_quantity` → `weight.unwrap().observation.value_quantity`, and likewise `height`.

- [ ] **Step 5: Update the MCP call sites**

In `crates/chartpds-mcp/src/server.rs`:

`latest_observation_by_code` (~line 165) — change the query call:

```rust
        let observation = chartpds_core::queries::latest_by_code(
            &self.pool,
            time::OffsetDateTime::now_utc(),
            &args.code,
        )
        .await
        .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
```

`get_observation_history` (~line 206) — change the query call:

```rust
        let rows = chartpds_core::queries::observation_history(
            &self.pool,
            time::OffsetDateTime::now_utc(),
            &codings,
            since,
            until,
        )
        .await
        .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;
```

- [ ] **Step 6: Run the affected tests**

Run: `cargo test -p chartpds-core --lib queries::observation_history queries::latest_by_code`
Expected: PASS.

Run: `cargo test -p chartpds-mcp`
Expected: PASS. If a `get_observation_history` MCP test asserts exact object shape and now sees an extra `confidence` key, update that assertion to tolerate/expect the new field; per-field assertions need no change.

- [ ] **Step 7: Full gate + commit**

Run: `just check`
Expected: PASS.

```bash
git add crates/chartpds-core/src/queries/observation_history.rs crates/chartpds-core/src/queries/latest_by_code.rs crates/chartpds-mcp/src/server.rs
git commit -m "Surface confidence on observation history and latest-by-code (#15)"
```

---

### Task 4: Fold confidence into `observation_duration_in_range` (Buckets)

**Files:**
- Modify: `crates/chartpds-core/src/queries/duration_in_value_range.rs`
- Modify: `crates/chartpds-mcp/src/server.rs` (~line 495)

**Interfaces:**
- Consumes: `roll_up_bucket_confidence` (Task 2).
- Produces: `duration_in_value_range(pool, now: OffsetDateTime, params) -> Result<DurationInRange, sqlx::Error>`; `BucketMinutes` gains `pub confidence: DayConfidence`. `Total` variant unchanged.

- [ ] **Step 1: Add `confidence` to `BucketMinutes`**

In `crates/chartpds-core/src/queries/duration_in_value_range.rs`, add the import:

```rust
use crate::queries::roll_up_bucket_confidence;
use crate::sources::DayConfidence;
```

Add the field to the struct:

```rust
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct BucketMinutes {
    /// UTC calendar day (`YYYY-MM-DD`) the bucket covers.
    pub bucket_start: String,
    /// Total minutes inside the value range for the bucket.
    pub total_minutes: f64,
    /// Confidence of the bucket: `Provisional` if any contributing source-day
    /// is provisional, else `Confirmed`.
    pub confidence: DayConfidence,
}
```

- [ ] **Step 2: Thread `now` and roll up in the `Bucket::Day` branch**

Change the signature to add `now` after `pool`:

```rust
pub async fn duration_in_value_range(
    pool: &SqlitePool,
    now: OffsetDateTime,
    params: DurationInValueRangeParams<'_>,
) -> Result<DurationInRange, sqlx::Error> {
```

In the `Bucket::Day` arm, after the existing `rows` aggregation query and before building `DurationInRange::Buckets`, add a companion query over the SAME filter to gather each bucket's contributing source-days, then roll up:

```rust
            let contrib = sqlx::query!(
                r#"
                SELECT date(o.effective_start) AS "bucket!: String",
                       sd.source AS "source!: String",
                       sd.document_date AS "document_date?: String"
                FROM observations o
                JOIN source_documents sd ON o.source_document_id = sd.id
                WHERE o.coding_system = ?
                  AND o.coding_code = ?
                  AND o.effective_start >= ?
                  AND o.effective_start < ?
                  AND o.effective_end IS NOT NULL
                  AND o.value_quantity >= ?
                  AND o.value_quantity <= ?
                GROUP BY date(o.effective_start), sd.source, sd.document_date
                "#,
                coding_system,
                coding_code,
                start,
                end,
                value_min,
                value_max,
            )
            .fetch_all(pool)
            .await?;

            let contributions: Vec<(String, String, Option<String>)> = contrib
                .into_iter()
                .map(|r| (r.bucket, r.source, r.document_date))
                .collect();
            let confidence_by_bucket =
                roll_up_bucket_confidence(pool, now, &contributions).await?;
```

Then change the `BucketMinutes` construction inside the `.map(...)`:

```rust
                        BucketMinutes {
                            bucket_start: r.day.clone(),
                            total_minutes,
                            confidence: confidence_by_bucket
                                .get(&r.day)
                                .copied()
                                .unwrap_or(DayConfidence::Confirmed),
                        }
```

(Note the added `.clone()` on `r.day` because it is now used twice.)

- [ ] **Step 3: Update the duration tests**

In this file's `#[cfg(test)] mod tests`:

- Add `use crate::sources::DayConfidence;` and `use time::macros::datetime;` (if not already imported).
- Every `duration_in_value_range(&pool, <params>)` call becomes `duration_in_value_range(&pool, datetime!(2026-06-01 00:00:00 UTC), <params>)`.
- Every expected `BucketMinutes { bucket_start, total_minutes }` literal gains `confidence: DayConfidence::Confirmed` (the test seed uses `source: "test"`, `document_date: None` → always confirmed).

- [ ] **Step 4: Update the MCP call site**

In `crates/chartpds-mcp/src/server.rs` (~line 495), pass `now` as the second argument:

```rust
        let result = chartpds_core::queries::duration_in_value_range(
            &self.pool,
            time::OffsetDateTime::now_utc(),
            /* existing params argument unchanged */
```

Keep the existing params construction; only insert the `time::OffsetDateTime::now_utc()` argument before it.

- [ ] **Step 5: Regenerate SQL cache**

Run: `just prepare-sql`
Expected: new `.sqlx/query-*.json` for the companion query.

- [ ] **Step 6: Run tests**

Run: `cargo test -p chartpds-core --lib queries::duration_in_value_range`
Expected: PASS.

- [ ] **Step 7: Full gate + commit**

Run: `just check`
Expected: PASS.

```bash
git add crates/chartpds-core/src/queries/duration_in_value_range.rs crates/chartpds-mcp/src/server.rs .sqlx
git commit -m "Surface per-bucket confidence on duration-in-range (#15)"
```

---

### Task 5: Fold confidence into `observation_longest_period_in_range`

**Files:**
- Modify: `crates/chartpds-core/src/queries/longest_continuous_in_value_range.rs`
- Modify: `crates/chartpds-mcp/src/server.rs` (~line 537)

**Interfaces:**
- Consumes: `roll_up_bucket_confidence` (Task 2).
- Produces: `longest_continuous_in_value_range(pool, now: OffsetDateTime, params) -> Result<LongestContinuousInRange, sqlx::Error>`; `BucketLongest` gains `pub confidence: DayConfidence`.

- [ ] **Step 1: Add `confidence` to `BucketLongest`**

In `crates/chartpds-core/src/queries/longest_continuous_in_value_range.rs`, add imports:

```rust
use crate::queries::roll_up_bucket_confidence;
use crate::sources::DayConfidence;
```

Add the field:

```rust
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct BucketLongest {
    /// UTC calendar day (`YYYY-MM-DD`) the run started on.
    pub bucket_start: String,
    /// Length of the longest run that started that day, in minutes.
    pub longest_minutes: f64,
    /// Confidence of the bucket: `Provisional` if any contributing source-day
    /// (keyed by observation UTC day) is provisional, else `Confirmed`.
    pub confidence: DayConfidence,
}
```

- [ ] **Step 2: Thread `now` and roll up**

Change the signature to add `now` after `pool`:

```rust
pub async fn longest_continuous_in_value_range(
    pool: &SqlitePool,
    now: OffsetDateTime,
    params: LongestContinuousParams<'_>,
) -> Result<LongestContinuousInRange, sqlx::Error> {
```

After the existing `rows` fetch and before building `by_day`, add the companion query + roll-up (same filter as the interval fetch, grouped by UTC day + source-day):

```rust
    let contrib = sqlx::query!(
        r#"
        SELECT date(o.effective_start) AS "bucket!: String",
               sd.source AS "source!: String",
               sd.document_date AS "document_date?: String"
        FROM observations o
        JOIN source_documents sd ON o.source_document_id = sd.id
        WHERE o.coding_system = ?
          AND o.coding_code = ?
          AND o.effective_start >= ?
          AND o.effective_start < ?
          AND o.effective_end IS NOT NULL
          AND o.value_quantity >= ?
          AND o.value_quantity <= ?
        GROUP BY date(o.effective_start), sd.source, sd.document_date
        "#,
        coding_system,
        coding_code,
        start,
        end,
        value_min,
        value_max,
    )
    .fetch_all(pool)
    .await?;

    let contributions: Vec<(String, String, Option<String>)> = contrib
        .into_iter()
        .map(|r| (r.bucket, r.source, r.document_date))
        .collect();
    let confidence_by_bucket = roll_up_bucket_confidence(pool, now, &contributions).await?;
```

Change the final `per_bucket` construction to attach confidence:

```rust
    Ok(LongestContinuousInRange {
        per_bucket: by_day
            .into_iter()
            .map(|(bucket_start, longest_minutes)| BucketLongest {
                confidence: confidence_by_bucket
                    .get(&bucket_start)
                    .copied()
                    .unwrap_or(DayConfidence::Confirmed),
                bucket_start,
                longest_minutes,
            })
            .collect(),
    })
```

- [ ] **Step 3: Update the longest tests**

In its `#[cfg(test)] mod tests`:

- Add `use crate::sources::DayConfidence;` and `use time::macros::datetime;` (if not already imported).
- Every `longest_continuous_in_value_range(&pool, <params>)` call becomes `longest_continuous_in_value_range(&pool, datetime!(2026-06-01 00:00:00 UTC), <params>)`.
- Every expected `BucketLongest { bucket_start, longest_minutes }` literal gains `confidence: DayConfidence::Confirmed`.
- Pure-walker tests (`runs(...)`) are unaffected — do not touch them.

- [ ] **Step 4: Update the MCP call site**

In `crates/chartpds-mcp/src/server.rs` (~line 537), insert `time::OffsetDateTime::now_utc()` as the second argument to `longest_continuous_in_value_range`, keeping the existing params argument unchanged.

- [ ] **Step 5: Regenerate SQL cache**

Run: `just prepare-sql`
Expected: new `.sqlx/query-*.json` for the longest companion query.

- [ ] **Step 6: Run tests**

Run: `cargo test -p chartpds-core --lib queries::longest_continuous_in_value_range`
Expected: PASS.

- [ ] **Step 7: Full gate + commit**

Run: `just check`
Expected: PASS.

```bash
git add crates/chartpds-core/src/queries/longest_continuous_in_value_range.rs crates/chartpds-mcp/src/server.rs .sqlx
git commit -m "Surface per-bucket confidence on longest-period-in-range (#15)"
```

---

### Task 6: End-to-end confirmed-path integration test

**Files:**
- Modify: `crates/chartpds-core/src/queries/day_confidence.rs` (add integration test)

**Interfaces:**
- Consumes: everything from Tasks 1–3. This proves the confirmed path end-to-end below the MCP layer, seeding a frontier directly (since `rebuild_index` will not — #14 is WONTFIX).

- [ ] **Step 1: Write the confirmed end-to-end test**

In `crates/chartpds-core/src/queries/day_confidence.rs`'s `#[cfg(test)] mod tests`, add a test that seeds a Fitbit doc + observation + a stable two-pull `source_day_state` + a frontier past the day, then drives `observation_history` and asserts `Confirmed`:

```rust
#[tokio::test]
async fn observation_history_reports_confirmed_for_stable_old_fitbit_day() {
    use crate::queries::observation_history;
    use crate::queries::CodingKey;

    let pool = pool().await;
    let doc = seed_doc(&pool, "fitbit", Some("2026-01-10"),
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc").await;
    let _obs = seed_obs(&pool, doc, datetime!(2026-01-10 08:00:00 UTC)).await;

    // Frontier well past 2026-01-10 + 36h, and a stable two-pull day-state.
    set_frontier(&pool, "fitbit", "2026-01-12T12:00:00Z").await;
    set_day_state(&pool, "fitbit", "2026-01-10", 100, Some(100)).await;

    // "now" is 2026-01-20 → the day is outside the 5-day force-refresh window.
    let rows = observation_history(
        &pool,
        datetime!(2026-01-20 00:00:00 UTC),
        &[CodingKey { coding_system: "http://loinc.org", coding_code: "8867-4" }],
        None,
        None,
    )
    .await
    .expect("history");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].confidence, DayConfidence::Confirmed);
}
```

- [ ] **Step 2: Run it**

Run: `cargo test -p chartpds-core --lib queries::day_confidence::tests::observation_history_reports_confirmed_for_stable_old_fitbit_day`
Expected: PASS.

- [ ] **Step 3: Full gate + commit**

Run: `just check`
Expected: PASS.

```bash
git add crates/chartpds-core/src/queries/day_confidence.rs
git commit -m "Test confirmed confidence end-to-end via observation history (#15)"
```

---

### Task 7: Provisional-case holdout test (staged for bless)

**Files:**
- Create: `holdout/fixtures/fitbit_confidence/<hash>` + `<hash>.meta.json` (copied from an existing Fitbit fixture)
- Create: `holdout/tests/fitbit_confidence.rs`

**Interfaces:**
- Consumes: the full tool surface via `chartpds_holdout::Harness` (see `holdout/tests/fitbit_hr_dedup.rs` for the pattern).

> This task creates PROTECTED files. Write them, run the test green, then LEAVE THEM STAGED-BUT-UNCOMMITTED and hand off: tell the human it is "ready to bless". Do NOT run `just holdout-bless`. Do NOT commit these files.

Rationale for feasibility: after `rebuild_index`, `source_state.freshness_frontier_at` is null (#14 is WONTFIX), so `fitbit_day_confidence` returns `Provisional` for any Fitbit day regardless of age. The holdout leverages this to assert the provisional path without depending on wall-clock recency.

- [ ] **Step 1: Create the fixture from an existing Fitbit blob**

Copy one blob + its sidecar from the existing Fitbit fixture into a new fixture directory (this reuses known-valid Fitbit bytes; a single pull is enough):

```bash
mkdir -p holdout/fixtures/fitbit_confidence
# Pick the newer (3-sample) pull from fitbit_hr_dup; copy both blob and sidecar.
cp holdout/fixtures/fitbit_hr_dup/988cc63f940e35a4f7b348715dabbc9fcc5772eb35d4f994dbece0eb73999974 \
   holdout/fixtures/fitbit_hr_dup/988cc63f940e35a4f7b348715dabbc9fcc5772eb35d4f994dbece0eb73999974.meta.json \
   holdout/fixtures/fitbit_confidence/
ls holdout/fixtures/fitbit_confidence/
```

If that exact hash is absent, list `holdout/fixtures/fitbit_hr_dup/` and copy any one `<64hex>` blob together with its matching `<64hex>.meta.json`.

- [ ] **Step 2: Write the holdout test**

Create `holdout/tests/fitbit_confidence.rs`:

```rust
//! Holdout regression test: recent/unsettled Fitbit data must be reported as
//! `provisional`, never silently as settled fact.
//!
//! PROTECTED: part of the holdout suite. A failure here is a real regression in
//! the product contract — fix `crates/**`, never edit this file or its fixtures
//! to make it pass. Changes under `holdout/` require a human-signed bless commit.
//!
//! Gap being guarded (issue #15): ChartPDS computes per-day confidence but did
//! not surface it, so a consumer querying recent data could not tell an
//! incomplete day from a settled one. After `rebuild_index` (no live frontier),
//! every Fitbit day is unsettled — `get_observation_history` must tag its rows
//! `confidence: "provisional"`.

use chartpds_holdout::Harness;

/// LOINC code for heart rate.
const HEART_RATE: &str = "8867-4";

#[tokio::test]
async fn fitbit_history_reports_provisional_confidence() {
    let server = Harness::start().await;

    server.seed_archive_from_fixtures("fitbit_confidence");
    let rebuild = server
        .call_tool("rebuild_index", serde_json::Value::Null)
        .await;
    assert!(
        rebuild["fitbit_ingested"].as_i64().unwrap_or(0) >= 1,
        "fitbit blob should replay: {rebuild}"
    );

    let history = server
        .call_tool(
            "get_observation_history",
            serde_json::json!({
                "codings": [{ "system": "http://loinc.org", "code": HEART_RATE }]
            }),
        )
        .await;
    let rows = history.as_array().expect("history array");
    assert!(!rows.is_empty(), "expected some HR rows: {history}");
    for row in rows {
        assert_eq!(
            row["confidence"], "provisional",
            "recent Fitbit data must be reported provisional: {row}"
        );
    }
}
```

- [ ] **Step 3: Run the holdout test to confirm it passes (with the fix in place)**

Run: `cargo test -p chartpds-holdout --test fitbit_confidence`
Expected: PASS — rows carry `"confidence": "provisional"`.

If it FAILS because the fixture's document/source strings differ (e.g. the blob replays under a different source name or the codings differ), inspect `holdout/fixtures/fitbit_confidence/*.meta.json` and the replay path, and adjust the fixture selection — NOT the assertion — until the test genuinely reproduces the provisional contract.

- [ ] **Step 4: Stage and hand off (do NOT commit, do NOT bless)**

```bash
git add -N holdout/tests/fitbit_confidence.rs holdout/fixtures/fitbit_confidence
git status --short holdout/
```

Then STOP and tell the human:

> The provisional-case holdout test (`holdout/tests/fitbit_confidence.rs` + `holdout/fixtures/fitbit_confidence/`) is written and passing, staged-but-uncommitted. It's ready to bless — run `just holdout-bless "surface per-day confidence: provisional Fitbit history"` to admit it via a signed commit.

---

## Self-Review

**Spec coverage:**
- Resolver from the index → Task 1 (`resolve_source_day_confidence`). ✓
- `get_observation_history` per-observation confidence → Task 3. ✓
- `latest_observation_by_code` per-observation confidence → Task 3 (added per spec Scope). ✓
- `observation_duration_in_range` Buckets per-bucket confidence → Task 4. ✓
- `observation_longest_period_in_range` per-bucket confidence → Task 5. ✓
- Conservative bucket roll-up (provisional wins) → Task 2 (`roll_up_bucket_confidence`) + Tasks 4/5. ✓
- Day-key = `document_date`, not `effective_start` → Task 2 (`confidence_for_doc` keys on `document_date`; companion query selects `sd.document_date`). ✓
- CCDA / non-wearable / null-date → Confirmed → Task 1 (`_ => Confirmed`) + Task 2 (`confidence_for_doc`). ✓
- Lowercase serialization → Task 1 Step 1. ✓
- Flatten-for-observations, new-field-for-buckets → Tasks 2–5. ✓
- Total variant untouched → Task 4 modifies only `Bucket::Day`. ✓
- `now` injected, MCP passes `now_utc()` → Tasks 3–5. ✓
- Core unit tests + integration test + provisional-only holdout → Tasks 1, 2, 6, 7. ✓
- Longest midnight-crossing subtlety documented → Task 2 (`roll_up_bucket_confidence` doc) + Task 5 (`BucketLongest.confidence` doc). ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code; test bodies are concrete. The only conditional instructions (Task 2 Step 1 pool constructor, Task 7 Step 1 hash fallback, Task 7 Step 3 fixture adjust) are explicit fallbacks, not gaps.

**Type consistency:** `resolve_source_day_confidence` / `annotate_observations` / `roll_up_bucket_confidence` / `ObservationWithConfidence` names and signatures match across Tasks 1–6. `confidence_for_doc` and `wearable_keys` are private and defined once (Task 2). Bucket field is `confidence: DayConfidence` in both `BucketMinutes` (Task 4) and `BucketLongest` (Task 5). All four public query fns take `now` as the second parameter.

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-07-01-surface-per-day-confidence.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

**Which approach?**
