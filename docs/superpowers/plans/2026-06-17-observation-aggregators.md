# P0 Observation Aggregators Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add two analytical query primitives + MCP tools (duration-in-value-range, longest-continuous-run-in-value-range) and make the Oura adapter emit a nightly total-sleep-duration observation.

**Architecture:** #1 (duration) aggregates in SQL with `SUM`/`GROUP BY` so >1M heart-rate rows never reach Rust. #3 (longest run) fetches qualifying rows ordered and walks them in a pure Rust function. #2 (nightly sleep) is a projection change in the Oura adapter only — read through the existing `observations_in_range`. Both query primitives select by a full `{coding_system, coding_code}` pair.

**Tech Stack:** Rust, sqlx (SQLite, compile-time offline verification), `time` crate, rmcp (MCP), `just` task runner.

## Global Constraints

- **Never bypass a lint.** `#[allow(...)]` requires a `reason = "..."` string (workspace `clippy::allow_attributes_without_reason = "deny"`). `just lint` runs clippy with `-D warnings`, so every `pub` item needs a doc comment.
- **sqlx offline mode.** After adding or changing any `sqlx::query!`, run `just prepare-sql` to regenerate `.sqlx/`. The build reads the committed cache, not a live DB. Commit `.sqlx/` changes alongside the code. `just check` runs `cargo sqlx prepare --check`.
- **sqlx column overrides.** Inferred expressions need explicit type hints: `SUM(...) AS "total_seconds!: f64"`, `date(...) AS "day!: String"`, `effective_end AS "effective_end?: OffsetDateTime"`.
- **Module boundaries.** Default visibility in `chartpds-core` is `pub(crate)`; items the binary uses are `pub` and reached through `crate::queries::` / `crate::clinical::` (both are `pub mod`s in `lib.rs`).
- **Migrations are forward-only.** (No schema change is needed in this plan — the `observations` table already has every column used.)
- **`just check`** (fmt-check, lint, typecheck, test, deny, machete, sqlx prepare --check) must pass before the work is complete.
- **House naming:** MCP tool names have no `get_` prefix (`observation_duration_in_range`, `observation_longest_period_in_range`).

---

### Task 1: `duration_in_value_range` query primitive + shared aggregation types

**Files:**
- Create: `crates/chartpds-core/src/queries/duration_in_value_range.rs`
- Modify: `crates/chartpds-core/src/queries/mod.rs`
- Modify: `crates/chartpds-core/src/queries/test_support.rs` (add interval fixture)

**Interfaces:**
- Produces:
  - `pub enum Bucket { None, Day }` (in `duration_in_value_range.rs`, re-exported)
  - `pub struct BucketMinutes { pub bucket_start: String, pub total_minutes: f64 }`
  - `pub enum DurationInRange { Total { total_minutes: f64 }, Buckets { per_bucket: Vec<BucketMinutes> } }` (serde `untagged`)
  - `pub async fn duration_in_value_range(pool, coding_system: &str, coding_code: &str, start: OffsetDateTime, end: OffsetDateTime, value_min: f64, value_max: f64, bucket: Bucket) -> Result<DurationInRange, sqlx::Error>`
  - test fixture `seed_interval_observations(&[IntervalObsSpec]) -> (SqlitePool, i64)` with `IntervalObsSpec { coding_system, coding_code, effective_start, effective_end, value_quantity }`

- [ ] **Step 1: Add the interval test fixture to `test_support.rs`**

Append to `crates/chartpds-core/src/queries/test_support.rs` (the existing `use` block already imports `insert_observation`, `InsertObservationParams`, `open_pool`, `BlobKey`, `insert_source_document`, `InsertSourceDocumentParams`, `OffsetDateTime`):

```rust
/// Spec for one interval observation with explicit system and end time.
///
/// Unlike [`ObsSpec`], this carries an `effective_end` (so duration-based
/// queries have an interval to measure) and an explicit `coding_system` (so
/// tests can mix LOINC and AASM rows).
#[derive(Clone)]
pub(crate) struct IntervalObsSpec {
    pub(crate) coding_system: &'static str,
    pub(crate) coding_code: &'static str,
    pub(crate) effective_start: OffsetDateTime,
    pub(crate) effective_end: OffsetDateTime,
    pub(crate) value_quantity: f64,
}

/// Open a fresh tempdir-backed pool and seed it with interval observations.
///
/// All observations share one `source_documents` row. Returns the pool and
/// that row's id.
pub(crate) async fn seed_interval_observations(
    observations: &[IntervalObsSpec],
) -> (SqlitePool, i64) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("test.db");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    std::mem::forget(dir);
    let pool = open_pool(&url).await.expect("open pool");

    let archive_key =
        BlobKey::from_hex_str("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .expect("valid key");

    let source_document_id = insert_source_document(
        &pool,
        InsertSourceDocumentParams {
            archive_key: &archive_key,
            kind: "ccda",
            source: "test",
            original_filename: None,
            archived_at: OffsetDateTime::now_utc(),
        },
    )
    .await
    .expect("seed source_document");

    for spec in observations {
        insert_observation(
            &pool,
            InsertObservationParams {
                source_document_id,
                coding_system: spec.coding_system,
                coding_code: spec.coding_code,
                coding_display: None,
                effective_start: spec.effective_start,
                effective_end: Some(spec.effective_end),
                value_quantity: Some(spec.value_quantity),
                value_string: None,
                value_unit: None,
            },
        )
        .await
        .expect("seed interval observation");
    }

    (pool, source_document_id)
}
```

- [ ] **Step 2: Write the failing test file**

Create `crates/chartpds-core/src/queries/duration_in_value_range.rs`:

```rust
//! Total time a coded periodic signal spent inside a value range.
//!
//! Sums the durations of interval observations whose `value_quantity` falls
//! within `[value_min, value_max]`, matched by `{coding_system, coding_code}`
//! and a half-open `[start, end)` window on `effective_start`. The sum runs in
//! SQLite so high-volume signals (e.g. heart rate) never ship rows to Rust.

use sqlx::SqlitePool;
use time::OffsetDateTime;

/// How to group aggregated durations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bucket {
    /// One aggregate over the whole window.
    None,
    /// One aggregate per UTC calendar day of `effective_start`.
    Day,
}

/// Total minutes for one UTC calendar day.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct BucketMinutes {
    /// UTC calendar day (`YYYY-MM-DD`) the bucket covers.
    pub bucket_start: String,
    /// Total minutes inside the value range for the bucket.
    pub total_minutes: f64,
}

/// Result of [`duration_in_value_range`].
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(untagged)]
pub enum DurationInRange {
    /// Returned for [`Bucket::None`]: a single total.
    Total {
        /// Total minutes across the window.
        total_minutes: f64,
    },
    /// Returned for [`Bucket::Day`]: per-day totals, ascending by day.
    Buckets {
        /// Per-day totals.
        per_bucket: Vec<BucketMinutes>,
    },
}

/// Sum the minutes a coded signal spent inside `[value_min, value_max]`.
///
/// Matches observations by `coding_system`/`coding_code`, `effective_start`
/// in the half-open window `[start, end)`, `value_quantity` within the
/// inclusive range, and a non-null `effective_end` (rows without an end have
/// no measurable duration and are ignored). Durations come from
/// `effective_end - effective_start`.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn duration_in_value_range(
    pool: &SqlitePool,
    coding_system: &str,
    coding_code: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
    value_min: f64,
    value_max: f64,
    bucket: Bucket,
) -> Result<DurationInRange, sqlx::Error> {
    match bucket {
        Bucket::None => {
            let row = sqlx::query!(
                r#"
                SELECT COALESCE(
                           SUM((julianday(effective_end) - julianday(effective_start)) * 86400.0),
                           0.0
                       ) AS "total_seconds!: f64"
                FROM observations
                WHERE coding_system = ?
                  AND coding_code = ?
                  AND effective_start >= ?
                  AND effective_start < ?
                  AND effective_end IS NOT NULL
                  AND value_quantity >= ?
                  AND value_quantity <= ?
                "#,
                coding_system,
                coding_code,
                start,
                end,
                value_min,
                value_max,
            )
            .fetch_one(pool)
            .await?;

            Ok(DurationInRange::Total {
                total_minutes: row.total_seconds / 60.0,
            })
        }
        Bucket::Day => {
            let rows = sqlx::query!(
                r#"
                SELECT date(effective_start) AS "day!: String",
                       SUM((julianday(effective_end) - julianday(effective_start)) * 86400.0)
                           AS "total_seconds!: f64"
                FROM observations
                WHERE coding_system = ?
                  AND coding_code = ?
                  AND effective_start >= ?
                  AND effective_start < ?
                  AND effective_end IS NOT NULL
                  AND value_quantity >= ?
                  AND value_quantity <= ?
                GROUP BY date(effective_start)
                ORDER BY date(effective_start)
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

            Ok(DurationInRange::Buckets {
                per_bucket: rows
                    .into_iter()
                    .map(|r| BucketMinutes {
                        bucket_start: r.day,
                        total_minutes: r.total_seconds / 60.0,
                    })
                    .collect(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clinical::{AASM_SLEEP_STAGE_CODE, AASM_SLEEP_STAGE_SYSTEM, SYSTEM_LOINC};
    use crate::queries::test_support::{seed_interval_observations, IntervalObsSpec};
    use time::macros::datetime;

    // Heart-rate-shaped seed: three 1-minute intervals; BPM 100, 110, 130.
    fn three_hr_minutes() -> [IntervalObsSpec; 3] {
        [
            IntervalObsSpec {
                coding_system: SYSTEM_LOINC,
                coding_code: "8867-4",
                effective_start: datetime!(2026-01-01 08:00:00 UTC),
                effective_end: datetime!(2026-01-01 08:01:00 UTC),
                value_quantity: 100.0,
            },
            IntervalObsSpec {
                coding_system: SYSTEM_LOINC,
                coding_code: "8867-4",
                effective_start: datetime!(2026-01-01 08:01:00 UTC),
                effective_end: datetime!(2026-01-01 08:02:00 UTC),
                value_quantity: 110.0,
            },
            IntervalObsSpec {
                coding_system: SYSTEM_LOINC,
                coding_code: "8867-4",
                effective_start: datetime!(2026-01-02 08:00:00 UTC),
                effective_end: datetime!(2026-01-02 08:01:00 UTC),
                value_quantity: 130.0,
            },
        ]
    }

    #[tokio::test]
    async fn total_sums_only_in_range_intervals() {
        let (pool, _) = seed_interval_observations(&three_hr_minutes()).await;
        // Range 101..118 includes only the 110 bpm minute => 1.0 minute.
        let result = duration_in_value_range(
            &pool,
            SYSTEM_LOINC,
            "8867-4",
            datetime!(2026-01-01 00:00:00 UTC),
            datetime!(2026-01-03 00:00:00 UTC),
            101.0,
            118.0,
            Bucket::None,
        )
        .await
        .expect("query");
        assert_eq!(result, DurationInRange::Total { total_minutes: 1.0 });
    }

    #[tokio::test]
    async fn day_bucket_groups_by_utc_day() {
        let (pool, _) = seed_interval_observations(&three_hr_minutes()).await;
        // Range 90..140 includes all three; day 1 has two minutes, day 2 one.
        let result = duration_in_value_range(
            &pool,
            SYSTEM_LOINC,
            "8867-4",
            datetime!(2026-01-01 00:00:00 UTC),
            datetime!(2026-01-03 00:00:00 UTC),
            90.0,
            140.0,
            Bucket::Day,
        )
        .await
        .expect("query");
        assert_eq!(
            result,
            DurationInRange::Buckets {
                per_bucket: vec![
                    BucketMinutes { bucket_start: "2026-01-01".to_string(), total_minutes: 2.0 },
                    BucketMinutes { bucket_start: "2026-01-02".to_string(), total_minutes: 1.0 },
                ],
            }
        );
    }

    #[tokio::test]
    async fn does_not_cross_coding_systems() {
        // An AASM row with value 3 must not be counted as an 8867-4 (HR) row,
        // even though both queries could share a numeric range.
        let specs = [IntervalObsSpec {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            effective_start: datetime!(2026-01-01 08:00:00 UTC),
            effective_end: datetime!(2026-01-01 08:05:00 UTC),
            value_quantity: 3.0,
        }];
        let (pool, _) = seed_interval_observations(&specs).await;
        let result = duration_in_value_range(
            &pool,
            SYSTEM_LOINC,
            "8867-4",
            datetime!(2026-01-01 00:00:00 UTC),
            datetime!(2026-01-02 00:00:00 UTC),
            1.0,
            4.0,
            Bucket::None,
        )
        .await
        .expect("query");
        assert_eq!(result, DurationInRange::Total { total_minutes: 0.0 });
    }

    #[tokio::test]
    async fn excludes_rows_outside_window() {
        let (pool, _) = seed_interval_observations(&three_hr_minutes()).await;
        // Window covers only day 1; day-2 row excluded. Range covers all values.
        let result = duration_in_value_range(
            &pool,
            SYSTEM_LOINC,
            "8867-4",
            datetime!(2026-01-01 00:00:00 UTC),
            datetime!(2026-01-02 00:00:00 UTC),
            90.0,
            140.0,
            Bucket::None,
        )
        .await
        .expect("query");
        assert_eq!(result, DurationInRange::Total { total_minutes: 2.0 });
    }
}
```

- [ ] **Step 3: Wire the module + re-exports in `queries/mod.rs`**

Add the `mod` declaration (alphabetical, after `mod counts_per_code;`):

```rust
mod duration_in_value_range;
```

Add the re-export (after `pub use counts_per_code::{counts_per_code, CodeCount};`):

```rust
pub use duration_in_value_range::{duration_in_value_range, Bucket, BucketMinutes, DurationInRange};
```

- [ ] **Step 4: Regenerate the sqlx cache**

Run: `just prepare-sql`
Expected: completes without error; new `.sqlx/query-*.json` files appear for the two new queries.

- [ ] **Step 5: Run the tests, verify they pass**

Run: `cargo test -p chartpds-core duration_in_value_range`
Expected: PASS — `total_sums_only_in_range_intervals`, `day_bucket_groups_by_utc_day`, `does_not_cross_coding_systems`, `excludes_rows_outside_window`.

- [ ] **Step 6: Commit**

```bash
git add crates/chartpds-core/src/queries/duration_in_value_range.rs \
        crates/chartpds-core/src/queries/mod.rs \
        crates/chartpds-core/src/queries/test_support.rs \
        .sqlx
git commit -m "Add duration_in_value_range query primitive"
```

---

### Task 2: `longest_continuous_in_value_range` query + pure run walker

**Files:**
- Create: `crates/chartpds-core/src/queries/longest_continuous_in_value_range.rs`
- Modify: `crates/chartpds-core/src/queries/mod.rs`

**Interfaces:**
- Consumes: `IntervalObsSpec` / `seed_interval_observations` from Task 1 (`crate::queries::test_support`); `AASM_SLEEP_STAGE_SYSTEM` / `AASM_SLEEP_STAGE_CODE` from `crate::clinical`.
- Produces:
  - `pub struct BucketLongest { pub bucket_start: String, pub longest_minutes: f64 }`
  - `pub struct LongestContinuousInRange { pub per_bucket: Vec<BucketLongest> }`
  - `pub async fn longest_continuous_in_value_range(pool, coding_system: &str, coding_code: &str, start: OffsetDateTime, end: OffsetDateTime, value_min: f64, value_max: f64, gap_seconds: i64) -> Result<LongestContinuousInRange, sqlx::Error>`

- [ ] **Step 1: Write the failing test file**

Create `crates/chartpds-core/src/queries/longest_continuous_in_value_range.rs`:

```rust
//! Longest unbroken run of in-range observations, bucketed by UTC start day.
//!
//! Fetches the qualifying interval observations (matched by
//! `{coding_system, coding_code}`, window, and value range) ordered by start,
//! then walks them in a pure function to find runs. Consecutive in-range
//! intervals join one run while the gap between them is `<= gap_seconds`.
//! Each run is attributed to the UTC calendar day of its start; the result
//! reports the longest run per day.

use std::collections::BTreeMap;

use sqlx::SqlitePool;
use time::{OffsetDateTime, UtcOffset};

/// Longest continuous run, in minutes, for one UTC calendar day.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct BucketLongest {
    /// UTC calendar day (`YYYY-MM-DD`) the run started on.
    pub bucket_start: String,
    /// Length of the longest run that started that day, in minutes.
    pub longest_minutes: f64,
}

/// Result of [`longest_continuous_in_value_range`]: per-day longest runs,
/// ascending by day.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct LongestContinuousInRange {
    /// Longest run per UTC start day.
    pub per_bucket: Vec<BucketLongest>,
}

/// One continuous run of in-range intervals.
struct Run {
    /// Start of the run's first interval.
    start: OffsetDateTime,
    /// Wall-clock length of the run (last end - first start), in minutes.
    minutes: f64,
}

/// Group already-in-range, start-ordered intervals into continuous runs.
///
/// Two consecutive intervals join the same run when
/// `next.start - prev.end <= gap_seconds`; a larger gap starts a new run.
/// A run's length is the wall-clock span from its first start to its last end.
fn runs(intervals: &[(OffsetDateTime, OffsetDateTime)], gap_seconds: i64) -> Vec<Run> {
    let mut out = Vec::new();
    let mut cur_start: Option<OffsetDateTime> = None;
    let mut cur_end: Option<OffsetDateTime> = None;

    for &(start, end) in intervals {
        match cur_end {
            Some(prev_end) if (start - prev_end).whole_seconds() <= gap_seconds => {
                cur_end = Some(end);
            }
            _ => {
                if let (Some(s), Some(e)) = (cur_start, cur_end) {
                    out.push(Run { start: s, minutes: (e - s).as_seconds_f64() / 60.0 });
                }
                cur_start = Some(start);
                cur_end = Some(end);
            }
        }
    }
    if let (Some(s), Some(e)) = (cur_start, cur_end) {
        out.push(Run { start: s, minutes: (e - s).as_seconds_f64() / 60.0 });
    }
    out
}

/// UTC calendar day (`YYYY-MM-DD`) of a timestamp.
fn utc_day(ts: OffsetDateTime) -> String {
    let utc = ts.to_offset(UtcOffset::UTC);
    format!("{:04}-{:02}-{:02}", utc.year(), u8::from(utc.month()), utc.day())
}

/// Find the longest continuous in-range run per UTC start day.
///
/// Matches observations by `coding_system`/`coding_code`, `effective_start`
/// in `[start, end)`, `value_quantity` within `[value_min, value_max]`, and a
/// non-null `effective_end`. Out-of-range rows are removed in SQL, so the
/// walker treats every fetched interval as in-range and `gap_seconds` bridges
/// only missing-data gaps.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn longest_continuous_in_value_range(
    pool: &SqlitePool,
    coding_system: &str,
    coding_code: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
    value_min: f64,
    value_max: f64,
    gap_seconds: i64,
) -> Result<LongestContinuousInRange, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT effective_start AS "effective_start: OffsetDateTime",
               effective_end AS "effective_end!: OffsetDateTime"
        FROM observations
        WHERE coding_system = ?
          AND coding_code = ?
          AND effective_start >= ?
          AND effective_start < ?
          AND effective_end IS NOT NULL
          AND value_quantity >= ?
          AND value_quantity <= ?
        ORDER BY effective_start
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

    let intervals: Vec<(OffsetDateTime, OffsetDateTime)> = rows
        .into_iter()
        .map(|r| (r.effective_start, r.effective_end))
        .collect();

    let mut by_day: BTreeMap<String, f64> = BTreeMap::new();
    for run in runs(&intervals, gap_seconds) {
        let day = utc_day(run.start);
        let entry = by_day.entry(day).or_insert(0.0);
        *entry = entry.max(run.minutes);
    }

    Ok(LongestContinuousInRange {
        per_bucket: by_day
            .into_iter()
            .map(|(bucket_start, longest_minutes)| BucketLongest { bucket_start, longest_minutes })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clinical::{AASM_SLEEP_STAGE_CODE, AASM_SLEEP_STAGE_SYSTEM};
    use crate::queries::test_support::{seed_interval_observations, IntervalObsSpec};
    use time::macros::datetime;

    // --- pure walker tests ---

    #[test]
    fn runs_empty_input_is_empty() {
        assert!(runs(&[], 0).is_empty());
    }

    #[test]
    fn runs_contiguous_intervals_form_one_run() {
        // Two back-to-back 5-min intervals, gap 0 => one 10-min run.
        let iv = [
            (datetime!(2026-01-01 22:00:00 UTC), datetime!(2026-01-01 22:05:00 UTC)),
            (datetime!(2026-01-01 22:05:00 UTC), datetime!(2026-01-01 22:10:00 UTC)),
        ];
        let r = runs(&iv, 0);
        assert_eq!(r.len(), 1);
        assert!((r[0].minutes - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn runs_gap_over_tolerance_splits() {
        // 5-min interval, then a 10-min gap, then a 5-min interval. gap 0 splits.
        let iv = [
            (datetime!(2026-01-01 22:00:00 UTC), datetime!(2026-01-01 22:05:00 UTC)),
            (datetime!(2026-01-01 22:15:00 UTC), datetime!(2026-01-01 22:20:00 UTC)),
        ];
        let r = runs(&iv, 0);
        assert_eq!(r.len(), 2);
        assert!((r[0].minutes - 5.0).abs() < f64::EPSILON);
        assert!((r[1].minutes - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn runs_gap_within_tolerance_joins() {
        // Same as above but gap_seconds = 600 bridges the 10-min gap into one
        // run spanning 22:00 -> 22:20 = 20 minutes.
        let iv = [
            (datetime!(2026-01-01 22:00:00 UTC), datetime!(2026-01-01 22:05:00 UTC)),
            (datetime!(2026-01-01 22:15:00 UTC), datetime!(2026-01-01 22:20:00 UTC)),
        ];
        let r = runs(&iv, 600);
        assert_eq!(r.len(), 1);
        assert!((r[0].minutes - 20.0).abs() < f64::EPSILON);
    }

    // --- query tests ---

    #[tokio::test]
    async fn longest_run_per_day_picks_the_max() {
        // Day 1: a 10-min run (two epochs) and a separate 5-min run.
        // Expect day 1 longest = 10 minutes.
        let specs = [
            IntervalObsSpec {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                effective_start: datetime!(2026-01-01 22:00:00 UTC),
                effective_end: datetime!(2026-01-01 22:05:00 UTC),
                value_quantity: 2.0,
            },
            IntervalObsSpec {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                effective_start: datetime!(2026-01-01 22:05:00 UTC),
                effective_end: datetime!(2026-01-01 22:10:00 UTC),
                value_quantity: 3.0,
            },
            IntervalObsSpec {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                effective_start: datetime!(2026-01-01 23:00:00 UTC),
                effective_end: datetime!(2026-01-01 23:05:00 UTC),
                value_quantity: 1.0,
            },
        ];
        let (pool, _) = seed_interval_observations(&specs).await;
        let result = longest_continuous_in_value_range(
            &pool,
            AASM_SLEEP_STAGE_SYSTEM,
            AASM_SLEEP_STAGE_CODE,
            datetime!(2026-01-01 00:00:00 UTC),
            datetime!(2026-01-02 00:00:00 UTC),
            1.0,
            4.0,
            0,
        )
        .await
        .expect("query");
        assert_eq!(
            result,
            LongestContinuousInRange {
                per_bucket: vec![BucketLongest {
                    bucket_start: "2026-01-01".to_string(),
                    longest_minutes: 10.0,
                }],
            }
        );
    }

    #[tokio::test]
    async fn out_of_range_epoch_breaks_the_run() {
        // Asleep (2), awake (0, out of range 1..4), asleep (3): the awake epoch
        // is filtered out in SQL, leaving a gap that splits the run at gap 0.
        // Each surviving run is 5 minutes.
        let specs = [
            IntervalObsSpec {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                effective_start: datetime!(2026-01-01 22:00:00 UTC),
                effective_end: datetime!(2026-01-01 22:05:00 UTC),
                value_quantity: 2.0,
            },
            IntervalObsSpec {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                effective_start: datetime!(2026-01-01 22:05:00 UTC),
                effective_end: datetime!(2026-01-01 22:10:00 UTC),
                value_quantity: 0.0,
            },
            IntervalObsSpec {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                effective_start: datetime!(2026-01-01 22:10:00 UTC),
                effective_end: datetime!(2026-01-01 22:15:00 UTC),
                value_quantity: 3.0,
            },
        ];
        let (pool, _) = seed_interval_observations(&specs).await;
        let result = longest_continuous_in_value_range(
            &pool,
            AASM_SLEEP_STAGE_SYSTEM,
            AASM_SLEEP_STAGE_CODE,
            datetime!(2026-01-01 00:00:00 UTC),
            datetime!(2026-01-02 00:00:00 UTC),
            1.0,
            4.0,
            0,
        )
        .await
        .expect("query");
        assert_eq!(
            result,
            LongestContinuousInRange {
                per_bucket: vec![BucketLongest {
                    bucket_start: "2026-01-01".to_string(),
                    longest_minutes: 5.0,
                }],
            }
        );
    }
}
```

- [ ] **Step 2: Wire the module + re-exports in `queries/mod.rs`**

Add the `mod` declaration (alphabetical, after `mod latest_by_code;` and before `mod list_medications;`):

```rust
mod longest_continuous_in_value_range;
```

Add the re-export (after the `duration_in_value_range` re-export from Task 1):

```rust
pub use longest_continuous_in_value_range::{
    longest_continuous_in_value_range, BucketLongest, LongestContinuousInRange,
};
```

- [ ] **Step 3: Regenerate the sqlx cache**

Run: `just prepare-sql`
Expected: completes without error; a new `.sqlx/query-*.json` appears for the longest-run query.

- [ ] **Step 4: Run the tests, verify they pass**

Run: `cargo test -p chartpds-core longest_continuous_in_value_range`
Expected: PASS — the three `runs_*` walker tests plus `longest_run_per_day_picks_the_max` and `out_of_range_epoch_breaks_the_run`.

- [ ] **Step 5: Commit**

```bash
git add crates/chartpds-core/src/queries/longest_continuous_in_value_range.rs \
        crates/chartpds-core/src/queries/mod.rs \
        .sqlx
git commit -m "Add longest_continuous_in_value_range query primitive"
```

---

### Task 3: Nightly total-sleep-duration observation (Oura adapter)

**Files:**
- Modify: `crates/chartpds-core/src/clinical/coding.rs` (add LOINC sleep-duration code constant)
- Modify: `crates/chartpds-core/src/clinical/mod.rs` (re-export it)
- Modify: `crates/chartpds-core/src/sources/oura/parser.rs` (pure nightly helper)
- Modify: `crates/chartpds-core/src/sources/oura/storage.rs` (insert nightly observation; update existing test)

**Interfaces:**
- Consumes: `OuraSleepSession` (`super::api`), `SYSTEM_LOINC` + new `LOINC_SLEEP_DURATION` (`crate::clinical`), `index::insert_observation` / `InsertObservationParams`.
- Produces:
  - `pub const LOINC_SLEEP_DURATION: &str` = `"93832-4"`
  - `pub struct ParsedSleepDuration { pub effective_start: OffsetDateTime, pub effective_end: OffsetDateTime, pub minutes: f64 }` (in `oura/parser.rs`)
  - `pub fn nightly_sleep_duration(session: &OuraSleepSession) -> sources::Result<Option<ParsedSleepDuration>>`

- [ ] **Step 1: Add the LOINC constant**

In `crates/chartpds-core/src/clinical/coding.rs`, after the `SYSTEM_SNOMED` constant (before `fhir_system_for_oid`):

```rust
/// LOINC code for total sleep duration (per-night summary observation).
///
/// Emitted once per night by the Oura adapter alongside the per-epoch
/// sleep-stage rows, so clients can read a nightly total without summing
/// epochs. Stored in minutes (`value_unit = "min"`).
pub const LOINC_SLEEP_DURATION: &str = "93832-4";
```

Add a unit test in that file's `mod tests`:

```rust
    #[test]
    fn loinc_sleep_duration_is_the_expected_code() {
        assert_eq!(LOINC_SLEEP_DURATION, "93832-4");
    }
```

- [ ] **Step 2: Re-export from `clinical/mod.rs`**

Change the `coding` re-export line to include the new constant:

```rust
pub use coding::{
    fhir_system_for_oid, LOINC_SLEEP_DURATION, SYSTEM_ICD10, SYSTEM_LOINC, SYSTEM_RXNORM,
    SYSTEM_SNOMED,
};
```

- [ ] **Step 3: Run the clinical test, verify it passes**

Run: `cargo test -p chartpds-core loinc_sleep_duration_is_the_expected_code`
Expected: PASS.

- [ ] **Step 4: Write the failing parser tests**

In `crates/chartpds-core/src/sources/oura/parser.rs`, add `use super::api::OuraSleepSession;` to the imports, then add these tests to `mod tests` (they reference `nightly_sleep_duration`, which does not exist yet — compile failure is the expected "fail"):

```rust
    fn session(session_type: &str, total: Option<i64>) -> OuraSleepSession {
        OuraSleepSession {
            id: "s1".to_owned(),
            day: "2026-01-15".to_owned(),
            bedtime_start: "2026-01-14T22:00:00Z".to_owned(),
            bedtime_end: "2026-01-15T06:00:00Z".to_owned(),
            session_type: session_type.to_owned(),
            sleep_phase_5_min: "4421".to_owned(),
            total_sleep_duration: total,
            rem_sleep_duration: None,
            deep_sleep_duration: None,
            light_sleep_duration: None,
        }
    }

    #[test]
    fn nightly_duration_for_long_sleep_with_total() {
        let parsed = nightly_sleep_duration(&session("long_sleep", Some(28800)))
            .expect("parse")
            .expect("some");
        assert_eq!(parsed.effective_start, datetime!(2026-01-14 22:00:00 UTC));
        assert_eq!(parsed.effective_end, datetime!(2026-01-15 06:00:00 UTC));
        assert!((parsed.minutes - 480.0).abs() < f64::EPSILON);
    }

    #[test]
    fn nightly_duration_none_for_null_total() {
        let parsed = nightly_sleep_duration(&session("long_sleep", None)).expect("parse");
        assert!(parsed.is_none());
    }

    #[test]
    fn nightly_duration_none_for_nap() {
        let parsed = nightly_sleep_duration(&session("late_nap", Some(3600))).expect("parse");
        assert!(parsed.is_none());
    }
```

- [ ] **Step 5: Run the parser tests, verify they fail to compile**

Run: `cargo test -p chartpds-core sources::oura::parser`
Expected: FAIL — `cannot find function nightly_sleep_duration`.

- [ ] **Step 6: Implement the nightly helper**

In `crates/chartpds-core/src/sources/oura/parser.rs`, add after `parse_sleep_epochs`:

```rust
/// A derived nightly total-sleep-duration summary for one session.
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedSleepDuration {
    /// Session start (observation `effective_start`).
    pub effective_start: OffsetDateTime,
    /// Session end (observation `effective_end`).
    pub effective_end: OffsetDateTime,
    /// Total minutes asleep.
    pub minutes: f64,
}

/// Derive the nightly total-sleep-duration observation for a session.
///
/// Returns `Some` only for `long_sleep` sessions that report a
/// `total_sleep_duration`; naps and null-duration sessions return `None`
/// (their per-epoch observations still land regardless).
///
/// # Errors
///
/// Returns [`sources::Error::Parse`] if `bedtime_start` or `bedtime_end` is
/// not valid RFC 3339.
pub fn nightly_sleep_duration(
    session: &OuraSleepSession,
) -> sources::Result<Option<ParsedSleepDuration>> {
    if session.session_type != "long_sleep" {
        return Ok(None);
    }
    let Some(total_secs) = session.total_sleep_duration else {
        return Ok(None);
    };

    let effective_start = OffsetDateTime::parse(&session.bedtime_start, &Rfc3339).map_err(|err| {
        sources::Error::Parse {
            reason: format!("invalid bedtime_start {:?}: {err}", session.bedtime_start),
        }
    })?;
    let effective_end = OffsetDateTime::parse(&session.bedtime_end, &Rfc3339).map_err(|err| {
        sources::Error::Parse {
            reason: format!("invalid bedtime_end {:?}: {err}", session.bedtime_end),
        }
    })?;

    #[allow(
        clippy::cast_precision_loss,
        reason = "sleep seconds for a realistic night fit f64 without precision loss"
    )]
    let minutes = total_secs as f64 / 60.0;

    Ok(Some(ParsedSleepDuration {
        effective_start,
        effective_end,
        minutes,
    }))
}
```

- [ ] **Step 7: Run the parser tests, verify they pass**

Run: `cargo test -p chartpds-core sources::oura::parser`
Expected: PASS (including the existing epoch tests and the three new nightly tests).

- [ ] **Step 8: Insert the nightly observation in `storage.rs`**

In `crates/chartpds-core/src/sources/oura/storage.rs`, extend the clinical import:

```rust
use crate::clinical::{
    AASM_SLEEP_STAGE_CODE, AASM_SLEEP_STAGE_SYSTEM, LOINC_SLEEP_DURATION, SYSTEM_LOINC,
};
```

In `index_sleep_session`, after the `for obs in &observations { insert_sleep_observation(...) }` loop and before the `source_day_state` upsert, add:

```rust
    if let Some(nightly) = parser::nightly_sleep_duration(session)? {
        index::insert_observation(
            pool,
            index::InsertObservationParams {
                source_document_id,
                coding_system: SYSTEM_LOINC,
                coding_code: LOINC_SLEEP_DURATION,
                coding_display: Some("Sleep duration"),
                effective_start: nightly.effective_start,
                effective_end: Some(nightly.effective_end),
                value_quantity: Some(nightly.minutes),
                value_string: None,
                value_unit: Some("min"),
            },
        )
        .await?;
    }
```

(`samples_count` is left as the per-epoch `observations.len()` — the nightly row is derived, not a raw sample.)

- [ ] **Step 9: Update the existing storage test to be coding-aware**

The existing `ingest_session_archives_and_inserts_observations` asserts exactly 4 observations and indexes them positionally. With the nightly row this becomes coding-mixed, so replace its assertions (everything after the `let observations = list_observations_by_source_document(...)` line) with:

```rust
        let observations = list_observations_by_source_document(&pool, doc_id)
            .await
            .expect("list");

        // 4 per-epoch AASM rows + 1 nightly LOINC sleep-duration row.
        let epochs: Vec<_> = observations
            .iter()
            .filter(|o| o.coding_code == AASM_SLEEP_STAGE_CODE)
            .collect();
        assert_eq!(epochs.len(), 4);
        assert_eq!(epochs[0].value_string.as_deref(), Some("wake"));
        assert_eq!(epochs[3].value_string.as_deref(), Some("n3"));

        let nightly: Vec<_> = observations
            .iter()
            .filter(|o| o.coding_code == LOINC_SLEEP_DURATION)
            .collect();
        assert_eq!(nightly.len(), 1);
        assert_eq!(nightly[0].coding_system, SYSTEM_LOINC);
        // 28800 s / 60 = 480 min.
        assert_eq!(nightly[0].value_quantity, Some(480.0));
        assert_eq!(nightly[0].value_unit.as_deref(), Some("min"));
```

Add a focused test for the nap/non-long-sleep case right after it:

```rust
    #[tokio::test]
    async fn nap_session_emits_no_nightly_duration() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);

        let session = OuraSleepSession {
            id: "nap-1".to_owned(),
            day: "2026-01-15".to_owned(),
            bedtime_start: "2026-01-15T13:00:00Z".to_owned(),
            bedtime_end: "2026-01-15T13:30:00Z".to_owned(),
            session_type: "late_nap".to_owned(),
            sleep_phase_5_min: "22".to_owned(),
            total_sleep_duration: Some(1800),
            rem_sleep_duration: None,
            deep_sleep_duration: None,
            light_sleep_duration: None,
        };
        let raw = serde_json::json!({ "id": "nap-1" });
        let doc_id = ingest_session(&archive, &pool, &session, &raw)
            .await
            .expect("ingest");

        let observations = list_observations_by_source_document(&pool, doc_id)
            .await
            .expect("list");
        assert!(observations.iter().all(|o| o.coding_code == AASM_SLEEP_STAGE_CODE));
    }
```

- [ ] **Step 10: Regenerate the sqlx cache**

(No new `sqlx::query!` was added — `insert_observation` is reused — but run this to confirm the cache is still in sync.)

Run: `just prepare-sql`
Expected: completes without error; no unexpected `.sqlx` changes.

- [ ] **Step 11: Run the Oura tests, verify they pass**

Run: `cargo test -p chartpds-core sources::oura`
Expected: PASS — parser nightly tests, `ingest_session_archives_and_inserts_observations` (updated), `nap_session_emits_no_nightly_duration`, `ingest_session_updates_source_day_state` (unchanged, still `samples_count == 2`).

- [ ] **Step 12: Commit**

```bash
git add crates/chartpds-core/src/clinical/coding.rs \
        crates/chartpds-core/src/clinical/mod.rs \
        crates/chartpds-core/src/sources/oura/parser.rs \
        crates/chartpds-core/src/sources/oura/storage.rs \
        .sqlx
git commit -m "Emit nightly total-sleep-duration observation from Oura adapter"
```

---

### Task 4: MCP tool `observation_duration_in_range`

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs` (arg structs + tool handler + tests)

**Interfaces:**
- Consumes: `chartpds_core::queries::{duration_in_value_range, Bucket, DurationInRange}`.
- Produces: `pub(crate) struct Coding { system: String, code: String }` (shared with Task 5); tool method `observation_duration_in_range`.

- [ ] **Step 1: Add the shared `Coding` arg struct and the duration args**

In `crates/chartpds-mcp/src/server.rs`, near the other `*Args` structs (after `ObservationsInRangeArgs`):

```rust
/// A coding selector: FHIR system URI plus code.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct Coding {
    /// FHIR system URI (e.g. `"http://loinc.org"` or the AASM sleep-stage URI).
    pub(crate) system: String,
    /// Code within the system (e.g. `"8867-4"`).
    pub(crate) code: String,
}

/// Arguments for the `observation_duration_in_range` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct ObservationDurationInRangeArgs {
    /// Coding to aggregate over.
    pub(crate) coding: Coding,
    /// Inclusive start of the time window (RFC 3339).
    pub(crate) start: String,
    /// Exclusive end of the time window (RFC 3339).
    pub(crate) end: String,
    /// Inclusive lower bound on `value_quantity`.
    pub(crate) value_min: f64,
    /// Inclusive upper bound on `value_quantity`.
    pub(crate) value_max: f64,
    /// Bucketing: `"none"` (default, single total) or `"day"` (per UTC day).
    pub(crate) bucket: Option<String>,
}
```

- [ ] **Step 2: Write the failing tool test**

In the `mod tests` block, add a seeding helper and a test (references the not-yet-existing method — compile failure is the expected "fail"):

```rust
    async fn fresh_server_with_hr_minutes() -> ChartPdsServer {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");

        let key = BlobKey::from_hex_str(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .expect("valid key");
        let doc_id = insert_source_document(
            &pool,
            InsertSourceDocumentParams {
                archive_key: &key,
                kind: "ccda",
                source: "test",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
            },
        )
        .await
        .expect("doc");

        // Two 1-minute HR intervals: 110 bpm (in 101..118) and 130 bpm (out).
        for (start_end, bpm) in [
            ((datetime!(2026-01-01 08:00:00 UTC), datetime!(2026-01-01 08:01:00 UTC)), 110.0),
            ((datetime!(2026-01-01 08:01:00 UTC), datetime!(2026-01-01 08:02:00 UTC)), 130.0),
        ] {
            insert_observation(
                &pool,
                InsertObservationParams {
                    source_document_id: doc_id,
                    coding_system: "http://loinc.org",
                    coding_code: "8867-4",
                    coding_display: Some("Heart rate"),
                    effective_start: start_end.0,
                    effective_end: Some(start_end.1),
                    value_quantity: Some(bpm),
                    value_string: None,
                    value_unit: Some("bpm"),
                },
            )
            .await
            .expect("obs");
        }

        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        ChartPdsServer::new(pool, archive, None, reqwest::Client::new())
    }

    #[tokio::test]
    async fn observation_duration_in_range_totals_in_zone_minutes() {
        let server = fresh_server_with_hr_minutes().await;
        let result = server
            .observation_duration_in_range(Parameters(ObservationDurationInRangeArgs {
                coding: Coding {
                    system: "http://loinc.org".to_string(),
                    code: "8867-4".to_string(),
                },
                start: "2026-01-01T00:00:00Z".to_string(),
                end: "2026-01-02T00:00:00Z".to_string(),
                value_min: 101.0,
                value_max: 118.0,
                bucket: None,
            }))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["total_minutes"], 1.0);
    }
```

- [ ] **Step 3: Run the test, verify it fails to compile**

Run: `cargo test -p chartpds-mcp observation_duration_in_range_totals_in_zone_minutes`
Expected: FAIL — `no method named observation_duration_in_range`.

- [ ] **Step 4: Implement the tool handler**

Inside the `#[tool_router]` impl block (alongside `observations_in_range`), add:

```rust
    #[tool(
        description = "Total minutes a coded periodic signal spent inside a value range over a window. Args: coding {system, code}, start/end (RFC 3339, half-open), value_min/value_max (inclusive). bucket \"none\" (default) returns {total_minutes}; \"day\" returns {per_bucket:[{bucket_start, total_minutes}]} grouped by UTC day."
    )]
    async fn observation_duration_in_range(
        &self,
        Parameters(args): Parameters<ObservationDurationInRangeArgs>,
    ) -> Result<CallToolResult, McpError> {
        let start = time::OffsetDateTime::parse(&args.start, &Rfc3339)
            .map_err(|err| McpError::invalid_params(format!("invalid start: {err}"), None))?;
        let end = time::OffsetDateTime::parse(&args.end, &Rfc3339)
            .map_err(|err| McpError::invalid_params(format!("invalid end: {err}"), None))?;
        let bucket = match args.bucket.as_deref() {
            None | Some("none") => chartpds_core::queries::Bucket::None,
            Some("day") => chartpds_core::queries::Bucket::Day,
            Some(other) => {
                return Err(McpError::invalid_params(
                    format!("invalid bucket {other:?}; expected \"none\" or \"day\""),
                    None,
                ))
            }
        };

        let result = chartpds_core::queries::duration_in_value_range(
            &self.pool,
            &args.coding.system,
            &args.coding.code,
            start,
            end,
            args.value_min,
            args.value_max,
            bucket,
        )
        .await
        .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;

        let json = serde_json::to_string(&result)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }
```

- [ ] **Step 5: Run the test, verify it passes**

Run: `cargo test -p chartpds-mcp observation_duration_in_range_totals_in_zone_minutes`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/chartpds-mcp/src/server.rs
git commit -m "Add observation_duration_in_range MCP tool"
```

---

### Task 5: MCP tool `observation_longest_period_in_range`

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs` (arg struct + tool handler + test)

**Interfaces:**
- Consumes: `Coding` (Task 4); `chartpds_core::queries::{longest_continuous_in_value_range, LongestContinuousInRange}`; AASM coding constants via literal strings in the test.

- [ ] **Step 1: Add the args struct**

In `crates/chartpds-mcp/src/server.rs`, after `ObservationDurationInRangeArgs`:

```rust
/// Arguments for the `observation_longest_period_in_range` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct ObservationLongestPeriodInRangeArgs {
    /// Coding to aggregate over.
    pub(crate) coding: Coding,
    /// Inclusive start of the time window (RFC 3339).
    pub(crate) start: String,
    /// Exclusive end of the time window (RFC 3339).
    pub(crate) end: String,
    /// Inclusive lower bound on `value_quantity`.
    pub(crate) value_min: f64,
    /// Inclusive upper bound on `value_quantity`.
    pub(crate) value_max: f64,
    /// Bucketing: currently only `"day"` (the default) is supported.
    pub(crate) bucket: Option<String>,
    /// Allowed gap, in seconds, between consecutive in-range intervals before a
    /// run breaks. Defaults to 0.
    pub(crate) gap_seconds: Option<i64>,
}
```

- [ ] **Step 2: Write the failing tool test**

In `mod tests`, add (uses the AASM sleep-stage coding; references the not-yet-existing method):

```rust
    async fn fresh_server_with_sleep_epochs() -> ChartPdsServer {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");

        let key = BlobKey::from_hex_str(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .expect("valid key");
        let doc_id = insert_source_document(
            &pool,
            InsertSourceDocumentParams {
                archive_key: &key,
                kind: "ccda",
                source: "test",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
            },
        )
        .await
        .expect("doc");

        // Two contiguous 5-min asleep epochs (N2, N3) => a 10-minute run.
        for (start_end, stage) in [
            ((datetime!(2026-01-01 22:00:00 UTC), datetime!(2026-01-01 22:05:00 UTC)), 2.0),
            ((datetime!(2026-01-01 22:05:00 UTC), datetime!(2026-01-01 22:10:00 UTC)), 3.0),
        ] {
            insert_observation(
                &pool,
                InsertObservationParams {
                    source_document_id: doc_id,
                    coding_system: "https://chartpds.fhwang.net/coding/aasm/sleep-stage",
                    coding_code: "aasm-sleep-stage",
                    coding_display: Some("Sleep stage"),
                    effective_start: start_end.0,
                    effective_end: Some(start_end.1),
                    value_quantity: Some(stage),
                    value_string: None,
                    value_unit: None,
                },
            )
            .await
            .expect("obs");
        }

        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        ChartPdsServer::new(pool, archive, None, reqwest::Client::new())
    }

    #[tokio::test]
    async fn observation_longest_period_in_range_reports_per_day_run() {
        let server = fresh_server_with_sleep_epochs().await;
        let result = server
            .observation_longest_period_in_range(Parameters(ObservationLongestPeriodInRangeArgs {
                coding: Coding {
                    system: "https://chartpds.fhwang.net/coding/aasm/sleep-stage".to_string(),
                    code: "aasm-sleep-stage".to_string(),
                },
                start: "2026-01-01T00:00:00Z".to_string(),
                end: "2026-01-02T00:00:00Z".to_string(),
                value_min: 1.0,
                value_max: 4.0,
                bucket: Some("day".to_string()),
                gap_seconds: Some(0),
            }))
            .await
            .expect("tool call succeeds");

        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        let arr = value["per_bucket"].as_array().expect("array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["bucket_start"], "2026-01-01");
        assert_eq!(arr[0]["longest_minutes"], 10.0);
    }
```

- [ ] **Step 3: Run the test, verify it fails to compile**

Run: `cargo test -p chartpds-mcp observation_longest_period_in_range_reports_per_day_run`
Expected: FAIL — `no method named observation_longest_period_in_range`.

- [ ] **Step 4: Implement the tool handler**

Inside the `#[tool_router]` impl block, after `observation_duration_in_range`:

```rust
    #[tool(
        description = "Longest unbroken run of in-range observations per UTC day. Args: coding {system, code}, start/end (RFC 3339, half-open), value_min/value_max (inclusive), gap_seconds (allowed gap between consecutive in-range intervals before a run breaks; default 0). bucket currently only \"day\" (default). Returns {per_bucket:[{bucket_start, longest_minutes}]}."
    )]
    async fn observation_longest_period_in_range(
        &self,
        Parameters(args): Parameters<ObservationLongestPeriodInRangeArgs>,
    ) -> Result<CallToolResult, McpError> {
        let start = time::OffsetDateTime::parse(&args.start, &Rfc3339)
            .map_err(|err| McpError::invalid_params(format!("invalid start: {err}"), None))?;
        let end = time::OffsetDateTime::parse(&args.end, &Rfc3339)
            .map_err(|err| McpError::invalid_params(format!("invalid end: {err}"), None))?;
        match args.bucket.as_deref() {
            None | Some("day") => {}
            Some(other) => {
                return Err(McpError::invalid_params(
                    format!("invalid bucket {other:?}; only \"day\" is supported"),
                    None,
                ))
            }
        }
        let gap_seconds = args.gap_seconds.unwrap_or(0);

        let result = chartpds_core::queries::longest_continuous_in_value_range(
            &self.pool,
            &args.coding.system,
            &args.coding.code,
            start,
            end,
            args.value_min,
            args.value_max,
            gap_seconds,
        )
        .await
        .map_err(|err| McpError::internal_error(format!("query failed: {err}"), None))?;

        let json = serde_json::to_string(&result)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }
```

- [ ] **Step 5: Run the test, verify it passes**

Run: `cargo test -p chartpds-mcp observation_longest_period_in_range_reports_per_day_run`
Expected: PASS.

- [ ] **Step 6: Update the MCP tool count in docs/comments if asserted**

Run: `grep -rn "10 tools\|serves 10" crates/chartpds-mcp/src CLAUDE.md`
If any count/list of tools is asserted in code comments or `CLAUDE.md`, update it to include the two new tools (now 12). If the grep returns nothing in code, no change is needed (CLAUDE.md is updated in the finalization step).

- [ ] **Step 7: Commit**

```bash
git add crates/chartpds-mcp/src/server.rs
git commit -m "Add observation_longest_period_in_range MCP tool"
```

---

### Task 6: Finalize — docs + full `just check`

**Files:**
- Modify: `CLAUDE.md` (MCP tool list + Queries section)

- [ ] **Step 1: Update `CLAUDE.md` MCP server tool list**

In the "MCP server" section, update the tool count from "10 tools" to "12 tools" and add two bullets after `observation_counts`:

```markdown
- `observation_duration_in_range` — total minutes a coded signal spent in a value range
- `observation_longest_period_in_range` — longest continuous in-range run per day
```

- [ ] **Step 2: Update `CLAUDE.md` Queries section**

In the "Queries" section, extend the list of primitives:

```markdown
Currently: `latest_by_code`, `in_range`, `counts_per_code`,
`list_problems`, `list_medications`, `duration_in_value_range`,
`longest_continuous_in_value_range`.
```

Add a sentence noting both new primitives select by `{coding_system, coding_code}` and that day bucketing uses the UTC calendar day of the interval/run start.

- [ ] **Step 3: Run the full check gate**

Run: `just check`
Expected: PASS — fmt-check, lint (clippy `-D warnings`), typecheck, test (all prior tests + the new ones), `cargo deny`, `cargo machete`, `cargo sqlx prepare --check`.

If `sqlx prepare --check` reports drift, run `just prepare-sql`, `git add .sqlx`, and re-run `just check`.

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md .sqlx
git commit -m "Document new aggregator tools and queries"
```

---

## Self-Review

**Spec coverage:**
- #1 duration-in-zone aggregator → Task 1 (query) + Task 4 (tool). ✓
- #2 nightly sleep duration (`93832-4`) → Task 3. ✓
- #3 longest-continuous-period aggregator → Task 2 (query + pure walker) + Task 5 (tool). ✓
- Cross-cutting `{system, code}` selection → Tasks 1, 2 query signatures + `Coding` arg struct (Task 4). ✓
- UTC-day bucketing simplification → implemented in Tasks 1/2, documented in Task 6. ✓
- `samples_count` unchanged → Task 3 Step 8 note; verified by unchanged `ingest_session_updates_source_day_state`. ✓
- Rebuild correctness → no code needed: `index_sleep_session` is the shared write tail for sync and replay (verified in `storage.rs`); nightly rows are produced on rebuild automatically. ✓
- `just check` gate → Task 6. ✓

**Type consistency:** `Bucket`, `DurationInRange`/`BucketMinutes` (Task 1) reused by Task 4; `LongestContinuousInRange`/`BucketLongest` (Task 2) reused by Task 5; `Coding` (Task 4) reused by Task 5; `LOINC_SLEEP_DURATION` + `nightly_sleep_duration`/`ParsedSleepDuration` (Task 3) names match between definition and use. Query signatures in Tasks 1/2 match the calls in Tasks 4/5. ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code; every command has expected output. ✓
