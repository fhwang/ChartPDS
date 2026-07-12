# `observation_stats` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `observation_stats` MCP tool (GitHub issue #21): descriptive statistics (count, mean, sample sd, min/max, p25/p50/p75, optional threshold counts) for one coding's observations over a time window, optionally bucketed by day / ISO week / month / day-of-week in a request timezone.

**Architecture:** One new query primitive `crates/chartpds-core/src/queries/observation_stats.rs` following the existing `duration_in_value_range` pattern — a pure async function `(&SqlitePool, now, params) -> Result<T, Error>`. Rows are fetched to Rust (percentiles need the full sample anyway), field values are derived and bucketed with jiff (IANA tzdb is bundled), and per-bucket confidence reuses the existing `roll_up_bucket_confidence`. The MCP binary wires it up as a new `#[tool]` on `ChartPdsServer`.

**Tech Stack:** Rust stable, sqlx (offline mode — run `just prepare-sql` after adding SQL), jiff 0.2 (`tzdb-bundle-always`), `time` crate at the API boundary, rmcp for the tool surface.

## Global Constraints

- Run `just check` before declaring any change complete (fmt, clippy `-D warnings`, tests, cargo deny, cargo machete, sqlx prepare check, holdout).
- **Never bypass a lint rule.** `#[allow(...)]` requires `reason = "..."`. Refactor instead of relaxing lints.
- Every `pub` item (including struct fields) needs a doc comment (`missing_docs` is promoted to an error by `-D warnings`).
- After any new `sqlx::query!` invocation, run `just prepare-sql` and commit the `.sqlx/` cache updates in the same commit.
- Do NOT touch anything under `holdout/`, `holdout.lock`, `.github/allowed_signers`, or `.github/workflows/holdout.yml`. If a holdout test fails, STOP and report.
- `chartpds-core` internals stay `pub(crate)` unless the binary needs them; the binary only calls `chartpds_core::queries::*` re-exports.
- Work in the worktree at `.claude/worktrees/issue-21-observation-stats` (branch `worktree-issue-21-observation-stats`, based on origin/main `54bb364`). All paths below are relative to that worktree root.

## Locked design decisions (from issue #21 + this plan)

- **Fields:** `value` (= `value_quantity`), `start_time_of_day` / `end_time_of_day` (minutes since **local noon** in the request timezone, range `[0, 1440)` — `22:16 → 616`, `01:08 → 788`), `interval_minutes` (`effective_end − effective_start` in minutes, fractional). Observations lacking the requested field are excluded; `count` reflects rows actually aggregated.
- **Buckets:** `none` | `day` (`YYYY-MM-DD`) | `week` (ISO week, keyed by the Monday date `YYYY-MM-DD`) | `month` (`YYYY-MM`) | `day_of_week` (`mon` … `sun`, output in Monday-first order). Assignment is by `effective_start` in the request timezone. Empty buckets are omitted.
- **Stats object:** `count`, `mean`, `sd` (sample, n−1, `null` when `count` < 2), `min`, `max`, `p25`, `p50`, `p75`, optional `thresholds: [{threshold, n_below, n_at_or_above}]` (present iff the request passed `thresholds`; `n_below` = strictly less), `confidence` (`"provisional"` if any aggregated observation's source-day is provisional, else `"confirmed"`). All statistics `null` when `count` is 0 (only possible for `bucket: "none"`).
- **Percentile method:** linear interpolation between closest ranks (R type 7 / numpy default): `rank = p·(n−1)`, interpolate between `floor(rank)` and `ceil(rank)`. The issue doesn't pin a method; this is the most common default.
- **Return shape:** `bucket:"none"` → single flat stats object. Otherwise `{"per_bucket": [{"bucket_key": "...", ...stats}]}` (untagged serde enum, like `DurationInRange`).
- **Confidence plumbing:** collect `(bucket_label, source, document_date)` contributions per aggregated row (join `source_documents`), feed the existing `pub` `roll_up_bucket_confidence`. For `bucket:"none"` use the single label `""`.
- **Errors:** mirror `DurationInRangeError`: `Db(#[from] sqlx::Error)`, `InvalidTimezone(String)`, `Internal(String)`. The MCP layer maps `InvalidTimezone` → `invalid_params`, the rest → `internal_error`.

---

### Task 1: Core query `observation_stats` in `chartpds-core`

**Files:**
- Create: `crates/chartpds-core/src/queries/observation_stats.rs`
- Modify: `crates/chartpds-core/src/queries/mod.rs`
- Generated: `.sqlx/query-*.json` (via `just prepare-sql`)

**Interfaces:**
- Consumes: `crate::queries::roll_up_bucket_confidence` (existing, `pub`), `crate::sources::DayConfidence` (existing), test helpers `crate::queries::test_support::{seed_observations, seed_interval_observations, ObsSpec, IntervalObsSpec}` (existing).
- Produces (Task 2 relies on these exact names, re-exported from `chartpds_core::queries`):
  - `pub async fn observation_stats(pool: &SqlitePool, now: OffsetDateTime, params: ObservationStatsParams<'_>) -> Result<ObservationStats, ObservationStatsError>`
  - `pub struct ObservationStatsParams<'a> { coding_system: &'a str, coding_code: &'a str, start: OffsetDateTime, end: OffsetDateTime, field: StatsField, bucket: StatsBucket, timezone: Option<&'a str>, thresholds: Option<&'a [f64]> }`
  - `pub enum StatsField { Value, StartTimeOfDay, EndTimeOfDay, IntervalMinutes }`
  - `pub enum StatsBucket { None, Day, Week, Month, DayOfWeek }`
  - `pub enum ObservationStats { Flat(StatsSummary), Buckets { per_bucket: Vec<BucketStats> } }` (serde untagged)
  - `pub struct StatsSummary { count: usize, mean/sd/min/max/p25/p50/p75: Option<f64>, thresholds: Option<Vec<ThresholdCount>>, confidence: DayConfidence }`
  - `pub struct BucketStats { bucket_key: String, #[serde(flatten)] stats: StatsSummary }`
  - `pub struct ThresholdCount { threshold: f64, n_below: usize, n_at_or_above: usize }`
  - `pub enum ObservationStatsError { Db(sqlx::Error), InvalidTimezone(String), Internal(String) }`

Note on commits: intermediate steps run only `cargo test -p chartpds-core observation_stats`; the full `just check` gate (which would flag dead code for not-yet-wired helpers) runs once at the end of the task, and the task is a single commit.

- [ ] **Step 1: Create the file with types, stats-core stubs, and failing unit tests**

Create `crates/chartpds-core/src/queries/observation_stats.rs` with the types, the `summarize`/`percentile` implementations left as `todo!()`, and the first test block. (Types are needed for the tests to name; `todo!()` bodies make the tests fail at runtime, which is the TDD red step.)

```rust
//! Descriptive statistics over one coding's observations.
//!
//! Fetches the matching rows and computes count / mean / sample sd / min /
//! max / p25 / p50 / p75 (plus optional threshold counts) in Rust —
//! percentiles need the full sample, so unlike `duration_in_value_range`
//! there is no SQL-side aggregation fast path. Bucketing (`day`, ISO `week`,
//! `month`, `day_of_week`) and the time-of-day fields are evaluated in the
//! request timezone via jiff.

use std::collections::BTreeMap;

use jiff::tz::TimeZone;
use jiff::{Span, Timestamp};
use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::queries::roll_up_bucket_confidence;
use crate::sources::DayConfidence;

/// Which per-observation number to aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsField {
    /// The observation's `value_quantity`.
    Value,
    /// `effective_start` as minutes since local noon, in `[0, 1440)`.
    ///
    /// Anchoring at noon instead of midnight keeps overnight intervals
    /// (sleep, night shifts) linear: 22:16 → 616.0, 01:08 → 788.0.
    StartTimeOfDay,
    /// `effective_end` as minutes since local noon, in `[0, 1440)`.
    EndTimeOfDay,
    /// `effective_end − effective_start` in minutes.
    IntervalMinutes,
}

/// How to group statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsBucket {
    /// One stats object over the whole window.
    None,
    /// Per local calendar day, keyed `YYYY-MM-DD`.
    Day,
    /// Per ISO week, keyed by the Monday date (`YYYY-MM-DD`).
    Week,
    /// Per calendar month, keyed `YYYY-MM`.
    Month,
    /// Per day of week, keyed `mon` … `sun` (output Monday-first).
    DayOfWeek,
}

/// Failure modes of [`observation_stats`].
#[derive(Debug, thiserror::Error)]
pub enum ObservationStatsError {
    /// The underlying SQL query failed.
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    /// The supplied `timezone` is not a known IANA zone name.
    #[error("invalid timezone: {0}")]
    InvalidTimezone(String),
    /// An internal date/time conversion failed unexpectedly.
    #[error("internal datetime error: {0}")]
    Internal(String),
}

/// Counts of field values below / at-or-above one threshold.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ThresholdCount {
    /// The threshold the counts are split on.
    pub threshold: f64,
    /// Number of field values strictly below the threshold.
    pub n_below: usize,
    /// Number of field values at or above the threshold.
    pub n_at_or_above: usize,
}

/// Descriptive statistics over one set of field values.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct StatsSummary {
    /// Number of observations actually aggregated (rows lacking the
    /// requested field are excluded before counting).
    pub count: usize,
    /// Arithmetic mean; `null` when `count` is 0.
    pub mean: Option<f64>,
    /// Sample standard deviation (n−1); `null` when `count` < 2.
    pub sd: Option<f64>,
    /// Smallest field value; `null` when `count` is 0.
    pub min: Option<f64>,
    /// Largest field value; `null` when `count` is 0.
    pub max: Option<f64>,
    /// 25th percentile (linear interpolation); `null` when `count` is 0.
    pub p25: Option<f64>,
    /// Median (linear interpolation); `null` when `count` is 0.
    pub p50: Option<f64>,
    /// 75th percentile (linear interpolation); `null` when `count` is 0.
    pub p75: Option<f64>,
    /// Per-threshold counts; omitted when the request had no `thresholds`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thresholds: Option<Vec<ThresholdCount>>,
    /// `Provisional` if any aggregated observation's source-day is
    /// provisional, else `Confirmed`.
    pub confidence: DayConfidence,
}

/// One bucket's key and statistics.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct BucketStats {
    /// Bucket key: `YYYY-MM-DD` (day / ISO-week Monday), `YYYY-MM`
    /// (month), or `mon` … `sun` (day of week).
    pub bucket_key: String,
    /// Statistics for the bucket.
    #[serde(flatten)]
    pub stats: StatsSummary,
}

/// Result of [`observation_stats`].
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(untagged)]
pub enum ObservationStats {
    /// Returned for [`StatsBucket::None`]: one flat stats object.
    Flat(StatsSummary),
    /// Returned for every other bucket: per-bucket stats, chronological
    /// (day / week / month) or Monday-first (day of week). Empty buckets
    /// are omitted.
    Buckets {
        /// The non-empty buckets.
        per_bucket: Vec<BucketStats>,
    },
}

/// Parameters for [`observation_stats`].
pub struct ObservationStatsParams<'a> {
    /// FHIR coding system URI.
    pub coding_system: &'a str,
    /// Coding code within `coding_system`.
    pub coding_code: &'a str,
    /// Start of the half-open window (inclusive) on `effective_start`.
    pub start: OffsetDateTime,
    /// End of the half-open window (exclusive) on `effective_start`.
    pub end: OffsetDateTime,
    /// Which per-observation number to aggregate.
    pub field: StatsField,
    /// How to group the result.
    pub bucket: StatsBucket,
    /// IANA timezone for bucket boundaries and time-of-day fields;
    /// `None` = UTC.
    pub timezone: Option<&'a str>,
    /// Optional thresholds; each reports counts below / at-or-above.
    pub thresholds: Option<&'a [f64]>,
}

/// `p`-th percentile (`0.0..=1.0`) of an ascending-sorted slice, by linear
/// interpolation between closest ranks (R type 7). `None` when empty.
fn percentile(sorted: &[f64], p: f64) -> Option<f64> {
    todo!()
}

/// Compute the summary statistics for one bucket's field values.
fn summarize(values: &[f64], thresholds: Option<&[f64]>, confidence: DayConfidence) -> StatsSummary {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(actual: Option<f64>, expected: f64) {
        let actual = actual.expect("value present");
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected ~{expected}, got {actual}"
        );
    }

    #[test]
    fn summarize_empty_is_all_null_with_zero_threshold_counts() {
        let s = summarize(&[], Some(&[360.0]), DayConfidence::Confirmed);
        assert_eq!(s.count, 0);
        assert_eq!(s.mean, None);
        assert_eq!(s.sd, None);
        assert_eq!(s.min, None);
        assert_eq!(s.max, None);
        assert_eq!(s.p25, None);
        assert_eq!(s.p50, None);
        assert_eq!(s.p75, None);
        assert_eq!(
            s.thresholds,
            Some(vec![ThresholdCount {
                threshold: 360.0,
                n_below: 0,
                n_at_or_above: 0,
            }])
        );
    }

    #[test]
    fn summarize_single_value_has_null_sd() {
        let s = summarize(&[42.0], None, DayConfidence::Confirmed);
        assert_eq!(s.count, 1);
        approx(s.mean, 42.0);
        assert_eq!(s.sd, None);
        approx(s.min, 42.0);
        approx(s.max, 42.0);
        approx(s.p25, 42.0);
        approx(s.p50, 42.0);
        approx(s.p75, 42.0);
        assert_eq!(s.thresholds, None);
    }

    #[test]
    fn summarize_known_set_matches_type7_percentiles() {
        // Deliberately unsorted input: summarize must sort internally.
        let s = summarize(&[3.0, 1.0, 4.0, 2.0], None, DayConfidence::Confirmed);
        assert_eq!(s.count, 4);
        approx(s.mean, 2.5);
        // Sample sd of 1..4: sqrt(5/3).
        approx(s.sd, (5.0f64 / 3.0).sqrt());
        approx(s.min, 1.0);
        approx(s.max, 4.0);
        // Type-7: rank = p·(n−1) over [1,2,3,4].
        approx(s.p25, 1.75);
        approx(s.p50, 2.5);
        approx(s.p75, 3.25);
    }

    #[test]
    fn summarize_threshold_split_is_strictly_below() {
        let s = summarize(
            &[1.0, 2.0, 3.0, 4.0],
            Some(&[3.0]),
            DayConfidence::Confirmed,
        );
        assert_eq!(
            s.thresholds,
            Some(vec![ThresholdCount {
                threshold: 3.0,
                n_below: 2,
                n_at_or_above: 2,
            }])
        );
    }

    #[test]
    fn summarize_carries_confidence_through() {
        let s = summarize(&[1.0], None, DayConfidence::Provisional);
        assert_eq!(s.confidence, DayConfidence::Provisional);
    }
}
```

Register the module in `crates/chartpds-core/src/queries/mod.rs` — add to the `mod` block (alphabetical, after `mod observation_history;`):

```rust
mod observation_stats;
```

(The `pub use` re-export is added in Step 7 when the full public API exists.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p chartpds-core observation_stats`
Expected: FAIL — each `summarize_*` test panics with `not yet implemented` (the `todo!()`s).

- [ ] **Step 3: Implement `percentile` and `summarize`**

Replace the two `todo!()` bodies:

```rust
/// `p`-th percentile (`0.0..=1.0`) of an ascending-sorted slice, by linear
/// interpolation between closest ranks (R type 7). `None` when empty.
fn percentile(sorted: &[f64], p: f64) -> Option<f64> {
    let last = sorted.len().checked_sub(1)?;
    #[allow(
        clippy::cast_precision_loss,
        reason = "observation counts fit f64 without loss"
    )]
    let rank = p * last as f64;
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "rank is non-negative and bounded by len-1, so floor fits usize"
    )]
    let lo = rank.floor() as usize;
    let hi = (lo + 1).min(last);
    let frac = rank - rank.floor();
    Some(sorted[lo] + (sorted[hi] - sorted[lo]) * frac)
}

/// Compute the summary statistics for one bucket's field values.
fn summarize(values: &[f64], thresholds: Option<&[f64]>, confidence: DayConfidence) -> StatsSummary {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let count = sorted.len();
    #[allow(
        clippy::cast_precision_loss,
        reason = "observation counts fit f64 without loss"
    )]
    let n = count as f64;
    let mean = (count > 0).then(|| sorted.iter().sum::<f64>() / n);
    let sd = match mean {
        Some(m) if count > 1 => {
            let variance = sorted
                .iter()
                .map(|v| {
                    let d = v - m;
                    d * d
                })
                .sum::<f64>()
                / (n - 1.0);
            Some(variance.sqrt())
        }
        _ => None,
    };
    let threshold_counts = thresholds.map(|ts| {
        ts.iter()
            .map(|&threshold| {
                let n_below = sorted.iter().filter(|&&v| v < threshold).count();
                ThresholdCount {
                    threshold,
                    n_below,
                    n_at_or_above: count - n_below,
                }
            })
            .collect()
    });
    StatsSummary {
        count,
        mean,
        sd,
        min: sorted.first().copied(),
        max: sorted.last().copied(),
        p25: percentile(&sorted, 0.25),
        p50: percentile(&sorted, 0.5),
        p75: percentile(&sorted, 0.75),
        thresholds: threshold_counts,
        confidence,
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p chartpds-core observation_stats`
Expected: PASS (5 tests).

- [ ] **Step 5: Add failing unit tests for field derivation and bucket keys**

Append inside `mod tests`:

```rust
    use time::macros::datetime;

    fn ny() -> TimeZone {
        TimeZone::get("America/New_York").expect("tzdb has New York")
    }

    #[test]
    fn minutes_since_noon_anchors_at_local_noon() {
        // 22:16 UTC → 616; 01:08 UTC → 788; noon → 0 (issue #21 examples).
        approx(
            Some(minutes_since_noon(datetime!(2026-06-27 22:16:00 UTC), &TimeZone::UTC).expect("derive")),
            616.0,
        );
        approx(
            Some(minutes_since_noon(datetime!(2026-06-27 01:08:00 UTC), &TimeZone::UTC).expect("derive")),
            788.0,
        );
        approx(
            Some(minutes_since_noon(datetime!(2026-06-27 12:00:00 UTC), &TimeZone::UTC).expect("derive")),
            0.0,
        );
    }

    #[test]
    fn minutes_since_noon_uses_request_timezone() {
        // 02:16Z on 2026-06-27 is 22:16 EDT the previous evening → 616.
        approx(
            Some(minutes_since_noon(datetime!(2026-06-27 02:16:00 UTC), &ny()).expect("derive")),
            616.0,
        );
    }

    #[test]
    fn bucket_key_day_cuts_at_local_midnight() {
        // 03:30Z on 2026-06-27 is 23:30 NY the previous evening.
        let (_, label) =
            bucket_key(datetime!(2026-06-27 03:30:00 UTC), StatsBucket::Day, &ny()).expect("key");
        assert_eq!(label, "2026-06-26");
    }

    #[test]
    fn bucket_key_week_is_iso_monday_even_across_year_boundary() {
        // 2026-01-01 is a Thursday; its ISO week's Monday is 2025-12-29.
        let (_, label) = bucket_key(
            datetime!(2026-01-01 08:00:00 UTC),
            StatsBucket::Week,
            &TimeZone::UTC,
        )
        .expect("key");
        assert_eq!(label, "2025-12-29");
    }

    #[test]
    fn bucket_key_month_and_day_of_week() {
        let (_, month) = bucket_key(
            datetime!(2026-01-01 08:00:00 UTC),
            StatsBucket::Month,
            &TimeZone::UTC,
        )
        .expect("key");
        assert_eq!(month, "2026-01");

        // Thursday → index 3, label "thu"; Monday sorts before it.
        let (thu_idx, thu) = bucket_key(
            datetime!(2026-01-01 08:00:00 UTC),
            StatsBucket::DayOfWeek,
            &TimeZone::UTC,
        )
        .expect("key");
        let (mon_idx, mon) = bucket_key(
            datetime!(2026-01-05 08:00:00 UTC),
            StatsBucket::DayOfWeek,
            &TimeZone::UTC,
        )
        .expect("key");
        assert_eq!((thu_idx, thu.as_str()), (3, "thu"));
        assert_eq!((mon_idx, mon.as_str()), (0, "mon"));
    }
```

- [ ] **Step 6: Run to verify the new tests fail to compile, then implement the helpers**

Run: `cargo test -p chartpds-core observation_stats`
Expected: COMPILE FAIL — `minutes_since_noon` and `bucket_key` are not defined.

Then add above `percentile`:

```rust
/// Convert an `OffsetDateTime` (second precision) to a jiff `Zoned` in `tz`.
fn to_zoned(dt: OffsetDateTime, tz: &TimeZone) -> Result<jiff::Zoned, ObservationStatsError> {
    let ts = Timestamp::from_second(dt.unix_timestamp())
        .map_err(|err| ObservationStatsError::Internal(err.to_string()))?;
    Ok(ts.to_zoned(tz.clone()))
}

/// Minutes since local noon in `tz`, in `[0, 1440)`.
///
/// Noon anchoring keeps overnight timings linear (22:16 → 616, 01:08 → 788),
/// which is the useful behavior for sleep/night-shift statistics.
fn minutes_since_noon(dt: OffsetDateTime, tz: &TimeZone) -> Result<f64, ObservationStatsError> {
    let t = to_zoned(dt, tz)?.datetime().time();
    let seconds =
        i64::from(t.hour()) * 3600 + i64::from(t.minute()) * 60 + i64::from(t.second());
    #[allow(
        clippy::cast_precision_loss,
        reason = "seconds within a day fit f64 without loss"
    )]
    let minutes = seconds as f64 / 60.0;
    Ok((minutes - 720.0).rem_euclid(1440.0))
}

/// Sort-key + output label of the bucket `effective_start` falls in.
///
/// The `u8` prefix keeps `day_of_week` buckets in Monday-first order inside a
/// `BTreeMap`; it is 0 for every other bucket kind, whose labels already sort
/// chronologically.
fn bucket_key(
    dt: OffsetDateTime,
    bucket: StatsBucket,
    tz: &TimeZone,
) -> Result<(u8, String), ObservationStatsError> {
    if bucket == StatsBucket::None {
        return Ok((0, String::new()));
    }
    let date = to_zoned(dt, tz)?.datetime().date();
    match bucket {
        StatsBucket::None => Ok((0, String::new())),
        StatsBucket::Day => Ok((0, date.to_string())),
        StatsBucket::Week => {
            let back = i64::from(date.weekday().to_monday_zero_offset());
            let monday = date
                .checked_sub(Span::new().days(back))
                .map_err(|err| ObservationStatsError::Internal(err.to_string()))?;
            Ok((0, monday.to_string()))
        }
        StatsBucket::Month => Ok((0, format!("{:04}-{:02}", date.year(), date.month()))),
        StatsBucket::DayOfWeek => {
            const LABELS: [&str; 7] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
            let idx = u8::try_from(date.weekday().to_monday_zero_offset())
                .map_err(|err| ObservationStatsError::Internal(err.to_string()))?;
            Ok((idx, LABELS[usize::from(idx)].to_string()))
        }
    }
}
```

Run: `cargo test -p chartpds-core observation_stats`
Expected: PASS (10 tests).

- [ ] **Step 7: Add failing integration tests for the full query**

Append inside `mod tests`:

```rust
    use crate::queries::test_support::{
        seed_interval_observations, seed_observations, IntervalObsSpec, ObsSpec,
    };

    const NOW: OffsetDateTime = datetime!(2026-07-01 00:00:00 UTC);

    fn base_params<'a>(field: StatsField, bucket: StatsBucket) -> ObservationStatsParams<'a> {
        ObservationStatsParams {
            coding_system: "http://loinc.org",
            coding_code: "93832-4",
            start: datetime!(2026-01-01 00:00:00 UTC),
            end: datetime!(2026-02-01 00:00:00 UTC),
            field,
            bucket,
            timezone: None,
            thresholds: None,
        }
    }

    fn nightly_specs() -> [ObsSpec; 3] {
        [
            ObsSpec {
                coding_code: "93832-4",
                coding_display: None,
                effective_start: datetime!(2026-01-01 07:00:00 UTC),
                value_quantity: Some(400.0),
                value_unit: Some("min"),
            },
            ObsSpec {
                coding_code: "93832-4",
                coding_display: None,
                effective_start: datetime!(2026-01-02 07:00:00 UTC),
                value_quantity: Some(420.0),
                value_unit: Some("min"),
            },
            // No value_quantity: excluded from "value" statistics.
            ObsSpec {
                coding_code: "93832-4",
                coding_display: None,
                effective_start: datetime!(2026-01-03 07:00:00 UTC),
                value_quantity: None,
                value_unit: None,
            },
        ]
    }

    fn expect_flat(result: ObservationStats) -> StatsSummary {
        match result {
            ObservationStats::Flat(s) => s,
            ObservationStats::Buckets { .. } => panic!("expected flat stats"),
        }
    }

    fn expect_buckets(result: ObservationStats) -> Vec<BucketStats> {
        match result {
            ObservationStats::Buckets { per_bucket } => per_bucket,
            ObservationStats::Flat(_) => panic!("expected per_bucket stats"),
        }
    }

    #[tokio::test]
    async fn flat_value_stats_exclude_rows_without_value() {
        let (pool, _) = seed_observations(&nightly_specs()).await;
        let result = observation_stats(&pool, NOW, base_params(StatsField::Value, StatsBucket::None))
            .await
            .expect("query");
        let s = expect_flat(result);
        assert_eq!(s.count, 2);
        approx(s.mean, 410.0);
        approx(s.sd, 200.0f64.sqrt());
        approx(s.min, 400.0);
        approx(s.max, 420.0);
        approx(s.p50, 410.0);
        assert_eq!(s.confidence, DayConfidence::Confirmed);
    }

    #[tokio::test]
    async fn empty_window_is_count_zero_all_null() {
        let (pool, _) = seed_observations(&[]).await;
        let result = observation_stats(&pool, NOW, base_params(StatsField::Value, StatsBucket::None))
            .await
            .expect("query");
        let s = expect_flat(result);
        assert_eq!(s.count, 0);
        assert_eq!(s.mean, None);
        assert_eq!(s.sd, None);
        assert_eq!(s.p50, None);
        assert_eq!(s.confidence, DayConfidence::Confirmed);
    }

    #[tokio::test]
    async fn interval_minutes_excludes_rows_without_end() {
        // seed_observations rows carry no effective_end → all excluded.
        let (pool, _) = seed_observations(&nightly_specs()).await;
        let result = observation_stats(
            &pool,
            NOW,
            base_params(StatsField::IntervalMinutes, StatsBucket::None),
        )
        .await
        .expect("query");
        let s = expect_flat(result);
        assert_eq!(s.count, 0);
    }

    #[tokio::test]
    async fn interval_minutes_measures_interval_rows() {
        let (pool, _) = seed_interval_observations(&[
            IntervalObsSpec {
                coding_system: "http://loinc.org",
                coding_code: "93832-4",
                effective_start: datetime!(2026-01-01 07:00:00 UTC),
                effective_end: datetime!(2026-01-01 07:01:00 UTC),
                value_quantity: 1.0,
            },
            IntervalObsSpec {
                coding_system: "http://loinc.org",
                coding_code: "93832-4",
                effective_start: datetime!(2026-01-02 07:00:00 UTC),
                effective_end: datetime!(2026-01-02 07:05:00 UTC),
                value_quantity: 1.0,
            },
        ])
        .await;
        let result = observation_stats(
            &pool,
            NOW,
            base_params(StatsField::IntervalMinutes, StatsBucket::None),
        )
        .await
        .expect("query");
        let s = expect_flat(result);
        assert_eq!(s.count, 2);
        approx(s.mean, 3.0);
        approx(s.min, 1.0);
        approx(s.max, 5.0);
    }

    #[tokio::test]
    async fn start_time_of_day_uses_request_timezone() {
        // 02:16Z is 22:16 EDT the previous evening → 616 minutes since noon.
        let (pool, _) = seed_observations(&[ObsSpec {
            coding_code: "93832-4",
            coding_display: None,
            effective_start: datetime!(2026-01-15 02:16:00 UTC),
            value_quantity: Some(1.0),
            value_unit: None,
        }])
        .await;
        let mut params = base_params(StatsField::StartTimeOfDay, StatsBucket::None);
        params.timezone = Some("America/New_York");
        let result = observation_stats(&pool, NOW, params).await.expect("query");
        let s = expect_flat(result);
        assert_eq!(s.count, 1);
        approx(s.p50, 616.0);
    }

    #[tokio::test]
    async fn day_bucket_assigns_by_local_start_day() {
        // 03:30Z Jan 15 is 22:30 NY Jan 14; 13:00Z Jan 15 is 08:00 NY Jan 15.
        let (pool, _) = seed_observations(&[
            ObsSpec {
                coding_code: "93832-4",
                coding_display: None,
                effective_start: datetime!(2026-01-15 03:30:00 UTC),
                value_quantity: Some(400.0),
                value_unit: None,
            },
            ObsSpec {
                coding_code: "93832-4",
                coding_display: None,
                effective_start: datetime!(2026-01-15 13:00:00 UTC),
                value_quantity: Some(420.0),
                value_unit: None,
            },
        ])
        .await;
        let mut params = base_params(StatsField::Value, StatsBucket::Day);
        params.timezone = Some("America/New_York");
        let result = observation_stats(&pool, NOW, params).await.expect("query");
        let buckets = expect_buckets(result);
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].bucket_key, "2026-01-14");
        assert_eq!(buckets[0].stats.count, 1);
        assert_eq!(buckets[1].bucket_key, "2026-01-15");
        assert_eq!(buckets[1].stats.count, 1);
    }

    #[tokio::test]
    async fn week_bucket_groups_by_iso_monday() {
        // Thu Jan 1 + Fri Jan 2 share ISO week 2025-12-29; Mon Jan 5 starts a new one.
        let (pool, _) = seed_observations(&[
            ObsSpec {
                coding_code: "93832-4",
                coding_display: None,
                effective_start: datetime!(2026-01-01 07:00:00 UTC),
                value_quantity: Some(400.0),
                value_unit: None,
            },
            ObsSpec {
                coding_code: "93832-4",
                coding_display: None,
                effective_start: datetime!(2026-01-02 07:00:00 UTC),
                value_quantity: Some(420.0),
                value_unit: None,
            },
            ObsSpec {
                coding_code: "93832-4",
                coding_display: None,
                effective_start: datetime!(2026-01-05 07:00:00 UTC),
                value_quantity: Some(410.0),
                value_unit: None,
            },
        ])
        .await;
        let result =
            observation_stats(&pool, NOW, base_params(StatsField::Value, StatsBucket::Week))
                .await
                .expect("query");
        let buckets = expect_buckets(result);
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].bucket_key, "2025-12-29");
        assert_eq!(buckets[0].stats.count, 2);
        assert_eq!(buckets[1].bucket_key, "2026-01-05");
        assert_eq!(buckets[1].stats.count, 1);
    }

    #[tokio::test]
    async fn day_of_week_buckets_are_monday_first() {
        // Thu Jan 1 precedes Mon Jan 5 chronologically, but "mon" outputs first.
        let (pool, _) = seed_observations(&[
            ObsSpec {
                coding_code: "93832-4",
                coding_display: None,
                effective_start: datetime!(2026-01-01 07:00:00 UTC),
                value_quantity: Some(400.0),
                value_unit: None,
            },
            ObsSpec {
                coding_code: "93832-4",
                coding_display: None,
                effective_start: datetime!(2026-01-05 07:00:00 UTC),
                value_quantity: Some(420.0),
                value_unit: None,
            },
        ])
        .await;
        let result = observation_stats(
            &pool,
            NOW,
            base_params(StatsField::Value, StatsBucket::DayOfWeek),
        )
        .await
        .expect("query");
        let buckets = expect_buckets(result);
        let keys: Vec<&str> = buckets.iter().map(|b| b.bucket_key.as_str()).collect();
        assert_eq!(keys, vec!["mon", "thu"]);
    }

    #[tokio::test]
    async fn thresholds_are_reported_per_bucket_shape() {
        let (pool, _) = seed_observations(&nightly_specs()).await;
        let thresholds = [410.0];
        let mut params = base_params(StatsField::Value, StatsBucket::None);
        params.thresholds = Some(&thresholds);
        let result = observation_stats(&pool, NOW, params).await.expect("query");
        let s = expect_flat(result);
        assert_eq!(
            s.thresholds,
            Some(vec![ThresholdCount {
                threshold: 410.0,
                n_below: 1,
                n_at_or_above: 1,
            }])
        );
    }

    #[tokio::test]
    async fn invalid_timezone_is_an_error() {
        let (pool, _) = seed_observations(&[]).await;
        let mut params = base_params(StatsField::Value, StatsBucket::None);
        params.timezone = Some("Not/AZone");
        let err = observation_stats(&pool, NOW, params).await.unwrap_err();
        assert!(matches!(err, ObservationStatsError::InvalidTimezone(_)));
    }

    #[tokio::test]
    async fn provisional_source_day_marks_stats_provisional() {
        use crate::archive::BlobKey;
        use crate::index::{
            insert_observation, insert_source_document, open_pool, InsertObservationParams,
            InsertSourceDocumentParams,
        };

        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");

        // Fitbit doc with no freshness frontier → its day is provisional.
        let key = BlobKey::from_hex_str(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .expect("valid key");
        let doc_id = insert_source_document(
            &pool,
            InsertSourceDocumentParams {
                archive_key: &key,
                kind: "fitbit-heart-rate",
                source: "fitbit",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
                document_date: Some("2026-01-10"),
            },
        )
        .await
        .expect("doc");
        insert_observation(
            &pool,
            InsertObservationParams {
                source_document_id: doc_id,
                coding_system: "http://loinc.org",
                coding_code: "93832-4",
                coding_display: None,
                effective_start: datetime!(2026-01-10 07:00:00 UTC),
                effective_end: None,
                value_quantity: Some(400.0),
                value_string: None,
                value_unit: None,
            },
        )
        .await
        .expect("obs");

        let result =
            observation_stats(&pool, NOW, base_params(StatsField::Value, StatsBucket::None))
                .await
                .expect("query");
        let s = expect_flat(result);
        assert_eq!(s.confidence, DayConfidence::Provisional);
    }
```

- [ ] **Step 8: Run to verify the integration tests fail to compile**

Run: `cargo test -p chartpds-core observation_stats`
Expected: COMPILE FAIL — `observation_stats` (the function) is not defined.

- [ ] **Step 9: Implement `fetch_rows` and `observation_stats`, export from `mod.rs`**

Add between the params struct and `to_zoned`:

```rust
/// One fetched observation row plus its document's confidence keys.
struct ObsRow {
    effective_start: OffsetDateTime,
    effective_end: Option<OffsetDateTime>,
    value_quantity: Option<f64>,
    source: String,
    document_date: Option<String>,
}

/// The requested per-observation number, or `None` if the row lacks the field.
fn field_value(
    row: &ObsRow,
    field: StatsField,
    tz: &TimeZone,
) -> Result<Option<f64>, ObservationStatsError> {
    Ok(match field {
        StatsField::Value => row.value_quantity,
        StatsField::StartTimeOfDay => Some(minutes_since_noon(row.effective_start, tz)?),
        StatsField::EndTimeOfDay => match row.effective_end {
            Some(end) => Some(minutes_since_noon(end, tz)?),
            None => None,
        },
        StatsField::IntervalMinutes => match row.effective_end {
            Some(end) => {
                let seconds = end.unix_timestamp() - row.effective_start.unix_timestamp();
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "realistic interval seconds fit f64 without loss"
                )]
                Some(seconds as f64 / 60.0)
            }
            None => None,
        },
    })
}

/// Descriptive statistics for one coding's observations over `[start, end)`.
///
/// Matches observations by `coding_system`/`coding_code` with
/// `effective_start` in the half-open window. Rows lacking the requested
/// `field` (no `value_quantity` for [`StatsField::Value`], no
/// `effective_end` for [`StatsField::EndTimeOfDay`] /
/// [`StatsField::IntervalMinutes`]) are excluded; `count` reflects the rows
/// actually aggregated. Bucket assignment is by `effective_start` in the
/// request timezone; empty buckets are omitted.
///
/// # Errors
///
/// Returns [`ObservationStatsError::Db`] if a query fails,
/// [`ObservationStatsError::InvalidTimezone`] if `timezone` is not a known
/// IANA zone name, or [`ObservationStatsError::Internal`] if an internal
/// date/time conversion fails unexpectedly.
pub async fn observation_stats(
    pool: &SqlitePool,
    now: OffsetDateTime,
    params: ObservationStatsParams<'_>,
) -> Result<ObservationStats, ObservationStatsError> {
    let tz = match params.timezone {
        Some(name) => TimeZone::get(name)
            .map_err(|_| ObservationStatsError::InvalidTimezone(name.to_string()))?,
        None => TimeZone::UTC,
    };
    let rows = fetch_rows(pool, &params).await?;

    let mut grouped: BTreeMap<(u8, String), Vec<f64>> = BTreeMap::new();
    let mut contributions: Vec<(String, String, Option<String>)> = Vec::new();
    for row in &rows {
        let Some(value) = field_value(row, params.field, &tz)? else {
            continue;
        };
        let (idx, label) = bucket_key(row.effective_start, params.bucket, &tz)?;
        contributions.push((label.clone(), row.source.clone(), row.document_date.clone()));
        grouped.entry((idx, label)).or_default().push(value);
    }
    let confidence_by_bucket = roll_up_bucket_confidence(pool, now, &contributions).await?;
    let confidence_for = |label: &str| {
        confidence_by_bucket
            .get(label)
            .copied()
            .unwrap_or(DayConfidence::Confirmed)
    };

    if params.bucket == StatsBucket::None {
        let values = grouped.into_values().next().unwrap_or_default();
        return Ok(ObservationStats::Flat(summarize(
            &values,
            params.thresholds,
            confidence_for(""),
        )));
    }

    Ok(ObservationStats::Buckets {
        per_bucket: grouped
            .into_iter()
            .map(|((_, label), values)| BucketStats {
                stats: summarize(&values, params.thresholds, confidence_for(&label)),
                bucket_key: label,
            })
            .collect(),
    })
}

/// Fetch every matching row (with its document's confidence keys) for the
/// coding + window filter.
async fn fetch_rows(
    pool: &SqlitePool,
    params: &ObservationStatsParams<'_>,
) -> Result<Vec<ObsRow>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT o.effective_start AS "effective_start!: OffsetDateTime",
               o.effective_end AS "effective_end?: OffsetDateTime",
               o.value_quantity AS "value_quantity?: f64",
               sd.source AS "source!: String",
               sd.document_date AS "document_date?: String"
        FROM observations o
        JOIN source_documents sd ON o.source_document_id = sd.id
        WHERE o.coding_system = ?
          AND o.coding_code = ?
          AND o.effective_start >= ?
          AND o.effective_start < ?
        ORDER BY o.effective_start
        "#,
        params.coding_system,
        params.coding_code,
        params.start,
        params.end,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| ObsRow {
            effective_start: r.effective_start,
            effective_end: r.effective_end,
            value_quantity: r.value_quantity,
            source: r.source,
            document_date: r.document_date,
        })
        .collect())
}
```

In `crates/chartpds-core/src/queries/mod.rs`, add the re-export (alphabetical, after the `observation_history` `pub use`):

```rust
pub use observation_stats::{
    observation_stats, BucketStats, ObservationStats, ObservationStatsError,
    ObservationStatsParams, StatsBucket, StatsField, StatsSummary, ThresholdCount,
};
```

- [ ] **Step 10: Regenerate the sqlx offline cache**

Run: `just prepare-sql`
Expected: succeeds; new `.sqlx/query-*.json` file(s) appear (`git status` shows them).

- [ ] **Step 11: Run the tests to verify they pass**

Run: `cargo test -p chartpds-core observation_stats`
Expected: PASS (all unit + integration tests, 21 total).

- [ ] **Step 12: Run the full gate**

Run: `just check`
Expected: PASS (fmt, clippy, tests, deny, machete, sqlx prepare check, holdout). If clippy flags anything, fix the code — never add a bare `#[allow]`.

- [ ] **Step 13: Commit**

```bash
git add crates/chartpds-core/src/queries/observation_stats.rs \
        crates/chartpds-core/src/queries/mod.rs \
        .sqlx/
git commit -m "observation_stats query: descriptive statistics over an observation series

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: MCP tool `observation_stats` + docs

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs` (args struct near the other `*Args` structs ~line 110; tool fn inside the `#[tool_router]` block after `observation_longest_period_in_range` ~line 580; tests in the `#[cfg(test)]` module)
- Modify: `CLAUDE.md` (tool count + tool list + queries list)

**Interfaces:**
- Consumes (from Task 1, via `chartpds_core::queries::`): `observation_stats`, `ObservationStatsParams`, `StatsField::{Value, StartTimeOfDay, EndTimeOfDay, IntervalMinutes}`, `StatsBucket::{None, Day, Week, Month, DayOfWeek}`, `ObservationStatsError::{Db, InvalidTimezone, Internal}`. Also existing server items: `Coding` (args struct with `system`/`code`), `Parameters`, `CallToolResult`, `Content`, `McpError`, `Rfc3339`.
- Produces: MCP tool `observation_stats` (JSON described in issue #21).

- [ ] **Step 1: Write failing server tests**

In the `#[cfg(test)] mod tests` of `crates/chartpds-mcp/src/server.rs`, add a fixture and four tests:

```rust
    /// Three nightly total-sleep observations: 400, 420, 380 minutes.
    async fn fresh_server_with_nightly_sleep() -> ChartPdsServer {
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
                document_date: None,
            },
        )
        .await
        .expect("doc");
        for (start, minutes) in [
            (datetime!(2026-01-01 07:00:00 UTC), 400.0),
            (datetime!(2026-01-02 07:00:00 UTC), 420.0),
            (datetime!(2026-01-03 07:00:00 UTC), 380.0),
        ] {
            insert_observation(
                &pool,
                InsertObservationParams {
                    source_document_id: doc_id,
                    coding_system: "http://loinc.org",
                    coding_code: "93832-4",
                    coding_display: Some("Sleep duration"),
                    effective_start: start,
                    effective_end: None,
                    value_quantity: Some(minutes),
                    value_string: None,
                    value_unit: Some("min"),
                },
            )
            .await
            .expect("obs");
        }

        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        ChartPdsServer::new(pool, archive, None, reqwest::Client::new())
    }

    fn stats_args() -> ObservationStatsArgs {
        ObservationStatsArgs {
            coding: Coding {
                system: "http://loinc.org".to_string(),
                code: "93832-4".to_string(),
            },
            start: "2026-01-01T00:00:00Z".to_string(),
            end: "2026-02-01T00:00:00Z".to_string(),
            field: None,
            bucket: None,
            timezone: None,
            thresholds: None,
        }
    }

    #[tokio::test]
    async fn observation_stats_flat_defaults_to_value_field() {
        let server = fresh_server_with_nightly_sleep().await;
        let result = server
            .observation_stats(Parameters(stats_args()))
            .await
            .expect("tool call succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["count"], 3);
        assert_eq!(value["p50"], 400.0);
        assert_eq!(value["min"], 380.0);
        assert_eq!(value["max"], 420.0);
        assert_eq!(value["confidence"], "confirmed");
        // No thresholds requested → key omitted.
        assert!(value.get("thresholds").is_none());
    }

    #[tokio::test]
    async fn observation_stats_reports_thresholds() {
        let server = fresh_server_with_nightly_sleep().await;
        let mut args = stats_args();
        args.thresholds = Some(vec![400.0]);
        let result = server
            .observation_stats(Parameters(args))
            .await
            .expect("tool call succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        assert_eq!(value["thresholds"][0]["threshold"], 400.0);
        assert_eq!(value["thresholds"][0]["n_below"], 1);
        assert_eq!(value["thresholds"][0]["n_at_or_above"], 2);
    }

    #[tokio::test]
    async fn observation_stats_day_of_week_bucket_shape() {
        let server = fresh_server_with_nightly_sleep().await;
        let mut args = stats_args();
        args.bucket = Some("day_of_week".to_string());
        let result = server
            .observation_stats(Parameters(args))
            .await
            .expect("tool call succeeds");
        let text = match &result.content[0].raw {
            rmcp::model::RawContent::Text(t) => &t.text,
            _ => panic!("expected text content"),
        };
        let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
        // Jan 1/2/3 2026 are Thu/Fri/Sat → three buckets, Monday-first order.
        let keys: Vec<&str> = value["per_bucket"]
            .as_array()
            .expect("per_bucket array")
            .iter()
            .map(|b| b["bucket_key"].as_str().expect("key"))
            .collect();
        assert_eq!(keys, vec!["thu", "fri", "sat"]);
    }

    #[tokio::test]
    async fn observation_stats_rejects_unknown_field_and_bucket() {
        let server = fresh_server_with_nightly_sleep().await;
        let mut args = stats_args();
        args.field = Some("nope".to_string());
        let err = server
            .observation_stats(Parameters(args))
            .await
            .expect_err("unknown field");
        assert!(err.to_string().contains("invalid field"));

        let mut args = stats_args();
        args.bucket = Some("hour".to_string());
        let err = server
            .observation_stats(Parameters(args))
            .await
            .expect_err("unknown bucket");
        assert!(err.to_string().contains("invalid bucket"));
    }
```

- [ ] **Step 2: Run to verify they fail to compile**

Run: `cargo test -p chartpds-mcp observation_stats`
Expected: COMPILE FAIL — `ObservationStatsArgs` and the `observation_stats` method do not exist.

- [ ] **Step 3: Implement the args struct and tool**

Add after `ObservationLongestPeriodInRangeArgs` (~line 128):

```rust
/// Arguments for the `observation_stats` tool.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub(crate) struct ObservationStatsArgs {
    /// Coding to aggregate over.
    pub(crate) coding: Coding,
    /// Inclusive start of the time window (RFC 3339).
    pub(crate) start: String,
    /// Exclusive end of the time window (RFC 3339).
    pub(crate) end: String,
    /// Field to aggregate: `"value"` (default), `"start_time_of_day"`,
    /// `"end_time_of_day"` (minutes since local noon, `[0, 1440)`), or
    /// `"interval_minutes"`.
    pub(crate) field: Option<String>,
    /// Bucketing: `"none"` (default), `"day"`, `"week"` (ISO, keyed by
    /// Monday), `"month"`, or `"day_of_week"` (`mon` … `sun`).
    pub(crate) bucket: Option<String>,
    /// IANA timezone (e.g. `"America/New_York"`) governing bucket
    /// boundaries and time-of-day derivation. Omit for UTC.
    pub(crate) timezone: Option<String>,
    /// Optional thresholds; each reports counts below / at-or-above.
    pub(crate) thresholds: Option<Vec<f64>>,
}
```

Add inside the `#[tool_router]` impl, after `observation_longest_period_in_range`:

```rust
    #[tool(
        description = "Descriptive statistics (count, mean, sample sd, min/max, p25/p50/p75, optional threshold counts) for one coding's observations over a window. Args: coding {system, code}, start/end (RFC 3339, half-open), field (\"value\" default | \"start_time_of_day\" | \"end_time_of_day\" | \"interval_minutes\"; time-of-day fields are minutes since local noon in [0,1440) so overnight timings stay linear, e.g. 22:16 -> 616), bucket (\"none\" default | \"day\" | \"week\" ISO keyed by Monday | \"month\" | \"day_of_week\" mon..sun), timezone (IANA name, default UTC; governs bucket boundaries and time-of-day), thresholds (numbers; each reports n_below / n_at_or_above, n_below is strictly-less). Observations lacking the field are excluded and count reflects rows aggregated. bucket \"none\" returns one flat stats object (all stats null when count 0); otherwise {per_bucket:[{bucket_key, ...}]} with empty buckets omitted. sd is the sample sd (null when count < 2). confidence is \"provisional\" if any aggregated observation is provisional, else \"confirmed\"."
    )]
    async fn observation_stats(
        &self,
        Parameters(args): Parameters<ObservationStatsArgs>,
    ) -> Result<CallToolResult, McpError> {
        let start = time::OffsetDateTime::parse(&args.start, &Rfc3339)
            .map_err(|err| McpError::invalid_params(format!("invalid start: {err}"), None))?;
        let end = time::OffsetDateTime::parse(&args.end, &Rfc3339)
            .map_err(|err| McpError::invalid_params(format!("invalid end: {err}"), None))?;
        let field = match args.field.as_deref() {
            None | Some("value") => chartpds_core::queries::StatsField::Value,
            Some("start_time_of_day") => chartpds_core::queries::StatsField::StartTimeOfDay,
            Some("end_time_of_day") => chartpds_core::queries::StatsField::EndTimeOfDay,
            Some("interval_minutes") => chartpds_core::queries::StatsField::IntervalMinutes,
            Some(other) => {
                return Err(McpError::invalid_params(
                    format!(
                        "invalid field {other:?}; expected \"value\", \"start_time_of_day\", \"end_time_of_day\", or \"interval_minutes\""
                    ),
                    None,
                ))
            }
        };
        let bucket = match args.bucket.as_deref() {
            None | Some("none") => chartpds_core::queries::StatsBucket::None,
            Some("day") => chartpds_core::queries::StatsBucket::Day,
            Some("week") => chartpds_core::queries::StatsBucket::Week,
            Some("month") => chartpds_core::queries::StatsBucket::Month,
            Some("day_of_week") => chartpds_core::queries::StatsBucket::DayOfWeek,
            Some(other) => {
                return Err(McpError::invalid_params(
                    format!(
                        "invalid bucket {other:?}; expected \"none\", \"day\", \"week\", \"month\", or \"day_of_week\""
                    ),
                    None,
                ))
            }
        };

        let result = chartpds_core::queries::observation_stats(
            &self.pool,
            time::OffsetDateTime::now_utc(),
            chartpds_core::queries::ObservationStatsParams {
                coding_system: &args.coding.system,
                coding_code: &args.coding.code,
                start,
                end,
                field,
                bucket,
                timezone: args.timezone.as_deref(),
                thresholds: args.thresholds.as_deref(),
            },
        )
        .await
        .map_err(|err| match err {
            chartpds_core::queries::ObservationStatsError::InvalidTimezone(_) => {
                McpError::invalid_params(err.to_string(), None)
            }
            chartpds_core::queries::ObservationStatsError::Db(_)
            | chartpds_core::queries::ObservationStatsError::Internal(_) => {
                McpError::internal_error(format!("query failed: {err}"), None)
            }
        })?;

        let json = serde_json::to_string(&result)
            .map_err(|err| McpError::internal_error(format!("serializing: {err}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json)]))
    }
```

- [ ] **Step 4: Run the server tests to verify they pass**

Run: `cargo test -p chartpds-mcp observation_stats`
Expected: PASS (4 tests).

- [ ] **Step 5: Update CLAUDE.md**

Three edits in `CLAUDE.md`:

1. In the "MCP server" section, change `serves 13 tools` to `serves 14 tools`.
2. In the tool list, after the `observation_longest_period_in_range` bullet, add:

```markdown
- `observation_stats` — descriptive statistics (count, mean, sample sd,
  min/max, p25/p50/p75, optional threshold counts) for one coding over a
  window; `field` selects `value`, `start_time_of_day` / `end_time_of_day`
  (minutes since local noon), or `interval_minutes`; optional bucketing by
  `day` / `week` (ISO Monday) / `month` / `day_of_week` in a request timezone
```

3. In the "Queries" section, extend the primitives list: change

```markdown
Currently: `latest_by_code`, `observation_history`, `counts_per_code`,
`current_problems`, `current_medications`, `duration_in_value_range`,
`longest_continuous_in_value_range`.
```

to

```markdown
Currently: `latest_by_code`, `observation_history`, `counts_per_code`,
`current_problems`, `current_medications`, `duration_in_value_range`,
`longest_continuous_in_value_range`, `observation_stats`.
```

(If the existing wording differs slightly, keep the surrounding text and just add `observation_stats` to the list and bump the count.)

- [ ] **Step 6: Run the full gate**

Run: `just check`
Expected: PASS. If a holdout test fails, STOP and report — do not touch `holdout/`.

- [ ] **Step 7: Commit**

```bash
git add crates/chartpds-mcp/src/server.rs CLAUDE.md
git commit -m "observation_stats MCP tool

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Out of scope (per issue #21)

- Cross-metric analysis (correlating two codings' series) — natural follow-up, not here.
- Per-coding gating of fields/statistics — semantic fit is the caller's judgment.
- New storage or sync behavior — read-side only.
