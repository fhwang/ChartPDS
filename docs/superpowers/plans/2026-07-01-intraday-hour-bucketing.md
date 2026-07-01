# Intra-day hour bucketing + timezone Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the `observation_duration_in_range` MCP tool with an `"hour"` bucket granularity and an optional DST-aware IANA `timezone` parameter, so intra-day, wall-clock analysis (e.g. per-clock-hour wake minutes) is possible server-side.

**Architecture:** The value-range aggregation is unchanged. A new pure Rust bucketing helper, backed by `jiff` for IANA timezone math, computes local-time bucket keys. The existing SQL-only path stays for the back-compat cases (`none`, and `day` with no timezone); local-time paths (`hour` always, or any bucket + `timezone`) filter/duration in SQL, then bucket the shipped rows in Rust.

**Tech Stack:** Rust, sqlx (SQLite, offline mode), `time` (0.3), `jiff` (0.2, bundled tzdb), `thiserror`, rmcp (MCP server).

## Global Constraints

- **Toolchain / gate:** `just check` must pass (fmt-check, lint with `-D warnings`, typecheck, test, `cargo deny`, `cargo machete`, `cargo sqlx prepare --check`). Every `pub` item needs a doc comment or lint fails.
- **Never bypass a lint.** No `#[allow(...)]` without a `reason = "..."`. Follow existing `#[allow(clippy::cast_precision_loss, reason = "...")]` style already in the file.
- **sqlx offline mode:** after adding/changing any `sqlx::query!`, run `just prepare-sql` and commit the `.sqlx/` cache update in the same commit.
- **Module boundaries:** `jiff` stays an implementation detail of `chartpds-core`; it must NOT appear in any `pub` signature re-exported to the binary. Core exposes `timezone: Option<&str>`, never a `jiff` type.
- **Back-compat is mandatory:** with `timezone` omitted, `bucket:"none"` and `bucket:"day"` must produce byte-identical output to today (`day` still emits `"YYYY-MM-DD"` strings).
- **Holdout protected paths:** anything under `holdout/` requires a human-signed bless. You write and run the holdout test, then leave it **staged-but-uncommitted**. NEVER run `just holdout-bless`. NEVER edit other holdout files.
- **Commit author trailer** on every commit:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

### Key jiff/time API (verified against jiff 0.2.31, time 0.3)

```rust
use jiff::{Timestamp, tz::TimeZone};
use time::{Date, Month, Time, PrimitiveDateTime, OffsetDateTime, UtcOffset};
use time::format_description::well_known::Rfc3339;

// instant -> zoned local
let ts = Timestamp::from_second(odt.unix_timestamp())?;   // odt: time::OffsetDateTime
let tz = TimeZone::get("America/New_York")?;              // Err on unknown IANA name
let zoned = ts.to_zoned(tz.clone());                      // infallible; TimeZone is cheap to clone
let civ = zoned.datetime();                               // civil: .year() i16, .month() i8, .day() i8, .hour() i8
let off_secs: i32 = zoned.offset().seconds();

// HOUR bucket_start: truncate to civil hour, keep this instant's offset
let date = Date::from_calendar_date(i32::from(civ.year()),
    Month::try_from(u8::try_from(civ.month()).unwrap()).unwrap(),
    u8::try_from(civ.day()).unwrap())?;
let t = Time::from_hms(u8::try_from(civ.hour()).unwrap(), 0, 0)?;
let offset = UtcOffset::from_whole_seconds(off_secs)?;
let bucket_start: OffsetDateTime = PrimitiveDateTime::new(date, t).assume_offset(offset);

// DAY bucket_start: first instant of the local calendar day (offset consistent across the whole day)
let sod = zoned.start_of_day()?;                          // Result<Zoned>
let day_odt = OffsetDateTime::from_unix_timestamp(sod.timestamp().as_second())?
    .to_offset(UtcOffset::from_whole_seconds(sod.offset().seconds())?);

// RFC 3339 string for output (uses the stored offset: "...T02:00:00-04:00" or "...Z")
let s: String = bucket_start.format(&Rfc3339)?;
```

Grouping key: use `OffsetDateTime` as the `BTreeMap` key. `time`'s `Ord`/`Eq` compare by absolute instant, so two same-local-hour observations with the same offset merge, while the two `01:00` hours on a fall-back day (offsets −04:00 vs −05:00 → different instants) stay separate — the correct 25-slot behavior. The stored offset is preserved for formatting.

---

## Task 1: Add `jiff`, the error type, `Bucket::Hour`, and the pure local-bucket helper

**Files:**
- Modify: `crates/chartpds-core/Cargo.toml` (add `jiff`)
- Modify: `crates/chartpds-core/src/queries/duration_in_value_range.rs` (add `Bucket::Hour`, `DurationInRangeError`, pure helper + unit tests)

**Interfaces:**
- Produces:
  - `enum Bucket { None, Day, Hour }`
  - `enum DurationInRangeError { Db(sqlx::Error), InvalidTimezone(String) }` with `#[from] sqlx::Error`
  - private `fn bucket_local(rows: &[(OffsetDateTime, i64)], granularity: LocalGranularity, tz_name: Option<&str>) -> Result<Vec<BucketMinutes>, DurationInRangeError>` where `enum LocalGranularity { Hour, Day }` (private). Each tuple is `(effective_start, duration_seconds)`.

- [ ] **Step 1: Add the jiff dependency**

In `crates/chartpds-core/Cargo.toml`, under `[dependencies]` (alphabetical position, near `time`), add:

```toml
jiff = { version = "0.2", default-features = false, features = ["std", "tzdb-bundle-always"] }
```

Rationale for the features: `tzdb-bundle-always` embeds the IANA tzdb so tests/CI never depend on system zoneinfo (hermetic); `default-features = false` drops the platform-tzdb probing we don't want. This pulls only `jiff-tzdb` (both `Unlicense OR MIT` → deny-clean).

- [ ] **Step 2: Confirm it builds and clears deny**

Run: `cargo build -p chartpds-core && cargo deny check licenses bans 2>&1 | tail -5`
Expected: build succeeds; deny reports no license/duplicate errors involving `jiff` or `jiff-tzdb`.

- [ ] **Step 3: Write the failing unit tests for the pure helper**

Add to the `#[cfg(test)] mod tests` block in `duration_in_value_range.rs` (imports `use time::macros::datetime;` already present):

```rust
#[test]
fn hour_utc_buckets_align_to_utc_top_of_hour() {
    // One 5-min interval at 06:30Z contributes to the 06:00Z hour.
    let rows = [(datetime!(2026-06-27 06:30:00 UTC), 300i64)];
    let out = bucket_local(&rows, LocalGranularity::Hour, None).expect("bucket");
    assert_eq!(
        out,
        vec![BucketMinutes { bucket_start: "2026-06-27T06:00:00Z".into(), total_minutes: 5.0 }]
    );
}

#[test]
fn hour_local_buckets_shift_by_zone_offset() {
    // 06:30Z is 02:30 in America/New_York (EDT, -04:00) -> the 02:00-04:00 hour.
    let rows = [(datetime!(2026-06-27 06:30:00 UTC), 300i64)];
    let out =
        bucket_local(&rows, LocalGranularity::Hour, Some("America/New_York")).expect("bucket");
    assert_eq!(
        out,
        vec![BucketMinutes {
            bucket_start: "2026-06-27T02:00:00-04:00".into(),
            total_minutes: 5.0
        }]
    );
}

#[test]
fn hour_local_sums_within_a_bucket_and_sorts() {
    // Two intervals in the 02:00 NY hour (10 min) + one in the 03:00 NY hour (5 min).
    let rows = [
        (datetime!(2026-06-27 06:05:00 UTC), 300i64), // 02:05 NY
        (datetime!(2026-06-27 06:40:00 UTC), 300i64), // 02:40 NY
        (datetime!(2026-06-27 07:15:00 UTC), 300i64), // 03:15 NY
    ];
    let out =
        bucket_local(&rows, LocalGranularity::Hour, Some("America/New_York")).expect("bucket");
    assert_eq!(
        out,
        vec![
            BucketMinutes { bucket_start: "2026-06-27T02:00:00-04:00".into(), total_minutes: 10.0 },
            BucketMinutes { bucket_start: "2026-06-27T03:00:00-04:00".into(), total_minutes: 5.0 },
        ]
    );
}

#[test]
fn day_local_cuts_at_local_midnight() {
    // 03:30Z on 2026-06-27 is 23:30 the previous NY evening -> the 06-26 local day.
    let rows = [(datetime!(2026-06-27 03:30:00 UTC), 300i64)];
    let out =
        bucket_local(&rows, LocalGranularity::Day, Some("America/New_York")).expect("bucket");
    assert_eq!(
        out,
        vec![BucketMinutes {
            bucket_start: "2026-06-26T00:00:00-04:00".into(),
            total_minutes: 5.0
        }]
    );
}

#[test]
fn dst_fall_back_day_keeps_two_distinct_1am_hours() {
    // 2026-11-01 fall-back: 05:30Z is 01:30 EDT (-04:00), 06:30Z is 01:30 EST (-05:00).
    // Wall-clock 01:00 occurs twice -> two distinct buckets (the 25-hour day).
    let rows = [
        (datetime!(2026-11-01 05:30:00 UTC), 300i64),
        (datetime!(2026-11-01 06:30:00 UTC), 300i64),
    ];
    let out =
        bucket_local(&rows, LocalGranularity::Hour, Some("America/New_York")).expect("bucket");
    assert_eq!(
        out,
        vec![
            BucketMinutes { bucket_start: "2026-11-01T01:00:00-04:00".into(), total_minutes: 5.0 },
            BucketMinutes { bucket_start: "2026-11-01T01:00:00-05:00".into(), total_minutes: 5.0 },
        ]
    );
}

#[test]
fn invalid_timezone_is_an_error() {
    let rows = [(datetime!(2026-06-27 06:30:00 UTC), 300i64)];
    let err = bucket_local(&rows, LocalGranularity::Hour, Some("Not/AZone")).unwrap_err();
    assert!(matches!(err, DurationInRangeError::InvalidTimezone(_)));
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cargo test -p chartpds-core duration_in_value_range 2>&1 | tail -20`
Expected: compile error / FAIL — `bucket_local`, `LocalGranularity`, `Bucket::Hour`, `DurationInRangeError` do not exist yet.

- [ ] **Step 5: Add `Bucket::Hour`, the error enum, and the helper**

In `duration_in_value_range.rs`:

Add imports at the top (after existing `use` lines):

```rust
use std::collections::BTreeMap;

use jiff::{tz::TimeZone, Timestamp};
use time::format_description::well_known::Rfc3339;
use time::{Date, Month, PrimitiveDateTime, Time, UtcOffset};
```

Extend the `Bucket` enum with a doc-commented variant:

```rust
    /// One aggregate per clock hour (local to `timezone`, else UTC).
    Hour,
```

Add the error type (near the top-level types):

```rust
/// Failure modes of [`duration_in_value_range`].
#[derive(Debug, thiserror::Error)]
pub enum DurationInRangeError {
    /// The underlying SQL query failed.
    #[error(transparent)]
    Db(#[from] sqlx::Error),
    /// The supplied `timezone` is not a known IANA zone name.
    #[error("invalid timezone: {0}")]
    InvalidTimezone(String),
}
```

Add the private granularity enum and the pure helper (place above `duration_in_value_range`):

```rust
/// Local-time bucket granularity for the Rust-side aggregation path.
#[derive(Debug, Clone, Copy)]
enum LocalGranularity {
    Hour,
    Day,
}

/// Bucket `(effective_start, duration_seconds)` rows by local hour or day.
///
/// `tz_name` is an IANA zone name; `None` means UTC. Each interval is credited
/// whole to the bucket of its `effective_start` (matching the SQL day path).
/// `bucket_start` is emitted as RFC 3339 with the bucket's local offset.
fn bucket_local(
    rows: &[(OffsetDateTime, i64)],
    granularity: LocalGranularity,
    tz_name: Option<&str>,
) -> Result<Vec<BucketMinutes>, DurationInRangeError> {
    let tz = match tz_name {
        Some(name) => TimeZone::get(name)
            .map_err(|_| DurationInRangeError::InvalidTimezone(name.to_string()))?,
        None => TimeZone::UTC,
    };

    // Key by the truncated instant; time's Ord/Eq compare by absolute instant,
    // so same-local-hour rows merge and the two fall-back 01:00 hours stay split.
    let mut totals: BTreeMap<OffsetDateTime, i64> = BTreeMap::new();
    for (effective_start, duration_seconds) in rows {
        let ts = Timestamp::from_second(effective_start.unix_timestamp())
            .map_err(|err| DurationInRangeError::InvalidTimezone(err.to_string()))?;
        let zoned = ts.to_zoned(tz.clone());
        let bucket_start = match granularity {
            LocalGranularity::Hour => {
                let civ = zoned.datetime();
                let date = Date::from_calendar_date(
                    i32::from(civ.year()),
                    month_from(civ.month()),
                    u8::try_from(civ.day()).unwrap_or(1),
                )
                .map_err(|err| DurationInRangeError::InvalidTimezone(err.to_string()))?;
                let time = Time::from_hms(u8::try_from(civ.hour()).unwrap_or(0), 0, 0)
                    .map_err(|err| DurationInRangeError::InvalidTimezone(err.to_string()))?;
                let offset = UtcOffset::from_whole_seconds(zoned.offset().seconds())
                    .map_err(|err| DurationInRangeError::InvalidTimezone(err.to_string()))?;
                PrimitiveDateTime::new(date, time).assume_offset(offset)
            }
            LocalGranularity::Day => {
                let sod = zoned
                    .start_of_day()
                    .map_err(|err| DurationInRangeError::InvalidTimezone(err.to_string()))?;
                let offset = UtcOffset::from_whole_seconds(sod.offset().seconds())
                    .map_err(|err| DurationInRangeError::InvalidTimezone(err.to_string()))?;
                OffsetDateTime::from_unix_timestamp(sod.timestamp().as_second())
                    .map_err(|err| DurationInRangeError::InvalidTimezone(err.to_string()))?
                    .to_offset(offset)
            }
        };
        *totals.entry(bucket_start).or_insert(0) += duration_seconds;
    }

    totals
        .into_iter()
        .map(|(start, secs)| {
            #[allow(
                clippy::cast_precision_loss,
                reason = "total_seconds for realistic observation windows fits f64 without loss"
            )]
            let total_minutes = secs as f64 / 60.0;
            Ok(BucketMinutes {
                bucket_start: start
                    .format(&Rfc3339)
                    .map_err(|err| DurationInRangeError::InvalidTimezone(err.to_string()))?,
                total_minutes,
            })
        })
        .collect()
}

/// Convert a jiff civil month (1–12, `i8`) into a `time::Month`.
fn month_from(month: i8) -> Month {
    Month::try_from(u8::try_from(month).unwrap_or(1)).unwrap_or(Month::January)
}
```

Note: the several `map_err(... InvalidTimezone ...)` on infallible-in-practice conversions keep the helper total without `unwrap`/`panic`; they can never fire for a valid zone and a whole-second timestamp, but satisfy the no-panic posture. `TimeZone::UTC` is a jiff associated constant.

- [ ] **Step 6: Run the unit tests to verify they pass**

Run: `cargo test -p chartpds-core duration_in_value_range 2>&1 | tail -20`
Expected: PASS (all six new tests + the four pre-existing tests still green).

- [ ] **Step 7: Commit**

```bash
git add crates/chartpds-core/Cargo.toml Cargo.lock crates/chartpds-core/src/queries/duration_in_value_range.rs
git commit -m "Add jiff + pure local-time bucketing helper for duration_in_range

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Route the query through the local-time path

**Files:**
- Modify: `crates/chartpds-core/src/queries/duration_in_value_range.rs` (`duration_in_value_range` body + signature)
- Modify: `crates/chartpds-core/src/queries/mod.rs` (re-export the new error type)
- Modify: `.sqlx/` (regenerated cache)

**Interfaces:**
- Consumes: `bucket_local`, `LocalGranularity`, `DurationInRangeError`, `Bucket::Hour` from Task 1.
- Produces: `pub async fn duration_in_value_range(...) -> Result<DurationInRange, DurationInRangeError>` (return type changed from `Result<_, sqlx::Error>`); `DurationInValueRangeParams` gains `pub timezone: Option<&'a str>`.

- [ ] **Step 1: Write the failing DB-backed tests**

Add to the `#[cfg(test)] mod tests` block. These reuse the existing `seed_interval_observations` / `IntervalObsSpec` helpers and `AASM_SLEEP_STAGE_*` constants already imported in the module. First add a seed builder near `three_hr_minutes`:

```rust
// Two 5-min sleep-wake epochs: 06:30Z and 07:15Z on 2026-06-27.
// In America/New_York (EDT) these are 02:30 and 03:15 -> the 02:00 and 03:00
// local hours; in UTC they are the 06:00 and 07:00 hours.
fn two_wake_epochs() -> [IntervalObsSpec; 2] {
    [
        IntervalObsSpec {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            effective_start: datetime!(2026-06-27 06:30:00 UTC),
            effective_end: datetime!(2026-06-27 06:35:00 UTC),
            value_quantity: 0.0, // AASM Wake
        },
        IntervalObsSpec {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            effective_start: datetime!(2026-06-27 07:15:00 UTC),
            effective_end: datetime!(2026-06-27 07:20:00 UTC),
            value_quantity: 0.0,
        },
    ]
}
```

```rust
#[tokio::test]
async fn hour_bucket_utc_groups_by_utc_hour() {
    let (pool, _) = seed_interval_observations(&two_wake_epochs()).await;
    let result = duration_in_value_range(
        &pool,
        DurationInValueRangeParams {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            start: datetime!(2026-06-27 00:00:00 UTC),
            end: datetime!(2026-06-28 00:00:00 UTC),
            value_min: 0.0,
            value_max: 0.0,
            bucket: Bucket::Hour,
            timezone: None,
        },
    )
    .await
    .expect("query");
    assert_eq!(
        result,
        DurationInRange::Buckets {
            per_bucket: vec![
                BucketMinutes { bucket_start: "2026-06-27T06:00:00Z".into(), total_minutes: 5.0 },
                BucketMinutes { bucket_start: "2026-06-27T07:00:00Z".into(), total_minutes: 5.0 },
            ],
        }
    );
}

#[tokio::test]
async fn hour_bucket_local_groups_by_local_hour() {
    let (pool, _) = seed_interval_observations(&two_wake_epochs()).await;
    let result = duration_in_value_range(
        &pool,
        DurationInValueRangeParams {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            start: datetime!(2026-06-27 00:00:00 UTC),
            end: datetime!(2026-06-28 00:00:00 UTC),
            value_min: 0.0,
            value_max: 0.0,
            bucket: Bucket::Hour,
            timezone: Some("America/New_York"),
        },
    )
    .await
    .expect("query");
    assert_eq!(
        result,
        DurationInRange::Buckets {
            per_bucket: vec![
                BucketMinutes { bucket_start: "2026-06-27T02:00:00-04:00".into(), total_minutes: 5.0 },
                BucketMinutes { bucket_start: "2026-06-27T03:00:00-04:00".into(), total_minutes: 5.0 },
            ],
        }
    );
}

#[tokio::test]
async fn day_bucket_with_timezone_cuts_at_local_midnight() {
    let (pool, _) = seed_interval_observations(&two_wake_epochs()).await;
    // Both epochs are still on the NY-local 2026-06-27 day (02:30, 03:15).
    let result = duration_in_value_range(
        &pool,
        DurationInValueRangeParams {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            start: datetime!(2026-06-27 00:00:00 UTC),
            end: datetime!(2026-06-28 00:00:00 UTC),
            value_min: 0.0,
            value_max: 0.0,
            bucket: Bucket::Day,
            timezone: Some("America/New_York"),
        },
    )
    .await
    .expect("query");
    assert_eq!(
        result,
        DurationInRange::Buckets {
            per_bucket: vec![BucketMinutes {
                bucket_start: "2026-06-27T00:00:00-04:00".into(),
                total_minutes: 10.0
            }],
        }
    );
}
```

Also update the four pre-existing tests: add `timezone: None,` to each `DurationInValueRangeParams { ... }` literal (they omit the new field otherwise and will not compile).

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p chartpds-core duration_in_value_range 2>&1 | tail -20`
Expected: compile error — `timezone` field missing / return-type mismatch.

- [ ] **Step 3: Change the signature and add the local-path branch**

In `DurationInValueRangeParams`, add the field (with a doc comment):

```rust
    /// IANA timezone for `day`/`hour` bucket boundaries; `None` = UTC.
    pub timezone: Option<&'a str>,
```

Change the function return type to `Result<DurationInRange, DurationInRangeError>` and destructure `timezone` from params. Restructure the `match bucket` so the legacy SQL path runs only for the back-compat cases, and everything else uses the local path:

```rust
    match (bucket, timezone) {
        // Back-compat SQL fast paths (no rows shipped to Rust).
        (Bucket::None, _) => {
            // ... existing None query, unchanged, but the final Ok(...) stays
            // Result<_, DurationInRangeError> via the `?`/From on sqlx::Error ...
        }
        (Bucket::Day, None) => {
            // ... existing Day query, unchanged ...
        }
        // Local-time paths: filter+duration in SQL, bucket in Rust.
        (Bucket::Day, Some(_)) => {
            let rows = fetch_interval_rows(pool, coding_system, coding_code, start, end, value_min, value_max).await?;
            Ok(DurationInRange::Buckets {
                per_bucket: bucket_local(&rows, LocalGranularity::Day, timezone)?,
            })
        }
        (Bucket::Hour, _) => {
            let rows = fetch_interval_rows(pool, coding_system, coding_code, start, end, value_min, value_max).await?;
            Ok(DurationInRange::Buckets {
                per_bucket: bucket_local(&rows, LocalGranularity::Hour, timezone)?,
            })
        }
    }
```

The two existing SQL arms keep their bodies verbatim; only the outer `match` shape and the wrapping change. Their `.fetch_one/.fetch_all(pool).await?` now yields `DurationInRangeError` through `#[from]`.

Add the shared row-fetch helper (new `sqlx::query!`):

```rust
/// Fetch `(effective_start, duration_seconds)` for every in-range interval.
async fn fetch_interval_rows(
    pool: &SqlitePool,
    coding_system: &str,
    coding_code: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
    value_min: f64,
    value_max: f64,
) -> Result<Vec<(OffsetDateTime, i64)>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT effective_start AS "effective_start!: OffsetDateTime",
               (CAST(strftime('%s', effective_end) AS INTEGER)
                - CAST(strftime('%s', effective_start) AS INTEGER)) AS "duration_seconds!: i64"
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
    Ok(rows
        .into_iter()
        .map(|r| (r.effective_start, r.duration_seconds))
        .collect())
}
```

- [ ] **Step 4: Regenerate the sqlx cache**

Run: `just prepare-sql`
Expected: writes/updates a `.sqlx/query-*.json` for the new query. If it complains sqlx-cli is missing, run `just install-tools` first.

- [ ] **Step 5: Re-export the error type**

In `crates/chartpds-core/src/queries/mod.rs`, add `DurationInRangeError` to the `duration_in_value_range` re-export list:

```rust
pub use duration_in_value_range::{
    duration_in_value_range, Bucket, BucketMinutes, DurationInRange, DurationInRangeError,
    DurationInValueRangeParams,
};
```

- [ ] **Step 6: Run tests to verify pass**

Run: `cargo test -p chartpds-core duration_in_value_range 2>&1 | tail -20`
Expected: PASS (new + updated pre-existing tests).

- [ ] **Step 7: Commit**

```bash
git add crates/chartpds-core/src/queries/duration_in_value_range.rs crates/chartpds-core/src/queries/mod.rs .sqlx
git commit -m "Route duration_in_range through local-time bucketing for hour/timezone

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Wire the MCP tool (`hour` + `timezone`)

**Files:**
- Modify: `crates/chartpds-mcp/src/server.rs` (`ObservationDurationInRangeArgs`, `observation_duration_in_range`, tool description, one server test)

**Interfaces:**
- Consumes: `chartpds_core::queries::{Bucket, DurationInRangeError, DurationInValueRangeParams, duration_in_value_range}`.

- [ ] **Step 1: Write the failing server test**

Add to the `#[cfg(test)] mod tests` in `server.rs`, next to `observation_duration_in_range_totals_in_zone_minutes`. It reuses `fresh_server_with_sleep_epochs` (already defined below in the test module — its two contiguous asleep epochs are at 22:00–22:10 UTC on 2026-01-01, which in America/New_York EST is 17:00–17:10). This test asserts an asleep-range query buckets by the local hour:

```rust
#[tokio::test]
async fn observation_duration_in_range_hour_bucket_uses_local_timezone() {
    let server = fresh_server_with_sleep_epochs().await;
    let result = server
        .observation_duration_in_range(Parameters(ObservationDurationInRangeArgs {
            coding: Coding {
                system: "https://chartpds.fhwang.net/coding/aasm/sleep-stage".to_string(),
                code: "aasm-sleep-stage".to_string(),
            },
            start: "2026-01-01T00:00:00Z".to_string(),
            end: "2026-01-02T00:00:00Z".to_string(),
            value_min: 1.0,
            value_max: 4.0, // asleep epochs
            bucket: Some("hour".to_string()),
            timezone: Some("America/New_York".to_string()),
        }))
        .await
        .expect("tool call succeeds");

    let text = match &result.content[0].raw {
        rmcp::model::RawContent::Text(t) => &t.text,
        _ => panic!("expected text content"),
    };
    let value: serde_json::Value = serde_json::from_str(text).expect("valid JSON");
    // 22:00-22:10 UTC = 17:00-17:10 EST -> the 17:00 local hour, 10 minutes.
    assert_eq!(value["per_bucket"][0]["bucket_start"], "2026-01-01T17:00:00-05:00");
    assert_eq!(value["per_bucket"][0]["total_minutes"], 10.0);
}
```

Also update the existing `observation_duration_in_range_totals_in_zone_minutes` test: its `ObservationDurationInRangeArgs { ... }` literal needs the new `timezone: None,` field to compile.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p chartpds-mcp observation_duration_in_range 2>&1 | tail -20`
Expected: compile error — `timezone` field missing.

- [ ] **Step 3: Add the arg, the `hour` variant, and error mapping**

In `ObservationDurationInRangeArgs`, add:

```rust
    /// IANA timezone (e.g. `"America/New_York"`) for `day`/`hour` bucket
    /// boundaries. Omit for UTC (default). No-op for `bucket:"none"`.
    pub(crate) timezone: Option<String>,
```

In `observation_duration_in_range`, extend the bucket parser and pass `timezone`, and map the new error type:

```rust
        let bucket = match args.bucket.as_deref() {
            None | Some("none") => chartpds_core::queries::Bucket::None,
            Some("day") => chartpds_core::queries::Bucket::Day,
            Some("hour") => chartpds_core::queries::Bucket::Hour,
            Some(other) => {
                return Err(McpError::invalid_params(
                    format!("invalid bucket {other:?}; expected \"none\", \"day\", or \"hour\""),
                    None,
                ))
            }
        };

        let result = chartpds_core::queries::duration_in_value_range(
            &self.pool,
            chartpds_core::queries::DurationInValueRangeParams {
                coding_system: &args.coding.system,
                coding_code: &args.coding.code,
                start,
                end,
                value_min: args.value_min,
                value_max: args.value_max,
                bucket,
                timezone: args.timezone.as_deref(),
            },
        )
        .await
        .map_err(|err| match err {
            chartpds_core::queries::DurationInRangeError::InvalidTimezone(_) => {
                McpError::invalid_params(err.to_string(), None)
            }
            chartpds_core::queries::DurationInRangeError::Db(_) => {
                McpError::internal_error(format!("query failed: {err}"), None)
            }
        })?;
```

- [ ] **Step 4: Update the tool description**

Replace the `#[tool(description = "...")]` string on `observation_duration_in_range` with:

```
Total minutes a coded periodic signal spent inside a value range over a window. Args: coding {system, code}, start/end (RFC 3339, half-open), value_min/value_max (inclusive), bucket, timezone. bucket \"none\" (default) returns {total_minutes}; \"day\" and \"hour\" return {per_bucket:[{bucket_start, total_minutes}]}. Empty buckets are omitted. Optional timezone is an IANA name (e.g. \"America/New_York\", DST-aware) setting day/hour boundaries; omit for UTC. bucket_start format: \"YYYY-MM-DD\" for day+UTC (back-compat); otherwise RFC 3339 with the local offset (e.g. \"2026-06-27T02:00:00-04:00\", or \"...Z\" for hour+UTC). timezone is a no-op for bucket \"none\".
```

- [ ] **Step 5: Run tests to verify pass**

Run: `cargo test -p chartpds-mcp observation_duration_in_range 2>&1 | tail -20`
Expected: PASS (new + updated test).

- [ ] **Step 6: Commit**

```bash
git add crates/chartpds-mcp/src/server.rs
git commit -m "Expose hour bucket + timezone on observation_duration_in_range MCP tool

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Holdout regression test + Oura fixture (PROTECTED — leave staged)

**Files:**
- Create: `holdout/fixtures/oura_sleep_night/<sha256>` (raw Oura sleep-session JSON blob)
- Create: `holdout/fixtures/oura_sleep_night/<sha256>.meta.json` (CloudEvents sidecar)
- Create: `holdout/tests/intraday_hour_bucketing.rs`

**⚠️ These are protected paths.** You may create and run them, but must NOT commit them and must NOT run `just holdout-bless`. After confirming the test passes, leave everything staged and hand off to the human.

- [ ] **Step 1: Author the Oura sleep-session blob**

The blob must deserialize directly into `OuraSleepSession` (that is what `sources::oura::storage::replay` reads). Craft one night that crosses UTC midnight so local-hour bucketing is visibly different from UTC. `bedtime_start` = `2026-06-26T22:00:00-04:00` (= `2026-06-27T02:00:00Z`). 48 five-minute epochs; wake char `'4'` at index 12 (23:00–23:05 NY) and index 36 (01:00–01:05 NY); light-sleep `'2'` elsewhere.

Write the JSON to a scratch file first (exact bytes matter — do not reformat after hashing):

```bash
SCRATCH="$(mktemp -d)"
cat > "$SCRATCH/session.json" <<'JSON'
{"id":"holdout-night-1","day":"2026-06-27","bedtime_start":"2026-06-26T22:00:00-04:00","bedtime_end":"2026-06-27T02:00:00-04:00","type":"long_sleep","sleep_phase_5_min":"222222222222422222222222222222222222422222222222"}
JSON
# sanity: 48 chars, '4' at positions 13 and 37 (1-indexed)
python3 -c "s=open('$SCRATCH/session.json').read(); import json; d=json.loads(s); p=d['sleep_phase_5_min']; print(len(p), [i for i,c in enumerate(p) if c=='4'])"
```
Expected: `48 [12, 36]`

- [ ] **Step 2: Hash, place the blob, and write the sidecar**

```bash
HASH="$(shasum -a 256 "$SCRATCH/session.json" | cut -d' ' -f1)"
DEST="holdout/fixtures/oura_sleep_night"
mkdir -p "$DEST"
cp "$SCRATCH/session.json" "$DEST/$HASH"
cat > "$DEST/$HASH.meta.json" <<JSON
{"specversion":"1.0","id":"$HASH","source":"oura","type":"oura-sleep-session","datacontenttype":"application/json","subject":"2026-06-27","time":"2026-06-27T12:00:00Z","originalfilename":"oura-sleep-2026-06-27-holdout-night-1.json"}
JSON
echo "blob=$HASH"
```

The manifest `type` = `oura-sleep-session` is what routes `rebuild_index` to the Oura replay path; `id` must equal the blob hash (mirrors the committed `fitbit_hr_dup` fixture).

- [ ] **Step 3: Write the holdout test**

Create `holdout/tests/intraday_hour_bucketing.rs`:

```rust
//! Holdout regression test for intra-day (hour) + timezone bucketing.
//!
//! PROTECTED: part of the holdout suite. A failure here is a real regression in
//! the product contract — fix `crates/**`, never edit this file or its fixtures
//! to make it pass. Changes under `holdout/` require a human-signed bless commit.
//!
//! Locks the ChartPDS issue #4 capability: `observation_duration_in_range` with
//! `bucket:"hour"` + an IANA `timezone` must bucket wake epochs by LOCAL clock
//! hour (carrying the zone offset), not by UTC hour. The fixture is one Oura
//! night crossing UTC midnight, with wake epochs at NY-local 23:00 (2026-06-26)
//! and 01:00 (2026-06-27) — which are 03:00Z and 05:00Z on 2026-06-27.

use chartpds_holdout::Harness;

const AASM_SYSTEM: &str = "https://chartpds.fhwang.net/coding/aasm/sleep-stage";
const AASM_CODE: &str = "aasm-sleep-stage";

fn bucket_starts(per_bucket: &serde_json::Value) -> Vec<String> {
    per_bucket
        .as_array()
        .expect("per_bucket array")
        .iter()
        .map(|b| b["bucket_start"].as_str().expect("bucket_start str").to_string())
        .collect()
}

#[tokio::test]
async fn hour_bucketing_uses_local_wall_clock_with_timezone() {
    let server = Harness::start().await;
    server.seed_archive_from_fixtures("oura_sleep_night");

    let rebuild = server
        .call_tool("rebuild_index", serde_json::Value::Null)
        .await;
    assert_eq!(rebuild["oura_ingested"], 1, "the Oura night must replay: {rebuild}");

    // Local (America/New_York): wake epochs fall in the 23:00 (06-26) and
    // 01:00 (06-27) LOCAL hours, each carrying the -04:00 EDT offset.
    let local = server
        .call_tool(
            "observation_duration_in_range",
            serde_json::json!({
                "coding": { "system": AASM_SYSTEM, "code": AASM_CODE },
                "start": "2026-06-26T00:00:00-04:00",
                "end":   "2026-06-28T00:00:00-04:00",
                "value_min": 0.0, "value_max": 0.0,
                "bucket": "hour",
                "timezone": "America/New_York"
            }),
        )
        .await;
    let local_starts = bucket_starts(&local["per_bucket"]);
    assert_eq!(
        local_starts,
        vec![
            "2026-06-26T23:00:00-04:00".to_string(),
            "2026-06-27T01:00:00-04:00".to_string(),
        ],
        "hour+timezone must bucket by local wall-clock hour: {local}"
    );
    for b in local["per_bucket"].as_array().unwrap() {
        assert_eq!(b["total_minutes"], 5.0, "each wake epoch is 5 minutes: {local}");
    }

    // Contrast: without timezone the same epochs bucket by UTC hour (03:00Z,
    // 05:00Z) — proving the timezone parameter actually changes the result.
    let utc = server
        .call_tool(
            "observation_duration_in_range",
            serde_json::json!({
                "coding": { "system": AASM_SYSTEM, "code": AASM_CODE },
                "start": "2026-06-26T00:00:00Z",
                "end":   "2026-06-28T00:00:00Z",
                "value_min": 0.0, "value_max": 0.0,
                "bucket": "hour"
            }),
        )
        .await;
    assert_eq!(
        bucket_starts(&utc["per_bucket"]),
        vec![
            "2026-06-27T03:00:00Z".to_string(),
            "2026-06-27T05:00:00Z".to_string(),
        ],
        "hour without timezone must bucket by UTC hour: {utc}"
    );
}
```

- [ ] **Step 4: Run the holdout test to confirm it reproduces the behavior**

Run: `cargo test -p chartpds-holdout intraday_hour_bucketing 2>&1 | tail -25`
Expected: PASS (the binary is built by the workspace test run). If it fails to find the binary, run `cargo build -p chartpds-mcp` first.

- [ ] **Step 5: Stage but DO NOT commit; hand off**

```bash
git add holdout/tests/intraday_hour_bucketing.rs holdout/fixtures/oura_sleep_night
git status --short holdout/
```

Then STOP and tell the human: the holdout test and fixture are **staged and ready to bless**; they must run `just holdout-bless "intraday hour bucketing + timezone"` (Touch ID) to admit them via a signed commit. Do not commit these files yourself.

---

## Task 5: Full gate

**Files:** none (verification only).

- [ ] **Step 1: Run the full check gate**

Run: `just check 2>&1 | tail -30`
Expected: all stages green — fmt-check, lint (`-D warnings`), typecheck, test (incl. holdout), `cargo deny`, `cargo machete`, `cargo sqlx prepare --check`, `holdout-verify`.

Note: `holdout-verify` compares against `holdout.lock`. Because the new holdout test/fixture are staged-but-unblessed, `holdout-verify` may report the pending protected-path change — that is expected and is resolved by the human's `just holdout-bless`, NOT by editing the lock. If any OTHER stage fails, fix the code in `crates/**` and re-run. Do not weaken tests.

- [ ] **Step 2: Report**

Summarize: what shipped, that Tasks 1–3 are committed, and that the Task 4 holdout test is staged awaiting a human bless.

---

## Self-Review

- **Spec coverage:** `"hour"` bucket → Tasks 1–3. `timezone` param (day+hour, default UTC) → Tasks 1–3. Local-time boundary math + DST → Task 1 helper (+ fall-back test). `bucket_start` format table → Task 1/2 tests + Task 3 description. Back-compat (none, day-UTC unchanged) → Task 2 keeps the SQL arms + updates existing tests. jiff dependency + gates → Task 1 + Task 5. Holdout test → Task 4. `no-op timezone for bucket:"none"` → Task 3 (parser passes `timezone` but the `(Bucket::None, _)` arm ignores it). Out-of-scope items (no histogram fold, no WASO, no longest-period change) → not implemented, by omission.
- **Placeholder scan:** none — every code step shows full code; commands have expected output.
- **Type consistency:** `bucket_local` / `LocalGranularity` / `DurationInRangeError` / `fetch_interval_rows` names match across Tasks 1–3; `timezone: Option<&'a str>` (core) vs `Option<String>` (MCP, passed via `.as_deref()`) is consistent; `Bucket::Hour` used uniformly.
