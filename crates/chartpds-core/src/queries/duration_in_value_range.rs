//! Total time a coded periodic signal spent inside a value range.
//!
//! Sums the durations of interval observations whose `value_quantity` falls
//! within `[value_min, value_max]`, matched by `{coding_system, coding_code}`
//! and a half-open `[start, end)` window on `effective_start`. The `none`
//! bucket and the `day` bucket without a `timezone` sum entirely in `SQLite`,
//! so high-volume signals (e.g. heart rate) never ship rows to Rust. The
//! local-time paths — `hour`, or `day` with a `timezone` — still filter and
//! compute per-row durations in SQL, but ship the matching in-range interval
//! rows to Rust for local-time bucketing; `bucket:"hour"` on a high-cardinality
//! signal can therefore be a large transfer.

use std::collections::BTreeMap;

use jiff::{tz::TimeZone, Timestamp};
use sqlx::SqlitePool;
use time::format_description::well_known::Rfc3339;
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

use crate::queries::roll_up_bucket_confidence;
use crate::sources::DayConfidence;

/// How to group aggregated durations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bucket {
    /// One aggregate over the whole window.
    None,
    /// One aggregate per UTC calendar day of `effective_start`.
    Day,
    /// One aggregate per clock hour (local to `timezone`, else UTC).
    Hour,
}

/// Failure modes of [`duration_in_value_range`].
#[derive(Debug, thiserror::Error)]
pub enum DurationInRangeError {
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
            .map_err(|err| DurationInRangeError::Internal(err.to_string()))?;
        let zoned = ts.to_zoned(tz.clone());
        let bucket_start = match granularity {
            LocalGranularity::Hour => {
                let civ = zoned.datetime();
                let day = u8::try_from(civ.day())
                    .map_err(|err| DurationInRangeError::Internal(err.to_string()))?;
                let date =
                    Date::from_calendar_date(i32::from(civ.year()), month_from(civ.month())?, day)
                        .map_err(|err| DurationInRangeError::Internal(err.to_string()))?;
                let hour = u8::try_from(civ.hour())
                    .map_err(|err| DurationInRangeError::Internal(err.to_string()))?;
                let time = Time::from_hms(hour, 0, 0)
                    .map_err(|err| DurationInRangeError::Internal(err.to_string()))?;
                let offset = UtcOffset::from_whole_seconds(zoned.offset().seconds())
                    .map_err(|err| DurationInRangeError::Internal(err.to_string()))?;
                PrimitiveDateTime::new(date, time).assume_offset(offset)
            }
            LocalGranularity::Day => {
                let sod = zoned
                    .start_of_day()
                    .map_err(|err| DurationInRangeError::Internal(err.to_string()))?;
                let offset = UtcOffset::from_whole_seconds(sod.offset().seconds())
                    .map_err(|err| DurationInRangeError::Internal(err.to_string()))?;
                OffsetDateTime::from_unix_timestamp(sod.timestamp().as_second())
                    .map_err(|err| DurationInRangeError::Internal(err.to_string()))?
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
                    .map_err(|err| DurationInRangeError::Internal(err.to_string()))?,
                total_minutes,
                confidence: DayConfidence::Confirmed,
            })
        })
        .collect()
}

/// Convert a jiff civil month (1-12, `i8`) into a `time::Month`.
fn month_from(month: i8) -> Result<Month, DurationInRangeError> {
    let month_num =
        u8::try_from(month).map_err(|err| DurationInRangeError::Internal(err.to_string()))?;
    Month::try_from(month_num).map_err(|err| DurationInRangeError::Internal(err.to_string()))
}

/// Total minutes for one UTC calendar day.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct BucketMinutes {
    /// UTC calendar day (`YYYY-MM-DD`) the bucket covers.
    pub bucket_start: String,
    /// Total minutes inside the value range for the bucket.
    pub total_minutes: f64,
    /// Confidence of the bucket: `Provisional` if any contributing source-day
    /// is provisional, else `Confirmed`. For local (timezone/hour) buckets this
    /// is keyed by the bucket start instant's UTC calendar day — a conservative
    /// approximation for buckets that straddle UTC midnight.
    pub confidence: DayConfidence,
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

/// A time-in-range question: how long a coded signal spent inside a value range.
pub struct TimeInRangeQuery<'a> {
    /// FHIR coding system URI.
    pub coding_system: &'a str,
    /// Coding code within `coding_system`.
    pub coding_code: &'a str,
    /// Start of the half-open window (inclusive).
    pub start: OffsetDateTime,
    /// End of the half-open window (exclusive).
    pub end: OffsetDateTime,
    /// Minimum `value_quantity` (inclusive).
    pub value_min: f64,
    /// Maximum `value_quantity` (inclusive).
    pub value_max: f64,
    /// How to group the result.
    pub bucket: Bucket,
    /// IANA timezone for `day`/`hour` bucket boundaries; `None` = UTC.
    pub timezone: Option<&'a str>,
}

/// Shared filter for the interval queries: coding + half-open window + value range.
struct RangeQuery<'a> {
    coding_system: &'a str,
    coding_code: &'a str,
    start: OffsetDateTime,
    end: OffsetDateTime,
    value_min: f64,
    value_max: f64,
}

/// Sum the minutes a coded signal spent inside `[value_min, value_max]`.
///
/// Matches observations by `coding_system`/`coding_code`, `effective_start`
/// in the half-open window `[start, end)`, `value_quantity` within the
/// inclusive range, and a non-null `effective_end` (rows without an end have
/// no measurable duration and are ignored). Durations come from
/// `effective_end - effective_start`.
///
/// `bucket:"none"` and `bucket:"day"` with no `timezone` run entirely in SQL.
/// `bucket:"hour"` (any timezone) or `bucket:"day"` with a `timezone` fetch
/// the matching rows and bucket them in Rust by local clock time.
///
/// # Errors
///
/// Returns [`DurationInRangeError::Db`] if the query fails,
/// [`DurationInRangeError::InvalidTimezone`] if `timezone` is not a known
/// IANA zone name, or [`DurationInRangeError::Internal`] if an internal
/// date/time conversion fails unexpectedly.
pub async fn duration_in_value_range(
    pool: &SqlitePool,
    now: OffsetDateTime,
    query: TimeInRangeQuery<'_>,
) -> Result<DurationInRange, DurationInRangeError> {
    let TimeInRangeQuery {
        coding_system,
        coding_code,
        start,
        end,
        value_min,
        value_max,
        bucket,
        timezone,
    } = query;
    let query = RangeQuery {
        coding_system,
        coding_code,
        start,
        end,
        value_min,
        value_max,
    };
    match (bucket, timezone) {
        // Back-compat SQL fast paths (no rows shipped to Rust).
        (Bucket::None, _) => total_in_range(pool, &query).await,
        (Bucket::Day, None) => day_bucket_sql(pool, now, &query).await,
        // Local-time paths: filter+duration in SQL, bucket in Rust.
        (Bucket::Day, Some(_)) => {
            local_bucketed(pool, now, &query, LocalGranularity::Day, timezone).await
        }
        (Bucket::Hour, _) => {
            local_bucketed(pool, now, &query, LocalGranularity::Hour, timezone).await
        }
    }
}

/// SQL fast path for [`Bucket::None`]: a single total, summed in `SQLite`.
async fn total_in_range(
    pool: &SqlitePool,
    query: &RangeQuery<'_>,
) -> Result<DurationInRange, DurationInRangeError> {
    let row = sqlx::query!(
        r#"
        SELECT COALESCE(
                   SUM(
                       CAST(strftime('%s', effective_end) AS INTEGER)
                       - CAST(strftime('%s', effective_start) AS INTEGER)
                   ),
                   0
               ) AS "total_seconds!: i64"
        FROM observations
        WHERE coding_system = ?
          AND coding_code = ?
          AND effective_start >= ?
          AND effective_start < ?
          AND effective_end IS NOT NULL
          AND value_quantity >= ?
          AND value_quantity <= ?
        "#,
        query.coding_system,
        query.coding_code,
        query.start,
        query.end,
        query.value_min,
        query.value_max,
    )
    .fetch_one(pool)
    .await?;

    #[allow(
        clippy::cast_precision_loss,
        reason = "total_seconds for realistic observation windows fits f64 without loss"
    )]
    let total_minutes = row.total_seconds as f64 / 60.0;
    Ok(DurationInRange::Total { total_minutes })
}

/// SQL fast path for [`Bucket::Day`] with no `timezone`: grouped by UTC
/// calendar day directly in `SQLite`.
async fn day_bucket_sql(
    pool: &SqlitePool,
    now: OffsetDateTime,
    query: &RangeQuery<'_>,
) -> Result<DurationInRange, DurationInRangeError> {
    let rows = sqlx::query!(
        r#"
        SELECT date(effective_start) AS "day!: String",
               SUM(
                   CAST(strftime('%s', effective_end) AS INTEGER)
                   - CAST(strftime('%s', effective_start) AS INTEGER)
               ) AS "total_seconds!: i64"
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
        query.coding_system,
        query.coding_code,
        query.start,
        query.end,
        query.value_min,
        query.value_max,
    )
    .fetch_all(pool)
    .await?;

    let contributions = crate::queries::day_confidence::contributions_for_filter(
        pool,
        query.coding_system,
        query.coding_code,
        query.start,
        query.end,
        query.value_min,
        query.value_max,
    )
    .await?;
    let confidence_by_bucket = roll_up_bucket_confidence(pool, now, &contributions).await?;

    Ok(DurationInRange::Buckets {
        per_bucket: rows
            .into_iter()
            .map(|r| {
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "total_seconds for realistic observation windows fits f64 without loss"
                )]
                let total_minutes = r.total_seconds as f64 / 60.0;
                BucketMinutes {
                    confidence: confidence_by_bucket
                        .get(&r.day)
                        .copied()
                        .unwrap_or(DayConfidence::Confirmed),
                    bucket_start: r.day,
                    total_minutes,
                }
            })
            .collect(),
    })
}

/// Local-time path: fetch matching interval rows in SQL, then bucket them in
/// Rust by local hour or day (see [`bucket_local`]).
async fn local_bucketed(
    pool: &SqlitePool,
    now: OffsetDateTime,
    query: &RangeQuery<'_>,
    granularity: LocalGranularity,
    timezone: Option<&str>,
) -> Result<DurationInRange, DurationInRangeError> {
    let rows = fetch_interval_rows(pool, query).await?;
    let mut per_bucket = bucket_local(&rows, granularity, timezone)?;

    let contributions = crate::queries::day_confidence::contributions_for_filter(
        pool,
        query.coding_system,
        query.coding_code,
        query.start,
        query.end,
        query.value_min,
        query.value_max,
    )
    .await?;
    let confidence_by_day = roll_up_bucket_confidence(pool, now, &contributions).await?;
    for b in &mut per_bucket {
        let utc_day = utc_day_of_rfc3339(&b.bucket_start)?;
        b.confidence = confidence_by_day
            .get(&utc_day)
            .copied()
            .unwrap_or(DayConfidence::Confirmed);
    }

    Ok(DurationInRange::Buckets { per_bucket })
}

/// UTC calendar day (`YYYY-MM-DD`) of an RFC 3339 timestamp string.
fn utc_day_of_rfc3339(s: &str) -> Result<String, DurationInRangeError> {
    let dt = OffsetDateTime::parse(s, &Rfc3339)
        .map_err(|err| DurationInRangeError::Internal(err.to_string()))?
        .to_offset(UtcOffset::UTC);
    Ok(format!(
        "{:04}-{:02}-{:02}",
        dt.year(),
        u8::from(dt.month()),
        dt.day()
    ))
}

/// Fetch `(effective_start, duration_seconds)` for every in-range interval.
async fn fetch_interval_rows(
    pool: &SqlitePool,
    query: &RangeQuery<'_>,
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
        query.coding_system,
        query.coding_code,
        query.start,
        query.end,
        query.value_min,
        query.value_max,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| (r.effective_start, r.duration_seconds))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clinical::{AASM_SLEEP_STAGE_CODE, AASM_SLEEP_STAGE_SYSTEM, SYSTEM_LOINC};
    use crate::queries::test_support::{seed_interval_observations, IntervalObsSpec};
    use crate::sources::DayConfidence;
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

    #[tokio::test]
    async fn total_sums_only_in_range_intervals() {
        let (pool, _) = seed_interval_observations(&three_hr_minutes()).await;
        // Range 101..118 includes only the 110 bpm minute => 1.0 minute.
        let result = duration_in_value_range(
            &pool,
            datetime!(2026-07-01 00:00:00 UTC),
            TimeInRangeQuery {
                coding_system: SYSTEM_LOINC,
                coding_code: "8867-4",
                start: datetime!(2026-01-01 00:00:00 UTC),
                end: datetime!(2026-01-03 00:00:00 UTC),
                value_min: 101.0,
                value_max: 118.0,
                bucket: Bucket::None,
                timezone: None,
            },
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
            datetime!(2026-07-01 00:00:00 UTC),
            TimeInRangeQuery {
                coding_system: SYSTEM_LOINC,
                coding_code: "8867-4",
                start: datetime!(2026-01-01 00:00:00 UTC),
                end: datetime!(2026-01-03 00:00:00 UTC),
                value_min: 90.0,
                value_max: 140.0,
                bucket: Bucket::Day,
                timezone: None,
            },
        )
        .await
        .expect("query");
        assert_eq!(
            result,
            DurationInRange::Buckets {
                per_bucket: vec![
                    BucketMinutes {
                        bucket_start: "2026-01-01".to_string(),
                        total_minutes: 2.0,
                        confidence: DayConfidence::Confirmed,
                    },
                    BucketMinutes {
                        bucket_start: "2026-01-02".to_string(),
                        total_minutes: 1.0,
                        confidence: DayConfidence::Confirmed,
                    },
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
            datetime!(2026-07-01 00:00:00 UTC),
            TimeInRangeQuery {
                coding_system: SYSTEM_LOINC,
                coding_code: "8867-4",
                start: datetime!(2026-01-01 00:00:00 UTC),
                end: datetime!(2026-01-02 00:00:00 UTC),
                value_min: 1.0,
                value_max: 4.0,
                bucket: Bucket::None,
                timezone: None,
            },
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
            datetime!(2026-07-01 00:00:00 UTC),
            TimeInRangeQuery {
                coding_system: SYSTEM_LOINC,
                coding_code: "8867-4",
                start: datetime!(2026-01-01 00:00:00 UTC),
                end: datetime!(2026-01-02 00:00:00 UTC),
                value_min: 90.0,
                value_max: 140.0,
                bucket: Bucket::None,
                timezone: None,
            },
        )
        .await
        .expect("query");
        assert_eq!(result, DurationInRange::Total { total_minutes: 2.0 });
    }

    #[tokio::test]
    async fn hour_bucket_utc_groups_by_utc_hour() {
        let (pool, _) = seed_interval_observations(&two_wake_epochs()).await;
        let result = duration_in_value_range(
            &pool,
            datetime!(2026-07-01 00:00:00 UTC),
            TimeInRangeQuery {
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
                    BucketMinutes {
                        bucket_start: "2026-06-27T06:00:00Z".into(),
                        total_minutes: 5.0,
                        confidence: DayConfidence::Confirmed,
                    },
                    BucketMinutes {
                        bucket_start: "2026-06-27T07:00:00Z".into(),
                        total_minutes: 5.0,
                        confidence: DayConfidence::Confirmed,
                    },
                ],
            }
        );
    }

    #[tokio::test]
    async fn hour_bucket_local_groups_by_local_hour() {
        let (pool, _) = seed_interval_observations(&two_wake_epochs()).await;
        let result = duration_in_value_range(
            &pool,
            datetime!(2026-07-01 00:00:00 UTC),
            TimeInRangeQuery {
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
                    BucketMinutes {
                        bucket_start: "2026-06-27T02:00:00-04:00".into(),
                        total_minutes: 5.0,
                        confidence: DayConfidence::Confirmed,
                    },
                    BucketMinutes {
                        bucket_start: "2026-06-27T03:00:00-04:00".into(),
                        total_minutes: 5.0,
                        confidence: DayConfidence::Confirmed,
                    },
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
            datetime!(2026-07-01 00:00:00 UTC),
            TimeInRangeQuery {
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
                    total_minutes: 10.0,
                    confidence: DayConfidence::Confirmed,
                }],
            }
        );
    }

    #[test]
    fn hour_utc_buckets_align_to_utc_top_of_hour() {
        // One 5-min interval at 06:30Z contributes to the 06:00Z hour.
        let rows = [(datetime!(2026-06-27 06:30:00 UTC), 300i64)];
        let out = bucket_local(&rows, LocalGranularity::Hour, None).expect("bucket");
        assert_eq!(
            out,
            vec![BucketMinutes {
                bucket_start: "2026-06-27T06:00:00Z".into(),
                total_minutes: 5.0,
                confidence: DayConfidence::Confirmed,
            }]
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
                total_minutes: 5.0,
                confidence: DayConfidence::Confirmed,
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
                BucketMinutes {
                    bucket_start: "2026-06-27T02:00:00-04:00".into(),
                    total_minutes: 10.0,
                    confidence: DayConfidence::Confirmed,
                },
                BucketMinutes {
                    bucket_start: "2026-06-27T03:00:00-04:00".into(),
                    total_minutes: 5.0,
                    confidence: DayConfidence::Confirmed,
                },
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
                total_minutes: 5.0,
                confidence: DayConfidence::Confirmed,
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
                BucketMinutes {
                    bucket_start: "2026-11-01T01:00:00-04:00".into(),
                    total_minutes: 5.0,
                    confidence: DayConfidence::Confirmed,
                },
                BucketMinutes {
                    bucket_start: "2026-11-01T01:00:00-05:00".into(),
                    total_minutes: 5.0,
                    confidence: DayConfidence::Confirmed,
                },
            ]
        );
    }

    #[test]
    fn invalid_timezone_is_an_error() {
        let rows = [(datetime!(2026-06-27 06:30:00 UTC), 300i64)];
        let err = bucket_local(&rows, LocalGranularity::Hour, Some("Not/AZone")).unwrap_err();
        assert!(matches!(err, DurationInRangeError::InvalidTimezone(_)));
    }

    #[test]
    fn hour_local_fractional_offset_zone_regroups() {
        // Asia/Kolkata is +05:30. 06:15Z -> 11:45 IST (the 11:00 local hour);
        // 06:45Z -> 12:15 IST (the 12:00 local hour). In UTC both are the 06:00
        // hour, so the fractional offset actually moves them into different buckets.
        let rows = [
            (datetime!(2026-06-27 06:15:00 UTC), 300i64),
            (datetime!(2026-06-27 06:45:00 UTC), 300i64),
        ];
        let out =
            bucket_local(&rows, LocalGranularity::Hour, Some("Asia/Kolkata")).expect("bucket");
        assert_eq!(
            out,
            vec![
                BucketMinutes {
                    bucket_start: "2026-06-27T11:00:00+05:30".into(),
                    total_minutes: 5.0,
                    confidence: DayConfidence::Confirmed,
                },
                BucketMinutes {
                    bucket_start: "2026-06-27T12:00:00+05:30".into(),
                    total_minutes: 5.0,
                    confidence: DayConfidence::Confirmed,
                },
            ]
        );
    }
}
