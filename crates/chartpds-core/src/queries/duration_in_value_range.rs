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
                total_minutes: row.total_seconds as f64 / 60.0,
            })
        }
        Bucket::Day => {
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
                        total_minutes: r.total_seconds as f64 / 60.0,
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
