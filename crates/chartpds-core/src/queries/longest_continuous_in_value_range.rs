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

use crate::queries::episodes::{
    detect_episodes, episode_index_for, fetch_all_intervals, utc_instant_key,
};
use crate::queries::roll_up_bucket_confidence;
use crate::sources::DayConfidence;

/// Longest continuous run, in minutes, for one UTC calendar day.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct BucketLongest {
    /// UTC calendar day (`YYYY-MM-DD`) the run started on.
    pub bucket_start: String,
    /// Length of the longest run that started that day, in minutes.
    pub longest_minutes: f64,
    /// Confidence of the bucket: `Provisional` if any contributing source-day
    /// (keyed by observation UTC day) is provisional, else `Confirmed`.
    ///
    /// Because a run is attributed to its start day while confidence is
    /// keyed by each observation's own UTC day, a midnight-crossing run
    /// spanning a confirmed pre-midnight document and a provisional
    /// post-midnight document can leave this bucket reading `Confirmed`
    /// despite containing provisional data — a narrow under-flag.
    pub confidence: DayConfidence,
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
                    out.push(Run {
                        start: s,
                        minutes: (e - s).as_seconds_f64() / 60.0,
                    });
                }
                cur_start = Some(start);
                cur_end = Some(end);
            }
        }
    }
    if let (Some(s), Some(e)) = (cur_start, cur_end) {
        out.push(Run {
            start: s,
            minutes: (e - s).as_seconds_f64() / 60.0,
        });
    }
    out
}

/// UTC calendar day (`YYYY-MM-DD`) of a timestamp.
fn utc_day(ts: OffsetDateTime) -> String {
    let utc = ts.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}",
        utc.year(),
        u8::from(utc.month()),
        utc.day()
    )
}

/// How to group longest runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LongestBucket {
    /// One bucket per UTC calendar day the run started on.
    Day,
    /// One bucket per detected episode: a gap-tolerant chain of the coding's
    /// interval observations (e.g. one sleep period), keyed by the RFC 3339
    /// UTC instant the episode began. A run crossing a calendar-day boundary
    /// stays whole inside its episode. Every episode in the window is
    /// reported, `0.0` when it contains no in-range run.
    Episode,
}

/// Parameters for [`longest_continuous_in_value_range`].
pub struct LongestContinuousParams<'a> {
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
    /// Maximum gap in seconds between consecutive intervals that still counts
    /// as continuous (and, for [`LongestBucket::Episode`], that still chains
    /// intervals into one episode).
    pub gap_seconds: i64,
    /// How to group the result.
    pub bucket: LongestBucket,
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
    now: OffsetDateTime,
    params: LongestContinuousParams<'_>,
) -> Result<LongestContinuousInRange, sqlx::Error> {
    if params.bucket == LongestBucket::Episode {
        return longest_by_episode(pool, now, &params).await;
    }
    let by_day = longest_by_day(pool, &params).await?;
    let confidence_by_bucket = bucket_confidence_for(pool, now, &params).await?;

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
}

/// Fetch qualifying intervals and reduce them to the longest run per UTC
/// start day.
///
/// Split out from `longest_continuous_in_value_range` to keep that function
/// within the line-count lint; the query and walker logic are unchanged from
/// before per-bucket confidence was added.
async fn longest_by_day(
    pool: &SqlitePool,
    params: &LongestContinuousParams<'_>,
) -> Result<BTreeMap<String, f64>, sqlx::Error> {
    let &LongestContinuousParams {
        coding_system,
        coding_code,
        start,
        end,
        value_min,
        value_max,
        gap_seconds,
        bucket: _,
    } = params;
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
    Ok(by_day)
}

/// Episode path: chain ALL of the coding's intervals into episodes, walk the
/// in-range intervals into runs, and report each episode's longest run.
async fn longest_by_episode(
    pool: &SqlitePool,
    now: OffsetDateTime,
    params: &LongestContinuousParams<'_>,
) -> Result<LongestContinuousInRange, sqlx::Error> {
    let all = fetch_all_intervals(
        pool,
        params.coding_system,
        params.coding_code,
        params.start,
        params.end,
    )
    .await?;
    let intervals: Vec<(OffsetDateTime, OffsetDateTime)> =
        all.iter().map(|r| (r.start, r.end)).collect();
    let episodes = detect_episodes(&intervals, params.gap_seconds);

    let in_range: Vec<(OffsetDateTime, OffsetDateTime)> = all
        .iter()
        .filter(|r| {
            r.value
                .is_some_and(|v| v >= params.value_min && v <= params.value_max)
        })
        .map(|r| (r.start, r.end))
        .collect();
    let mut longest = vec![0.0f64; episodes.len()];
    for run in runs(&in_range, params.gap_seconds) {
        if let Some(idx) = episode_index_for(&episodes, run.start) {
            longest[idx] = longest[idx].max(run.minutes);
        }
    }

    let contributions: Vec<(String, String, Option<String>)> = all
        .iter()
        .filter_map(|r| {
            episode_index_for(&episodes, r.start).map(|idx| {
                (
                    utc_instant_key(episodes[idx].start),
                    r.source.clone(),
                    r.document_date.clone(),
                )
            })
        })
        .collect();
    let confidence_by_bucket = roll_up_bucket_confidence(pool, now, &contributions).await?;

    Ok(LongestContinuousInRange {
        per_bucket: episodes
            .iter()
            .zip(longest)
            .map(|(episode, longest_minutes)| {
                let label = utc_instant_key(episode.start);
                BucketLongest {
                    confidence: confidence_by_bucket
                        .get(&label)
                        .copied()
                        .unwrap_or(DayConfidence::Confirmed),
                    bucket_start: label,
                    longest_minutes,
                }
            })
            .collect(),
    })
}

/// Roll up per-bucket confidence for the same filter as the interval fetch
/// in [`longest_continuous_in_value_range`].
///
/// Split out from `longest_continuous_in_value_range` to keep that function
/// within the line-count lint: this companion query gathers per-source-day
/// contributions (grouped by UTC observation day, source, and document date)
/// and folds them through `roll_up_bucket_confidence`.
async fn bucket_confidence_for(
    pool: &SqlitePool,
    now: OffsetDateTime,
    params: &LongestContinuousParams<'_>,
) -> Result<std::collections::HashMap<String, DayConfidence>, sqlx::Error> {
    let &LongestContinuousParams {
        coding_system,
        coding_code,
        start,
        end,
        value_min,
        value_max,
        gap_seconds: _,
        bucket: _,
    } = params;
    let contributions = crate::queries::day_confidence::contributions_for_filter(
        pool,
        coding_system,
        coding_code,
        start,
        end,
        value_min,
        value_max,
    )
    .await?;
    roll_up_bucket_confidence(pool, now, &contributions).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clinical::{AASM_SLEEP_STAGE_CODE, AASM_SLEEP_STAGE_SYSTEM};
    use crate::queries::test_support::{seed_interval_observations, IntervalObsSpec};
    use crate::sources::DayConfidence;
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
            (
                datetime!(2026-01-01 22:00:00 UTC),
                datetime!(2026-01-01 22:05:00 UTC),
            ),
            (
                datetime!(2026-01-01 22:05:00 UTC),
                datetime!(2026-01-01 22:10:00 UTC),
            ),
        ];
        let r = runs(&iv, 0);
        assert_eq!(r.len(), 1);
        assert!((r[0].minutes - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn runs_gap_over_tolerance_splits() {
        // 5-min interval, then a 10-min gap, then a 5-min interval. gap 0 splits.
        let iv = [
            (
                datetime!(2026-01-01 22:00:00 UTC),
                datetime!(2026-01-01 22:05:00 UTC),
            ),
            (
                datetime!(2026-01-01 22:15:00 UTC),
                datetime!(2026-01-01 22:20:00 UTC),
            ),
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
            (
                datetime!(2026-01-01 22:00:00 UTC),
                datetime!(2026-01-01 22:05:00 UTC),
            ),
            (
                datetime!(2026-01-01 22:15:00 UTC),
                datetime!(2026-01-01 22:20:00 UTC),
            ),
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
            datetime!(2026-06-01 00:00:00 UTC),
            LongestContinuousParams {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                start: datetime!(2026-01-01 00:00:00 UTC),
                end: datetime!(2026-01-02 00:00:00 UTC),
                value_min: 1.0,
                value_max: 4.0,
                gap_seconds: 0,
                bucket: LongestBucket::Day,
            },
        )
        .await
        .expect("query");
        assert_eq!(
            result,
            LongestContinuousInRange {
                per_bucket: vec![BucketLongest {
                    bucket_start: "2026-01-01".to_string(),
                    longest_minutes: 10.0,
                    confidence: DayConfidence::Confirmed,
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
            datetime!(2026-06-01 00:00:00 UTC),
            LongestContinuousParams {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                start: datetime!(2026-01-01 00:00:00 UTC),
                end: datetime!(2026-01-02 00:00:00 UTC),
                value_min: 1.0,
                value_max: 4.0,
                gap_seconds: 0,
                bucket: LongestBucket::Day,
            },
        )
        .await
        .expect("query");
        assert_eq!(
            result,
            LongestContinuousInRange {
                per_bucket: vec![BucketLongest {
                    bucket_start: "2026-01-01".to_string(),
                    longest_minutes: 5.0,
                    confidence: DayConfidence::Confirmed,
                }],
            }
        );
    }

    /// Night A: contiguous epochs 23:50 -> 00:10 with a deep (3.0) run of
    /// 23:55 -> 00:05 crossing UTC midnight. Night B: one deep 5-min epoch.
    fn deep_run_nights() -> [IntervalObsSpec; 5] {
        let epoch = |start, end, stage| IntervalObsSpec {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            effective_start: start,
            effective_end: end,
            value_quantity: stage,
        };
        [
            epoch(
                datetime!(2026-06-26 23:50:00 UTC),
                datetime!(2026-06-26 23:55:00 UTC),
                2.0,
            ),
            epoch(
                datetime!(2026-06-26 23:55:00 UTC),
                datetime!(2026-06-27 00:00:00 UTC),
                3.0,
            ),
            epoch(
                datetime!(2026-06-27 00:00:00 UTC),
                datetime!(2026-06-27 00:05:00 UTC),
                3.0,
            ),
            epoch(
                datetime!(2026-06-27 00:05:00 UTC),
                datetime!(2026-06-27 00:10:00 UTC),
                2.0,
            ),
            epoch(
                datetime!(2026-06-27 23:00:00 UTC),
                datetime!(2026-06-27 23:05:00 UTC),
                3.0,
            ),
        ]
    }

    #[tokio::test]
    async fn episode_bucket_keeps_midnight_crossing_run_whole() {
        let (pool, _) = seed_interval_observations(&deep_run_nights()).await;
        let result = longest_continuous_in_value_range(
            &pool,
            datetime!(2026-07-01 00:00:00 UTC),
            LongestContinuousParams {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                start: datetime!(2026-06-26 00:00:00 UTC),
                end: datetime!(2026-06-29 00:00:00 UTC),
                value_min: 3.0,
                value_max: 3.0,
                gap_seconds: 0,
                bucket: LongestBucket::Episode,
            },
        )
        .await
        .expect("query");
        // The 10-min deep run straddles midnight but stays whole inside
        // night A's episode bucket; night B's episode reports its 5-min run.
        assert_eq!(
            result,
            LongestContinuousInRange {
                per_bucket: vec![
                    BucketLongest {
                        bucket_start: "2026-06-26T23:50:00Z".to_string(),
                        longest_minutes: 10.0,
                        confidence: DayConfidence::Confirmed,
                    },
                    BucketLongest {
                        bucket_start: "2026-06-27T23:00:00Z".to_string(),
                        longest_minutes: 5.0,
                        confidence: DayConfidence::Confirmed,
                    },
                ],
            }
        );
    }

    #[tokio::test]
    async fn episode_bucket_reports_runless_episode_as_zero() {
        // One night of purely light (2.0) sleep: the episode exists but has
        // no deep run.
        let specs = [IntervalObsSpec {
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            effective_start: datetime!(2026-06-27 01:00:00 UTC),
            effective_end: datetime!(2026-06-27 01:05:00 UTC),
            value_quantity: 2.0,
        }];
        let (pool, _) = seed_interval_observations(&specs).await;
        let result = longest_continuous_in_value_range(
            &pool,
            datetime!(2026-07-01 00:00:00 UTC),
            LongestContinuousParams {
                coding_system: AASM_SLEEP_STAGE_SYSTEM,
                coding_code: AASM_SLEEP_STAGE_CODE,
                start: datetime!(2026-06-26 00:00:00 UTC),
                end: datetime!(2026-06-29 00:00:00 UTC),
                value_min: 3.0,
                value_max: 3.0,
                gap_seconds: 0,
                bucket: LongestBucket::Episode,
            },
        )
        .await
        .expect("query");
        assert_eq!(
            result,
            LongestContinuousInRange {
                per_bucket: vec![BucketLongest {
                    bucket_start: "2026-06-27T01:00:00Z".to_string(),
                    longest_minutes: 0.0,
                    confidence: DayConfidence::Confirmed,
                }],
            }
        );
    }
}
