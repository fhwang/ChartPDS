//! Heart-rate parser with interval synthesis.
//!
//! Converts point-in-time heart-rate samples from Google Health into interval
//! observations. Synthesizes `[start, end)` intervals based on the gap between
//! consecutive samples:
//!
//! - Gap <= 90 s: interval ends at the next sample's timestamp.
//! - Gap > 90 s (device off-wrist): interval is capped at 60 s.
//! - Last sample: interval is 60 s.

use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use super::api::IntradayResult;
use crate::sources;

const DEFAULT_INTERVAL_SECONDS: i64 = 60;
const GAP_CAP_SECONDS: i64 = 90;

/// A parsed heart-rate observation ready for index insertion.
#[derive(Debug, Clone)]
pub struct ParsedHeartRate {
    /// Start of the observation interval.
    pub effective_start: OffsetDateTime,
    /// End of the observation interval.
    pub effective_end: OffsetDateTime,
    /// Heart rate in beats per minute.
    pub beats_per_minute: f64,
}

/// Parse intraday heart-rate samples into interval observations.
///
/// Sorts samples by `physical_time`, then synthesizes intervals:
/// - If the next sample is within [`GAP_CAP_SECONDS`], the interval extends
///   to the next sample's timestamp.
/// - Otherwise (gap > 90 s, or last sample), the interval is
///   [`DEFAULT_INTERVAL_SECONDS`].
///
/// # Errors
///
/// Returns [`sources::Error::Parse`] if any sample's `physical_time` cannot be
/// parsed as RFC 3339.
pub fn parse_intraday_day(result: &IntradayResult) -> Result<Vec<ParsedHeartRate>, sources::Error> {
    if result.samples.is_empty() {
        return Ok(Vec::new());
    }

    // Parse timestamps and sort by time.
    let mut timed: Vec<(OffsetDateTime, f64)> = result
        .samples
        .iter()
        .map(|s| {
            let ts = OffsetDateTime::parse(&s.physical_time, &Rfc3339).map_err(|err| {
                sources::Error::Parse {
                    reason: format!("invalid physical_time {:?}: {err}", s.physical_time),
                }
            })?;
            #[allow(
                clippy::cast_precision_loss,
                reason = "BPM is always a small integer, no precision lost"
            )]
            let bpm = s.beats_per_minute as f64;
            Ok((ts, bpm))
        })
        .collect::<Result<Vec<_>, sources::Error>>()?;

    timed.sort_by_key(|(ts, _)| *ts);

    let mut observations = Vec::with_capacity(timed.len());
    for i in 0..timed.len() {
        let (start, bpm) = timed[i];
        let end = if i + 1 < timed.len() {
            let next_start = timed[i + 1].0;
            let gap = (next_start - start).whole_seconds();
            if gap <= GAP_CAP_SECONDS {
                next_start
            } else {
                start + time::Duration::seconds(DEFAULT_INTERVAL_SECONDS)
            }
        } else {
            start + time::Duration::seconds(DEFAULT_INTERVAL_SECONDS)
        };

        observations.push(ParsedHeartRate {
            effective_start: start,
            effective_end: end,
            beats_per_minute: bpm,
        });
    }

    Ok(observations)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::fitbit::api::HeartRateSample;
    use time::macros::datetime;

    fn sample(time: &str, bpm: i64) -> HeartRateSample {
        HeartRateSample {
            physical_time: time.to_owned(),
            beats_per_minute: bpm,
        }
    }

    fn result_from(samples: Vec<HeartRateSample>) -> IntradayResult {
        IntradayResult {
            samples,
            raw_pages: Vec::new(),
        }
    }

    #[test]
    fn three_samples_close_together_synthesize_intervals() {
        // 3 samples 30 s apart: first two intervals end at next sample,
        // last gets the default +60 s.
        let r = result_from(vec![
            sample("2026-01-01T08:00:00Z", 70),
            sample("2026-01-01T08:00:30Z", 72),
            sample("2026-01-01T08:01:00Z", 75),
        ]);

        let obs = parse_intraday_day(&r).expect("parse");
        assert_eq!(obs.len(), 3);

        // First interval: 08:00:00 -> 08:00:30 (30 s gap, under cap)
        assert_eq!(obs[0].effective_start, datetime!(2026-01-01 08:00:00 UTC));
        assert_eq!(obs[0].effective_end, datetime!(2026-01-01 08:00:30 UTC));
        assert!((obs[0].beats_per_minute - 70.0).abs() < f64::EPSILON);

        // Second interval: 08:00:30 -> 08:01:00 (30 s gap, under cap)
        assert_eq!(obs[1].effective_start, datetime!(2026-01-01 08:00:30 UTC));
        assert_eq!(obs[1].effective_end, datetime!(2026-01-01 08:01:00 UTC));
        assert!((obs[1].beats_per_minute - 72.0).abs() < f64::EPSILON);

        // Third interval: 08:01:00 -> 08:02:00 (last sample, +60 s default)
        assert_eq!(obs[2].effective_start, datetime!(2026-01-01 08:01:00 UTC));
        assert_eq!(obs[2].effective_end, datetime!(2026-01-01 08:02:00 UTC));
        assert!((obs[2].beats_per_minute - 75.0).abs() < f64::EPSILON);
    }

    #[test]
    fn gap_over_90s_caps_interval_at_60s() {
        // 2 samples 120 s apart: first interval should be capped at 60 s.
        let r = result_from(vec![
            sample("2026-01-01T08:00:00Z", 70),
            sample("2026-01-01T08:02:00Z", 80),
        ]);

        let obs = parse_intraday_day(&r).expect("parse");
        assert_eq!(obs.len(), 2);

        // First interval: 08:00:00 -> 08:01:00 (120 s gap > 90 s cap => +60 s)
        assert_eq!(obs[0].effective_start, datetime!(2026-01-01 08:00:00 UTC));
        assert_eq!(obs[0].effective_end, datetime!(2026-01-01 08:01:00 UTC));

        // Second interval: 08:02:00 -> 08:03:00 (last sample, +60 s default)
        assert_eq!(obs[1].effective_start, datetime!(2026-01-01 08:02:00 UTC));
        assert_eq!(obs[1].effective_end, datetime!(2026-01-01 08:03:00 UTC));
    }

    #[test]
    fn single_sample_gets_60s_interval() {
        let r = result_from(vec![sample("2026-01-01T08:00:00Z", 65)]);

        let obs = parse_intraday_day(&r).expect("parse");
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].effective_start, datetime!(2026-01-01 08:00:00 UTC));
        assert_eq!(obs[0].effective_end, datetime!(2026-01-01 08:01:00 UTC));
        assert!((obs[0].beats_per_minute - 65.0).abs() < f64::EPSILON);
    }

    #[test]
    fn empty_result_returns_empty_vec() {
        let r = result_from(vec![]);
        let obs = parse_intraday_day(&r).expect("parse");
        assert!(obs.is_empty());
    }

    #[test]
    fn samples_sorted_by_time_regardless_of_input_order() {
        // Input in reverse chronological order.
        let r = result_from(vec![
            sample("2026-01-01T08:01:00Z", 75),
            sample("2026-01-01T08:00:00Z", 70),
            sample("2026-01-01T08:00:30Z", 72),
        ]);

        let obs = parse_intraday_day(&r).expect("parse");
        assert_eq!(obs.len(), 3);

        // Output should be chronological.
        assert_eq!(obs[0].effective_start, datetime!(2026-01-01 08:00:00 UTC));
        assert_eq!(obs[1].effective_start, datetime!(2026-01-01 08:00:30 UTC));
        assert_eq!(obs[2].effective_start, datetime!(2026-01-01 08:01:00 UTC));

        // BPM should match the sorted order.
        assert!((obs[0].beats_per_minute - 70.0).abs() < f64::EPSILON);
        assert!((obs[1].beats_per_minute - 72.0).abs() < f64::EPSILON);
        assert!((obs[2].beats_per_minute - 75.0).abs() < f64::EPSILON);
    }
}
