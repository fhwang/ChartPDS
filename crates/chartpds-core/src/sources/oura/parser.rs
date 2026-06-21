//! Oura sleep-epoch parser.
//!
//! Parses the `sleep_phase_5_min` string from an Oura sleep session
//! into per-epoch observations with AASM sleep-stage coding. Each
//! character in the string represents one 5-minute epoch.

use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use super::api::OuraSleepSession;
use super::sleep_stage::{oura_char_to_aasm, EPOCH_SECONDS};
use crate::clinical::AasmSleepStage;
use crate::sources;

/// A single parsed sleep observation (one 5-minute epoch).
#[derive(Debug, Clone)]
pub struct ParsedSleepObservation {
    /// Start of the epoch interval.
    pub effective_start: OffsetDateTime,
    /// End of the epoch interval (start + 300 seconds).
    pub effective_end: OffsetDateTime,
    /// AASM sleep stage for this epoch.
    pub stage: AasmSleepStage,
}

/// Parse a `sleep_phase_5_min` string into per-epoch observations.
///
/// Each character in `sleep_phase_5_min` maps to one 5-minute epoch
/// starting from `bedtime_start`. Unknown characters produce a parse
/// error. An empty string returns an empty vec.
///
/// # Arguments
///
/// * `bedtime_start` - RFC 3339 timestamp when the sleep session started.
/// * `sleep_phase_5_min` - Per-epoch stage string from the Oura API.
///
/// # Errors
///
/// - [`sources::Error::Parse`] if `bedtime_start` is not valid RFC 3339.
/// - [`sources::Error::Parse`] if `sleep_phase_5_min` contains an
///   unknown character.
pub fn parse_sleep_epochs(
    bedtime_start: &str,
    sleep_phase_5_min: &str,
) -> sources::Result<Vec<ParsedSleepObservation>> {
    if sleep_phase_5_min.is_empty() {
        return Ok(Vec::new());
    }

    let start =
        OffsetDateTime::parse(bedtime_start, &Rfc3339).map_err(|err| sources::Error::Parse {
            reason: format!("invalid bedtime_start {bedtime_start:?}: {err}"),
        })?;

    let mut observations = Vec::with_capacity(sleep_phase_5_min.len());
    for (i, c) in sleep_phase_5_min.chars().enumerate() {
        let stage = oura_char_to_aasm(c).ok_or_else(|| sources::Error::Parse {
            reason: format!("unknown Oura stage char '{c}' at index {i}"),
        })?;

        #[allow(
            clippy::cast_possible_wrap,
            reason = "epoch index is always small (< 200 for a full night)"
        )]
        let offset_secs = i as i64 * EPOCH_SECONDS;
        let epoch_start = start + time::Duration::seconds(offset_secs);
        let epoch_end = epoch_start + time::Duration::seconds(EPOCH_SECONDS);

        observations.push(ParsedSleepObservation {
            effective_start: epoch_start,
            effective_end: epoch_end,
            stage,
        });
    }

    Ok(observations)
}

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

    let effective_start =
        OffsetDateTime::parse(&session.bedtime_start, &Rfc3339).map_err(|err| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn four_epochs_map_correctly() {
        // "4421" → Wake, Wake, N2, N3 (deep)
        let obs = parse_sleep_epochs("2026-01-14T22:00:00Z", "4421").expect("parse");
        assert_eq!(obs.len(), 4);

        assert_eq!(obs[0].stage, AasmSleepStage::Wake);
        assert_eq!(obs[1].stage, AasmSleepStage::Wake);
        assert_eq!(obs[2].stage, AasmSleepStage::N2);
        assert_eq!(obs[3].stage, AasmSleepStage::N3);
    }

    #[test]
    fn epoch_times_are_300s_apart() {
        let obs = parse_sleep_epochs("2026-01-14T22:00:00Z", "4421").expect("parse");

        assert_eq!(obs[0].effective_start, datetime!(2026-01-14 22:00:00 UTC));
        assert_eq!(obs[0].effective_end, datetime!(2026-01-14 22:05:00 UTC));

        assert_eq!(obs[1].effective_start, datetime!(2026-01-14 22:05:00 UTC));
        assert_eq!(obs[1].effective_end, datetime!(2026-01-14 22:10:00 UTC));

        assert_eq!(obs[2].effective_start, datetime!(2026-01-14 22:10:00 UTC));
        assert_eq!(obs[2].effective_end, datetime!(2026-01-14 22:15:00 UTC));

        assert_eq!(obs[3].effective_start, datetime!(2026-01-14 22:15:00 UTC));
        assert_eq!(obs[3].effective_end, datetime!(2026-01-14 22:20:00 UTC));
    }

    #[test]
    fn empty_string_returns_empty_vec() {
        let obs = parse_sleep_epochs("2026-01-14T22:00:00Z", "").expect("parse");
        assert!(obs.is_empty());
    }

    #[test]
    fn unknown_char_returns_error() {
        let result = parse_sleep_epochs("2026-01-14T22:00:00Z", "44X1");
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown Oura stage char 'X'"), "got: {msg}");
    }

    #[test]
    fn invalid_bedtime_start_returns_error() {
        let result = parse_sleep_epochs("not-a-timestamp", "4421");
        assert!(result.is_err());
    }

    #[test]
    fn all_stage_types_parsed() {
        // "1234" → N3, N2, Rem, Wake
        let obs = parse_sleep_epochs("2026-01-14T22:00:00Z", "1234").expect("parse");
        assert_eq!(obs.len(), 4);
        assert_eq!(obs[0].stage, AasmSleepStage::N3);
        assert_eq!(obs[1].stage, AasmSleepStage::N2);
        assert_eq!(obs[2].stage, AasmSleepStage::Rem);
        assert_eq!(obs[3].stage, AasmSleepStage::Wake);
    }

    #[test]
    fn timezone_offset_preserved() {
        // bedtime_start with -05:00 offset
        let obs = parse_sleep_epochs("2026-01-14T22:00:00-05:00", "42").expect("parse");
        assert_eq!(obs.len(), 2);
        // Epoch starts at 22:00 EST = 03:00 UTC next day
        assert_eq!(obs[0].effective_start, datetime!(2026-01-15 03:00:00 UTC));
        assert_eq!(obs[0].effective_end, datetime!(2026-01-15 03:05:00 UTC));
    }

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
}
