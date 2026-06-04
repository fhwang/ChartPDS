//! Fitbit day-confidence model (stability-based).
//!
//! A Fitbit day is confirmed when:
//! 1. It is outside the force-refresh window (older than 5 days).
//! 2. A freshness frontier exists and has passed the day (with 36h buffer).
//! 3. A prior pull exists with the same sample count (data is stable).
//!
//! Otherwise, the day is provisional and should be re-pulled.

use crate::index::SourceDayState;
use crate::sources::confidence::DayConfidence;

/// Number of recent days that are always considered provisional,
/// regardless of stability. Recent data from Fitbit can still be
/// revised by the API for several days.
const FORCE_REFRESH_DAYS: i64 = 5;

/// Hours after midnight of a day before the freshness frontier is
/// considered to have passed that day. Accounts for late-arriving
/// data (e.g. syncing a watch mid-morning the next day).
const DAY_END_BUFFER_HOURS: i64 = 36;

/// Determine confidence for a single Fitbit day.
///
/// This is a pure function — no async, no database. It takes
/// pre-fetched snapshots and returns a confidence level.
///
/// # Arguments
///
/// * `today` — today's date as `YYYY-MM-DD`.
/// * `date` — the date to evaluate as `YYYY-MM-DD`.
/// * `freshness_frontier` — the source's freshness frontier as an
///   RFC 3339 timestamp, if any.
/// * `day_state` — the previously recorded `SourceDayState` for this
///   source-day, if any.
#[must_use]
pub fn fitbit_day_confidence(
    today: &str,
    date: &str,
    freshness_frontier: Option<&str>,
    day_state: Option<&SourceDayState>,
) -> DayConfidence {
    // 1. If date is within the last FORCE_REFRESH_DAYS of today → Provisional.
    if is_within_force_refresh_window(today, date) {
        return DayConfidence::Provisional;
    }

    // 2. If no freshness_frontier → Provisional.
    let Some(frontier_str) = freshness_frontier else {
        return DayConfidence::Provisional;
    };

    // 3. If freshness_frontier < day + DAY_END_BUFFER_HOURS → Provisional.
    if !frontier_past_day_buffer(frontier_str, date) {
        return DayConfidence::Provisional;
    }

    // 4. If no day_state or samples_count_prev is None → Provisional.
    let Some(state) = day_state else {
        return DayConfidence::Provisional;
    };
    let Some(prev) = state.samples_count_prev else {
        return DayConfidence::Provisional;
    };

    // 5. If samples_count != samples_count_prev → Provisional.
    if state.samples_count != prev {
        return DayConfidence::Provisional;
    }

    // 6. Otherwise → Confirmed.
    DayConfidence::Confirmed
}

/// Check if a date is within the last `FORCE_REFRESH_DAYS` of today.
///
/// Returns `true` if `(today - date) < FORCE_REFRESH_DAYS`.
fn is_within_force_refresh_window(today: &str, date: &str) -> bool {
    let format = time::macros::format_description!("[year]-[month]-[day]");
    let Ok(today_date) = time::Date::parse(today, &format) else {
        return true; // can't parse → treat as provisional
    };
    let Ok(target_date) = time::Date::parse(date, &format) else {
        return true;
    };
    let diff = today_date - target_date;
    diff.whole_days() < FORCE_REFRESH_DAYS
}

/// Check if the freshness frontier has passed `date + DAY_END_BUFFER_HOURS`.
///
/// The frontier timestamp (RFC 3339) must be at or after midnight UTC of
/// the given date plus the buffer hours.
fn frontier_past_day_buffer(frontier: &str, date: &str) -> bool {
    let date_format = time::macros::format_description!("[year]-[month]-[day]");
    let Ok(target_date) = time::Date::parse(date, &date_format) else {
        return false;
    };

    // date midnight UTC + buffer hours
    let day_midnight = target_date.midnight().assume_utc();
    let threshold = day_midnight + time::Duration::hours(DAY_END_BUFFER_HOURS);

    let Ok(frontier_dt) =
        time::OffsetDateTime::parse(frontier, &time::format_description::well_known::Rfc3339)
    else {
        return false;
    };

    frontier_dt >= threshold
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::SourceDayState;

    /// Helper to build a `SourceDayState` with given counts.
    fn day_state(count: i64, prev: Option<i64>) -> SourceDayState {
        SourceDayState {
            source_name: "fitbit".to_owned(),
            date: "2026-01-10".to_owned(),
            samples_count: count,
            samples_count_prev: prev,
            last_pulled_at: "2026-01-11T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn old_date_stable_counts_frontier_past_is_confirmed() {
        let state = day_state(100, Some(100));
        // Date is Jan 10, today is Jan 20 (10 days ago, outside 5-day window).
        // Frontier is Jan 12 noon — well past Jan 10 + 36h = Jan 11 12:00.
        let result = fitbit_day_confidence(
            "2026-01-20",
            "2026-01-10",
            Some("2026-01-12T12:00:00Z"),
            Some(&state),
        );
        assert_eq!(result, DayConfidence::Confirmed);
    }

    #[test]
    fn recent_date_within_5_days_is_provisional() {
        let state = day_state(100, Some(100));
        // Date is Jan 17, today is Jan 20 (3 days ago, inside 5-day window).
        let result = fitbit_day_confidence(
            "2026-01-20",
            "2026-01-17",
            Some("2026-01-20T12:00:00Z"),
            Some(&state),
        );
        assert_eq!(result, DayConfidence::Provisional);
    }

    #[test]
    fn old_date_different_counts_is_provisional() {
        let state = day_state(100, Some(95));
        let result = fitbit_day_confidence(
            "2026-01-20",
            "2026-01-10",
            Some("2026-01-12T12:00:00Z"),
            Some(&state),
        );
        assert_eq!(result, DayConfidence::Provisional);
    }

    #[test]
    fn old_date_no_day_state_is_provisional() {
        let result = fitbit_day_confidence(
            "2026-01-20",
            "2026-01-10",
            Some("2026-01-12T12:00:00Z"),
            None,
        );
        assert_eq!(result, DayConfidence::Provisional);
    }

    #[test]
    fn old_date_no_frontier_is_provisional() {
        let state = day_state(100, Some(100));
        let result = fitbit_day_confidence("2026-01-20", "2026-01-10", None, Some(&state));
        assert_eq!(result, DayConfidence::Provisional);
    }

    #[test]
    fn old_date_frontier_not_past_buffer_is_provisional() {
        let state = day_state(100, Some(100));
        // Frontier is Jan 11 at 06:00 — that's only 30h past Jan 10 midnight,
        // which is less than the 36h buffer.
        let result = fitbit_day_confidence(
            "2026-01-20",
            "2026-01-10",
            Some("2026-01-11T06:00:00Z"),
            Some(&state),
        );
        assert_eq!(result, DayConfidence::Provisional);
    }

    #[test]
    fn old_date_no_prev_count_is_provisional() {
        let state = day_state(100, None);
        let result = fitbit_day_confidence(
            "2026-01-20",
            "2026-01-10",
            Some("2026-01-12T12:00:00Z"),
            Some(&state),
        );
        assert_eq!(result, DayConfidence::Provisional);
    }
}
