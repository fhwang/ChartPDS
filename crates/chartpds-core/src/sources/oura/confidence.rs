//! Oura day-confidence model (time-based).
//!
//! Oura sleep data settles quickly — within hours of waking up. A day
//! is considered confirmed 24 hours after it ends (i.e. 24 hours after
//! midnight UTC of the following day). No stability check is needed.

use crate::sources::confidence::DayConfidence;

/// Hours after the end of a day before it is considered confirmed.
/// Sleep data settles within a few hours, but we use 24h as a
/// conservative buffer.
const PROVISIONAL_WINDOW_HOURS: i64 = 24;

/// Determine confidence for a single Oura day.
///
/// This is a pure function — no async, no database. It compares the
/// current time to the day's end plus a provisional window.
///
/// # Arguments
///
/// * `now` — the current time as an `OffsetDateTime`.
/// * `date` — the date to evaluate as `YYYY-MM-DD`.
#[must_use]
pub fn oura_day_confidence(now: time::OffsetDateTime, date: &str) -> DayConfidence {
    let date_format = time::macros::format_description!("[year]-[month]-[day]");
    let Ok(target_date) = time::Date::parse(date, &date_format) else {
        return DayConfidence::Provisional;
    };

    // date_end = midnight UTC of the day after `date`.
    let date_end = match target_date.next_day() {
        Some(next) => next.midnight().assume_utc(),
        None => return DayConfidence::Provisional,
    };

    // provisional_until = date_end + PROVISIONAL_WINDOW_HOURS.
    let provisional_until = date_end + time::Duration::hours(PROVISIONAL_WINDOW_HOURS);

    if now < provisional_until {
        DayConfidence::Provisional
    } else {
        DayConfidence::Confirmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse an RFC 3339 timestamp into an `OffsetDateTime`.
    fn parse_rfc3339(s: &str) -> time::OffsetDateTime {
        time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
            .expect("valid RFC 3339")
    }

    #[test]
    fn three_days_ago_is_confirmed() {
        // Now is Jan 20 noon, date is Jan 17.
        // provisional_until = Jan 18 midnight + 24h = Jan 19 midnight.
        // Jan 20 noon > Jan 19 midnight → Confirmed.
        let now = parse_rfc3339("2026-01-20T12:00:00Z");
        assert_eq!(
            oura_day_confidence(now, "2026-01-17"),
            DayConfidence::Confirmed
        );
    }

    #[test]
    fn today_is_provisional() {
        // Now is Jan 20 noon, date is Jan 20.
        // provisional_until = Jan 21 midnight + 24h = Jan 22 midnight.
        // Jan 20 noon < Jan 22 midnight → Provisional.
        let now = parse_rfc3339("2026-01-20T12:00:00Z");
        assert_eq!(
            oura_day_confidence(now, "2026-01-20"),
            DayConfidence::Provisional
        );
    }

    #[test]
    fn yesterday_within_24h_window_is_provisional() {
        // Now is Jan 20 at 10:00, date is Jan 19.
        // provisional_until = Jan 20 midnight + 24h = Jan 21 midnight.
        // Jan 20 10:00 < Jan 21 midnight → Provisional.
        let now = parse_rfc3339("2026-01-20T10:00:00Z");
        assert_eq!(
            oura_day_confidence(now, "2026-01-19"),
            DayConfidence::Provisional
        );
    }

    #[test]
    fn yesterday_past_24h_window_is_confirmed() {
        // Now is Jan 21 at 01:00, date is Jan 19.
        // provisional_until = Jan 20 midnight + 24h = Jan 21 midnight.
        // Jan 21 01:00 > Jan 21 midnight → Confirmed.
        let now = parse_rfc3339("2026-01-21T01:00:00Z");
        assert_eq!(
            oura_day_confidence(now, "2026-01-19"),
            DayConfidence::Confirmed
        );
    }

    #[test]
    fn exactly_at_threshold_is_confirmed() {
        // Now is exactly Jan 21 midnight, date is Jan 19.
        // provisional_until = Jan 20 midnight + 24h = Jan 21 midnight.
        // Jan 21 midnight >= Jan 21 midnight → Confirmed.
        let now = parse_rfc3339("2026-01-21T00:00:00Z");
        assert_eq!(
            oura_day_confidence(now, "2026-01-19"),
            DayConfidence::Confirmed
        );
    }
}
