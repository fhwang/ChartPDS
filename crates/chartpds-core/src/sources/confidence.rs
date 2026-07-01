//! Confidence tracking types and date helpers.
//!
//! Each adapter has a per-day confidence model that determines whether a
//! day's data is settled (`Confirmed`) or may still change (`Provisional`).
//! The sync daemon skips confirmed days to avoid redundant API calls.

/// Whether a day's data is settled or may still change.
///
/// Used by each adapter's confidence function to decide whether a day
/// needs to be re-pulled during sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum DayConfidence {
    /// Data for this day is stable — no need to re-pull.
    Confirmed,
    /// Data for this day may still change — should be re-pulled.
    Provisional,
}

/// Per-date confidence annotation.
///
/// Pairs a date string with its confidence status, useful for returning
/// batch confidence results to callers.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConfidenceByDate {
    /// ISO-8601 date string (e.g. `"2026-01-15"`).
    pub date: String,
    /// Whether this day's data is confirmed or provisional.
    pub confidence: DayConfidence,
}

/// Enumerate every `YYYY-MM-DD` date string in `[start, end]` inclusive.
///
/// Returns an empty `Vec` if `start > end`.
///
/// # Panics
///
/// Panics if `start` or `end` cannot be parsed as `YYYY-MM-DD`.
#[must_use]
pub fn enumerate_dates(start: &str, end: &str) -> Vec<String> {
    let format = time::macros::format_description!("[year]-[month]-[day]");
    let start_date =
        time::Date::parse(start, &format).expect("valid start date in YYYY-MM-DD format");
    let end_date = time::Date::parse(end, &format).expect("valid end date in YYYY-MM-DD format");

    let mut dates = Vec::new();
    let mut cursor = start_date;
    while cursor <= end_date {
        dates.push(format!(
            "{:04}-{:02}-{:02}",
            cursor.year(),
            cursor.month() as u8,
            cursor.day()
        ));
        cursor = match cursor.next_day() {
            Some(d) => d,
            None => break,
        };
    }
    dates
}

/// Select which dates still need fetching from a source API.
///
/// The general adapter fetch rule: a day needs fetching when it is either
/// **uncovered** (we have not ingested it yet) or **unsettled** (the source's
/// confidence rule says it may still change). A day is skipped only when it is
/// BOTH covered AND settled. This keeps backfill correct — uncovered days are
/// always fetched regardless of age — while avoiding redundant re-pulls of old,
/// stable days. Dedup at ingestion remains a safety net, not the primary
/// correctness mechanism.
#[must_use]
pub fn select_fetch_dates<C, S>(dates: &[String], is_covered: C, is_settled: S) -> Vec<String>
where
    C: Fn(&str) -> bool,
    S: Fn(&str) -> bool,
{
    dates
        .iter()
        .filter(|d| !is_covered(d) || !is_settled(d))
        .cloned()
        .collect()
}

/// Compute the `frontier_last_advanced_at` timestamp for a sync write.
///
/// The frontier "advances" whenever its value changes (it only ever moves
/// forward, since it is a running maximum). When it advances, stamp `now`;
/// otherwise preserve the previously recorded advance timestamp. This gives a
/// wall-clock signal of how long the frontier has been stuck — robust to the
/// daemon's tick cadence and independent of tick counting.
#[must_use]
pub(crate) fn frontier_advanced_at(
    previous_frontier: Option<&str>,
    new_frontier: Option<&str>,
    previous_advanced_at: Option<&str>,
    now: &str,
) -> Option<String> {
    if new_frontier == previous_frontier {
        previous_advanced_at.map(ToOwned::to_owned)
    } else {
        Some(now.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontier_advanced_first_time_stamps_now() {
        // No prior frontier → any new frontier is an advance.
        let stamped = frontier_advanced_at(
            None,
            Some("2026-01-14T00:00:00Z"),
            None,
            "2026-01-15T10:00:00Z",
        );
        assert_eq!(stamped.as_deref(), Some("2026-01-15T10:00:00Z"));
    }

    #[test]
    fn frontier_advanced_when_value_changes_stamps_now() {
        let stamped = frontier_advanced_at(
            Some("2026-01-14T00:00:00Z"),
            Some("2026-01-15T00:00:00Z"),
            Some("2026-01-14T09:00:00Z"),
            "2026-01-15T10:00:00Z",
        );
        assert_eq!(stamped.as_deref(), Some("2026-01-15T10:00:00Z"));
    }

    #[test]
    fn frontier_unchanged_preserves_previous_advanced_at() {
        // Frontier did not move → keep the old advanced-at timestamp, do not
        // re-stamp to now.
        let stamped = frontier_advanced_at(
            Some("2026-01-14T00:00:00Z"),
            Some("2026-01-14T00:00:00Z"),
            Some("2026-01-14T09:00:00Z"),
            "2026-01-15T10:00:00Z",
        );
        assert_eq!(stamped.as_deref(), Some("2026-01-14T09:00:00Z"));
    }

    #[test]
    fn frontier_both_none_preserves_previous_advanced_at() {
        let stamped = frontier_advanced_at(None, None, None, "2026-01-15T10:00:00Z");
        assert_eq!(stamped, None);
    }

    #[test]
    fn enumerate_dates_multi_day_range() {
        let dates = enumerate_dates("2026-01-01", "2026-01-03");
        assert_eq!(
            dates,
            vec![
                "2026-01-01".to_owned(),
                "2026-01-02".to_owned(),
                "2026-01-03".to_owned(),
            ]
        );
    }

    #[test]
    fn enumerate_dates_single_day() {
        let dates = enumerate_dates("2026-01-01", "2026-01-01");
        assert_eq!(dates, vec!["2026-01-01".to_owned()]);
    }

    #[test]
    fn enumerate_dates_start_after_end_returns_empty() {
        let dates = enumerate_dates("2026-01-03", "2026-01-01");
        assert!(dates.is_empty());
    }

    #[test]
    fn select_fetch_dates_backfill_nothing_covered() {
        let dates = vec![
            "2026-01-01".to_owned(),
            "2026-01-02".to_owned(),
            "2026-01-03".to_owned(),
        ];
        // Nothing covered, everything settled → all dates still need fetching
        // (this is the backfill regression: old settled days must be pulled).
        let needed = select_fetch_dates(&dates, |_| false, |_| true);
        assert_eq!(needed, dates);
    }

    #[test]
    fn select_fetch_dates_steady_state_skips_old_covered_settled() {
        let dates = vec![
            "2026-01-01".to_owned(),
            "2026-01-02".to_owned(),
            "2026-01-03".to_owned(),
        ];
        // Old days covered + settled; the last day is unsettled (recent).
        let covered = |_: &str| true;
        let settled = |d: &str| d != "2026-01-03";
        let needed = select_fetch_dates(&dates, covered, settled);
        assert_eq!(needed, vec!["2026-01-03".to_owned()]);
    }

    #[test]
    fn select_fetch_dates_keeps_uncovered_gap_day() {
        let dates = vec![
            "2026-01-01".to_owned(),
            "2026-01-02".to_owned(),
            "2026-01-03".to_owned(),
        ];
        // All settled, but the middle day was never ingested → must be fetched.
        let covered = |d: &str| d != "2026-01-02";
        let needed = select_fetch_dates(&dates, covered, |_| true);
        assert_eq!(needed, vec!["2026-01-02".to_owned()]);
    }
}
