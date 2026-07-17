//! Episode detection: gap-tolerant chains of interval observations.
//!
//! An *episode* is a maximal run of a coding's interval observations where
//! each interval starts within `gap_seconds` of the running end (e.g. one
//! night of contiguous 5-minute sleep epochs). Episode bucketing attributes
//! work to these chains instead of calendar days, so a sleep period crossing
//! midnight lands in exactly one bucket. Detection is pure (no async, no
//! database); callers fetch rows and hand them here start-ordered.

use sqlx::SqlitePool;
use time::{OffsetDateTime, UtcOffset};

/// One detected episode: a gap-tolerant chain of interval observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Episode {
    /// Start of the chain's first interval.
    pub(crate) start: OffsetDateTime,
    /// Largest interval end seen in the chain.
    pub(crate) end: OffsetDateTime,
}

/// Chain start-ordered intervals into episodes.
///
/// Consecutive intervals join the same episode while
/// `next.start - current_end <= gap_seconds`; a larger gap starts a new
/// episode. The running end advances by `max`, so overlapping rows (e.g. the
/// same epochs ingested from two documents) cannot shrink the envelope.
pub(crate) fn detect_episodes(
    intervals: &[(OffsetDateTime, OffsetDateTime)],
    gap_seconds: i64,
) -> Vec<Episode> {
    let mut out: Vec<Episode> = Vec::new();
    for &(start, end) in intervals {
        match out.last_mut() {
            Some(current) if (start - current.end).whole_seconds() <= gap_seconds => {
                current.end = current.end.max(end);
            }
            _ => out.push(Episode { start, end }),
        }
    }
    out
}

/// Index of the episode containing `ts`, comparing inclusively at both
/// bounds (`start <= ts <= end`). `None` when `ts` falls between episodes.
pub(crate) fn episode_index_for(episodes: &[Episode], ts: OffsetDateTime) -> Option<usize> {
    // Episodes are start-ordered and non-overlapping (adjacent chains would
    // have merged), so the candidate is the last episode starting at or
    // before `ts`.
    let candidate = episodes.partition_point(|e| e.start <= ts).checked_sub(1)?;
    (ts <= episodes[candidate].end).then_some(candidate)
}

/// One interval observation row plus its document's confidence keys, as
/// fetched by [`fetch_all_intervals`].
pub(crate) struct IntervalRow {
    /// `effective_start`.
    pub(crate) start: OffsetDateTime,
    /// `effective_end` (rows without one are not fetched).
    pub(crate) end: OffsetDateTime,
    /// `value_quantity`, if any.
    pub(crate) value: Option<f64>,
    /// The owning document's `source`.
    pub(crate) source: String,
    /// The owning document's `document_date`.
    pub(crate) document_date: Option<String>,
}

/// Fetch every interval observation (non-null `effective_end`) of one coding
/// in the half-open `[start, end)` window, start-ordered, with NO value
/// filter — episode detection must see the whole chain, not just in-range
/// rows.
pub(crate) async fn fetch_all_intervals(
    pool: &SqlitePool,
    coding_system: &str,
    coding_code: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> Result<Vec<IntervalRow>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT o.effective_start AS "effective_start!: OffsetDateTime",
               o.effective_end AS "effective_end!: OffsetDateTime",
               o.value_quantity AS "value_quantity?: f64",
               sd.source AS "source!: String",
               sd.document_date AS "document_date?: String"
        FROM observations o
        JOIN source_documents sd ON o.source_document_id = sd.id
        WHERE o.coding_system = ?
          AND o.coding_code = ?
          AND o.effective_start >= ?
          AND o.effective_start < ?
          AND o.effective_end IS NOT NULL
        ORDER BY o.effective_start
        "#,
        coding_system,
        coding_code,
        start,
        end,
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| IntervalRow {
            start: r.effective_start,
            end: r.effective_end,
            value: r.value_quantity,
            source: r.source,
            document_date: r.document_date,
        })
        .collect())
}

/// Infallible RFC 3339 UTC bucket key (`YYYY-MM-DDTHH:MM:SSZ`).
pub(crate) fn utc_instant_key(ts: OffsetDateTime) -> String {
    let utc = ts.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        utc.year(),
        u8::from(utc.month()),
        utc.day(),
        utc.hour(),
        utc.minute(),
        utc.second()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn contiguous_intervals_form_one_episode() {
        // Two back-to-back 5-min epochs: end of first == start of second.
        let intervals = [
            (
                datetime!(2026-06-27 02:00:00 UTC),
                datetime!(2026-06-27 02:05:00 UTC),
            ),
            (
                datetime!(2026-06-27 02:05:00 UTC),
                datetime!(2026-06-27 02:10:00 UTC),
            ),
        ];
        let episodes = detect_episodes(&intervals, 0);
        assert_eq!(
            episodes,
            vec![Episode {
                start: datetime!(2026-06-27 02:00:00 UTC),
                end: datetime!(2026-06-27 02:10:00 UTC),
            }]
        );
    }

    #[test]
    fn gap_beyond_tolerance_splits_episodes() {
        // 1-second gap with zero tolerance -> two episodes.
        let intervals = [
            (
                datetime!(2026-06-27 02:00:00 UTC),
                datetime!(2026-06-27 02:05:00 UTC),
            ),
            (
                datetime!(2026-06-27 02:05:01 UTC),
                datetime!(2026-06-27 02:10:00 UTC),
            ),
        ];
        let episodes = detect_episodes(&intervals, 0);
        assert_eq!(episodes.len(), 2);
        assert_eq!(episodes[0].end, datetime!(2026-06-27 02:05:00 UTC));
        assert_eq!(episodes[1].start, datetime!(2026-06-27 02:05:01 UTC));
    }

    #[test]
    fn gap_within_tolerance_bridges() {
        // Same 1-second gap, tolerance 1 -> one episode.
        let intervals = [
            (
                datetime!(2026-06-27 02:00:00 UTC),
                datetime!(2026-06-27 02:05:00 UTC),
            ),
            (
                datetime!(2026-06-27 02:05:01 UTC),
                datetime!(2026-06-27 02:10:00 UTC),
            ),
        ];
        let episodes = detect_episodes(&intervals, 1);
        assert_eq!(episodes.len(), 1);
        assert_eq!(episodes[0].start, datetime!(2026-06-27 02:00:00 UTC));
        assert_eq!(episodes[0].end, datetime!(2026-06-27 02:10:00 UTC));
    }

    #[test]
    fn overlapping_interval_keeps_max_end() {
        // A shorter interval nested inside the first must not pull the
        // running end backwards.
        let intervals = [
            (
                datetime!(2026-06-27 02:00:00 UTC),
                datetime!(2026-06-27 03:00:00 UTC),
            ),
            (
                datetime!(2026-06-27 02:10:00 UTC),
                datetime!(2026-06-27 02:15:00 UTC),
            ),
            (
                datetime!(2026-06-27 03:00:00 UTC),
                datetime!(2026-06-27 03:05:00 UTC),
            ),
        ];
        let episodes = detect_episodes(&intervals, 0);
        assert_eq!(
            episodes,
            vec![Episode {
                start: datetime!(2026-06-27 02:00:00 UTC),
                end: datetime!(2026-06-27 03:05:00 UTC),
            }]
        );
    }

    #[test]
    fn empty_input_yields_no_episodes() {
        assert_eq!(detect_episodes(&[], 0), vec![]);
    }

    #[test]
    fn point_intervals_each_form_their_own_episode() {
        // Zero-length intervals (point observations) with gap 0.
        let intervals = [
            (
                datetime!(2026-01-01 08:00:00 UTC),
                datetime!(2026-01-01 08:00:00 UTC),
            ),
            (
                datetime!(2026-01-02 08:00:00 UTC),
                datetime!(2026-01-02 08:00:00 UTC),
            ),
        ];
        let episodes = detect_episodes(&intervals, 0);
        assert_eq!(episodes.len(), 2);
    }

    #[test]
    fn episode_index_is_inclusive_at_both_bounds() {
        let episodes = detect_episodes(
            &[
                (
                    datetime!(2026-06-27 02:00:00 UTC),
                    datetime!(2026-06-27 06:00:00 UTC),
                ),
                (
                    datetime!(2026-06-28 02:00:00 UTC),
                    datetime!(2026-06-28 06:00:00 UTC),
                ),
            ],
            0,
        );
        assert_eq!(
            episode_index_for(&episodes, datetime!(2026-06-27 02:00:00 UTC)),
            Some(0),
            "inclusive at start"
        );
        assert_eq!(
            episode_index_for(&episodes, datetime!(2026-06-27 06:00:00 UTC)),
            Some(0),
            "inclusive at end"
        );
        assert_eq!(
            episode_index_for(&episodes, datetime!(2026-06-28 04:00:00 UTC)),
            Some(1)
        );
        assert_eq!(
            episode_index_for(&episodes, datetime!(2026-06-27 12:00:00 UTC)),
            None,
            "between episodes"
        );
        assert_eq!(
            episode_index_for(&episodes, datetime!(2026-06-26 12:00:00 UTC)),
            None,
            "before all episodes"
        );
    }

    #[test]
    fn utc_instant_key_normalizes_offsets_to_z() {
        assert_eq!(
            utc_instant_key(datetime!(2026-06-27 02:00:00 UTC)),
            "2026-06-27T02:00:00Z"
        );
        // 22:00 EDT (-04:00) is 02:00Z the next day.
        assert_eq!(
            utc_instant_key(datetime!(2026-06-26 22:00:00 -4)),
            "2026-06-27T02:00:00Z"
        );
    }
}
