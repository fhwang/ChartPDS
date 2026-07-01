//! Sync orchestration for Oura sleep data.
//!
//! Fetches recent sleep sessions from the Oura v2 API, archives raw
//! JSON, parses per-epoch sleep stages, and ingests observations into
//! the index.

use sqlx::SqlitePool;

use std::collections::HashSet;

use super::api;
use super::confidence::oura_day_confidence;
use super::storage;
use crate::archive::Archive;
use crate::index;
use crate::sources;
use crate::sources::confidence::{enumerate_dates, select_fetch_dates, DayConfidence};
use crate::sources::{Source, SyncResult};

/// Oura ring data source.
///
/// Uses a personal access token (PAT) for authentication — no OAuth
/// refresh flow needed. The token is long-lived and set via the
/// `set_oura_token` MCP tool or `OURA_PERSONAL_ACCESS_TOKEN` env var.
pub struct OuraSource {
    /// Shared HTTP client for API calls.
    pub http_client: reqwest::Client,
    /// Oura personal access token.
    pub access_token: String,
}

impl Source for OuraSource {
    fn name(&self) -> &'static str {
        "oura"
    }

    fn display_name(&self) -> &'static str {
        "Oura"
    }

    async fn sync(
        &self,
        archive: &Archive,
        pool: &SqlitePool,
        window_days: i64,
    ) -> sources::Result<SyncResult> {
        sync_recent_days(
            archive,
            pool,
            &self.http_client,
            &self.access_token,
            window_days,
        )
        .await
    }
}

/// Sync sleep data for the most recent `window_days` days.
///
/// 1. Computes the date window and selects which days still need fetching
///    (uncovered OR unsettled — see [`select_fetch_dates`]).
/// 2. If every day is already covered and settled, skips the API call.
/// 3. Fetches sleep sessions from the Oura API for the needed range.
/// 4. Ingests each session whose day is in the needed set.
/// 5. Updates `source_state` with the sync result and freshness frontier.
///
/// # Errors
///
/// Returns [`sources::Error`] if the API call fails or an unrecoverable
/// database error occurs. Individual session failures due to duplicate
/// archive keys are silently skipped.
pub async fn sync_recent_days(
    archive: &Archive,
    pool: &SqlitePool,
    http_client: &reqwest::Client,
    access_token: &str,
    window_days: i64,
) -> sources::Result<SyncResult> {
    // 1. Compute date range and select days that still need fetching.
    let (start_date, end_date) = date_window(window_days);
    let now = time::OffsetDateTime::now_utc();
    let all_dates = enumerate_dates(&start_date, &end_date);

    // A day is "covered" if we have a source_day_state row for it.
    let covered: HashSet<String> = index::list_source_day_states_by_source(pool, "oura")
        .await
        .map_err(sources::Error::Database)?
        .into_iter()
        .map(|s| s.date)
        .collect();

    // Fetch a day if it is uncovered OR unsettled. This keeps backfill
    // correct (old, never-fetched days are still pulled) while skipping old
    // days we already have.
    let needed = select_fetch_dates(
        &all_dates,
        |d| covered.contains(d),
        |d| oura_day_confidence(now, d) == DayConfidence::Confirmed,
    );
    let needed_set: HashSet<&str> = needed.iter().map(String::as_str).collect();

    // Load existing freshness frontier (and its advance timestamp) for
    // preservation.
    let source_state = index::get_source_state(pool, "oura").await.ok().flatten();
    let existing_frontier = source_state
        .as_ref()
        .and_then(|s| s.freshness_frontier_at.clone());
    let existing_advanced_at = source_state
        .as_ref()
        .and_then(|s| s.frontier_last_advanced_at.clone());

    // 2. If nothing needs fetching, skip the API call. The frontier is
    // unchanged, so its advance timestamp is preserved as-is.
    if needed.is_empty() {
        return finish_sync(
            pool,
            &end_date,
            existing_frontier.as_deref(),
            existing_frontier.as_deref(),
            existing_advanced_at.as_deref(),
            0,
            0,
        )
        .await;
    }

    // 3. Fetch the range spanning the needed days.
    // SAFETY: we checked `is_empty()` above, so both unwraps are safe.
    let fetch_start = &needed[0];
    let fetch_end = &needed[needed.len() - 1];
    let fetch_result = api::fetch_sleep(http_client, access_token, fetch_start, fetch_end).await?;

    // 4. Ingest each session whose day is in the needed set.
    let mut days_synced: i64 = 0;
    let mut total_samples: i64 = 0;
    let mut max_bedtime_end: Option<String> = None;

    for session in &fetch_result.sessions {
        // Skip sessions for days we don't need (already covered + settled).
        if !needed_set.contains(session.day.as_str()) {
            continue;
        }

        #[allow(
            clippy::cast_possible_wrap,
            reason = "epoch count is always small (< 200)"
        )]
        let epoch_count = session.sleep_phase_5_min.len() as i64;

        let session_raw = serde_json::to_value(session).map_err(|err| sources::Error::Parse {
            reason: format!("serializing oura session: {err}"),
        })?;

        match storage::ingest_session(archive, pool, session, &session_raw).await {
            Ok(_) => {
                days_synced += 1;
                total_samples += epoch_count;
                // Track the latest bedtime_end for freshness frontier.
                if max_bedtime_end
                    .as_ref()
                    .is_none_or(|m| &session.bedtime_end > m)
                {
                    max_bedtime_end = Some(session.bedtime_end.clone());
                }
            }
            Err(sources::Error::Database(ref db_err)) if is_unique_violation(db_err) => {
                // Session already ingested — skip silently.
            }
            Err(err) => return Err(err),
        }
    }

    // 5. Update source_state with freshness frontier.
    let new_frontier = max_bedtime_end.or_else(|| existing_frontier.clone());
    finish_sync(
        pool,
        &end_date,
        new_frontier.as_deref(),
        existing_frontier.as_deref(),
        existing_advanced_at.as_deref(),
        days_synced,
        total_samples,
    )
    .await
}

/// Record the sync result in `source_state`.
///
/// `previous_frontier` and `previous_advanced_at` are the values already stored;
/// `frontier_last_advanced_at` is re-stamped to now only when the frontier value
/// changed (see [`sources::confidence::frontier_advanced_at`]).
async fn finish_sync(
    pool: &SqlitePool,
    end_date: &str,
    freshness_frontier: Option<&str>,
    previous_frontier: Option<&str>,
    previous_advanced_at: Option<&str>,
    days_synced: i64,
    total_samples: i64,
) -> sources::Result<SyncResult> {
    let sync_at = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    let frontier_last_advanced_at = sources::confidence::frontier_advanced_at(
        previous_frontier,
        freshness_frontier,
        previous_advanced_at,
        &sync_at,
    );
    index::upsert_source_state(
        pool,
        index::UpsertSourceStateParams {
            source_name: "oura",
            last_sync_at: Some(&sync_at),
            last_sync_status: Some("ok"),
            last_error_message: None,
            last_error_reason: None,
            last_synced_window_end: Some(end_date),
            freshness_frontier_at: freshness_frontier,
            frontier_last_advanced_at: frontier_last_advanced_at.as_deref(),
            consecutive_sync_failures: 0,
        },
    )
    .await
    .map_err(sources::Error::Database)?;

    Ok(SyncResult {
        days_synced,
        total_samples,
    })
}

/// Compute start and end dates for a sync window.
///
/// Returns `(start_date, end_date)` as `YYYY-MM-DD` strings, where
/// `end_date` is today and `start_date` is `window_days` days before.
fn date_window(window_days: i64) -> (String, String) {
    let today = time::OffsetDateTime::now_utc().date();
    let format = time::macros::format_description!("[year]-[month]-[day]");
    let end_date = today.format(&format).unwrap_or_default();
    let start = today
        .checked_sub(time::Duration::days(window_days))
        .unwrap_or(today);
    let start_date = start.format(&format).unwrap_or_default();
    (start_date, end_date)
}

/// Check whether a `sqlx::Error` is a UNIQUE constraint violation.
fn is_unique_violation(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err.message().contains("UNIQUE constraint failed"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_window_produces_valid_dates() {
        let (start, end) = date_window(7);
        assert_eq!(start.len(), 10);
        assert_eq!(end.len(), 10);
        assert_eq!(&start[4..5], "-");
        assert_eq!(&end[4..5], "-");
        // Start should be before end.
        assert!(start <= end);
    }

    #[test]
    fn date_window_zero_gives_same_day() {
        let (start, end) = date_window(0);
        assert_eq!(start, end);
    }

    /// Compile-time check that `OuraSource` implements `Source`.
    const _: () = {
        fn assert_source<T: Source>() {}
        fn check() {
            assert_source::<OuraSource>();
        }
        let _ = check;
    };
}
