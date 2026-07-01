//! Resolve per-`(source, replay-day)` data confidence from the index.
//!
//! This is the single place that reads `source_state` / `source_day_state`
//! and dispatches to the pure per-adapter confidence functions. It has no
//! wall clock of its own — `now` is always injected so callers (and tests)
//! stay deterministic. The day key is the source's replay day
//! (`source_documents.document_date`), never the observation timestamp.

use std::collections::HashMap;

use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::index::{get_source_day_state, get_source_state};
use crate::sources::fitbit::confidence::fitbit_day_confidence;
use crate::sources::oura::confidence::oura_day_confidence;
use crate::sources::DayConfidence;

/// Format an `OffsetDateTime`'s calendar date as `YYYY-MM-DD`.
fn ymd(dt: OffsetDateTime) -> String {
    format!(
        "{:04}-{:02}-{:02}",
        dt.year(),
        u8::from(dt.month()),
        dt.day()
    )
}

/// Resolve confidence for a set of `(source, replay-day)` keys.
///
/// Dispatches per source: `fitbit` uses the stability-based rule (frontier +
/// day-state), `oura` uses the time-based rule, and any other source is
/// `Confirmed` by policy (a finalized clinical document does not accrete
/// data). Keys for sources with no meaningful confidence model should simply
/// not be passed; if they are, they resolve to `Confirmed`.
///
/// # Errors
///
/// Returns `sqlx::Error` if reading `source_state` / `source_day_state` fails.
pub async fn resolve_source_day_confidence(
    pool: &SqlitePool,
    now: OffsetDateTime,
    keys: &[(String, String)],
) -> Result<HashMap<(String, String), DayConfidence>, sqlx::Error> {
    let today = ymd(now);
    let mut frontier_cache: HashMap<String, Option<String>> = HashMap::new();
    let mut out = HashMap::new();

    for (source, date) in keys {
        let confidence = match source.as_str() {
            "fitbit" => {
                if !frontier_cache.contains_key(source) {
                    let frontier = get_source_state(pool, source)
                        .await?
                        .and_then(|s| s.freshness_frontier_at);
                    frontier_cache.insert(source.clone(), frontier);
                }
                let frontier = frontier_cache
                    .get(source)
                    .and_then(std::option::Option::as_deref);
                let day_state = get_source_day_state(pool, source, date).await?;
                fitbit_day_confidence(&today, date, frontier, day_state.as_ref())
            }
            "oura" => oura_day_confidence(now, date),
            _ => DayConfidence::Confirmed,
        };
        out.insert((source.clone(), date.clone()), confidence);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{
        open_pool, upsert_source_day_state, upsert_source_state, UpsertSourceDayStateParams,
        UpsertSourceStateParams,
    };
    use time::macros::datetime;

    async fn pool() -> SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    async fn set_frontier(pool: &SqlitePool, source: &str, frontier: &str) {
        upsert_source_state(
            pool,
            UpsertSourceStateParams {
                source_name: source,
                last_sync_at: None,
                last_sync_status: None,
                last_error_message: None,
                last_error_reason: None,
                last_synced_window_end: None,
                freshness_frontier_at: Some(frontier),
                frontier_last_advanced_at: None,
                consecutive_sync_failures: 0,
            },
        )
        .await
        .expect("upsert source_state");
    }

    async fn set_day_state(
        pool: &SqlitePool,
        source: &str,
        date: &str,
        count: i64,
        prev: Option<i64>,
    ) {
        upsert_source_day_state(
            pool,
            UpsertSourceDayStateParams {
                source_name: source,
                date,
                samples_count: count,
                samples_count_prev: prev,
                last_pulled_at: "2026-01-11T00:00:00Z",
            },
        )
        .await
        .expect("upsert source_day_state");
    }

    #[tokio::test]
    async fn fitbit_old_stable_frontier_past_is_confirmed() {
        let pool = pool().await;
        set_frontier(&pool, "fitbit", "2026-01-12T12:00:00Z").await;
        set_day_state(&pool, "fitbit", "2026-01-10", 100, Some(100)).await;
        let keys = [("fitbit".to_owned(), "2026-01-10".to_owned())];
        let out = resolve_source_day_confidence(&pool, datetime!(2026-01-20 00:00:00 UTC), &keys)
            .await
            .expect("resolve");
        assert_eq!(
            out[&("fitbit".to_owned(), "2026-01-10".to_owned())],
            DayConfidence::Confirmed
        );
    }

    #[tokio::test]
    async fn fitbit_no_frontier_is_provisional() {
        let pool = pool().await;
        set_day_state(&pool, "fitbit", "2026-01-10", 100, Some(100)).await;
        let keys = [("fitbit".to_owned(), "2026-01-10".to_owned())];
        let out = resolve_source_day_confidence(&pool, datetime!(2026-01-20 00:00:00 UTC), &keys)
            .await
            .expect("resolve");
        assert_eq!(
            out[&("fitbit".to_owned(), "2026-01-10".to_owned())],
            DayConfidence::Provisional
        );
    }

    #[tokio::test]
    async fn oura_old_day_is_confirmed_recent_is_provisional() {
        let pool = pool().await;
        let keys = [
            ("oura".to_owned(), "2026-01-10".to_owned()),
            ("oura".to_owned(), "2026-01-20".to_owned()),
        ];
        let out = resolve_source_day_confidence(&pool, datetime!(2026-01-20 12:00:00 UTC), &keys)
            .await
            .expect("resolve");
        assert_eq!(
            out[&("oura".to_owned(), "2026-01-10".to_owned())],
            DayConfidence::Confirmed
        );
        assert_eq!(
            out[&("oura".to_owned(), "2026-01-20".to_owned())],
            DayConfidence::Provisional
        );
    }

    #[tokio::test]
    async fn unknown_source_is_confirmed_by_policy() {
        let pool = pool().await;
        let keys = [("epic".to_owned(), "2026-01-10".to_owned())];
        let out = resolve_source_day_confidence(&pool, datetime!(2026-01-20 00:00:00 UTC), &keys)
            .await
            .expect("resolve");
        assert_eq!(
            out[&("epic".to_owned(), "2026-01-10".to_owned())],
            DayConfidence::Confirmed
        );
    }
}
