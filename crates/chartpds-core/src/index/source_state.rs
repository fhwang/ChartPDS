//! `source_state` table: sync cursors and status per source.

use sqlx::SqlitePool;

/// A row from the `source_state` table.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SourceState {
    /// Source name (primary key, e.g. `"fitbit"`, `"oura"`).
    pub source_name: String,
    /// ISO-8601 timestamp of the last sync attempt.
    pub last_sync_at: Option<String>,
    /// Status of the last sync (e.g. `"ok"`, `"error"`).
    pub last_sync_status: Option<String>,
    /// Human-readable error message from the last failed sync, if any.
    pub last_error_message: Option<String>,
    /// Machine-readable reason code for the last failure, if any.
    pub last_error_reason: Option<String>,
    /// ISO-8601 date of the end of the last synced window.
    pub last_synced_window_end: Option<String>,
    /// ISO-8601 timestamp of the freshness frontier.
    pub freshness_frontier_at: Option<String>,
    /// ISO-8601 wall-clock timestamp of when the frontier last advanced
    /// (i.e. changed value). `None` until the frontier is first set.
    pub frontier_last_advanced_at: Option<String>,
    /// Number of consecutive sync failures (resets on success).
    pub consecutive_sync_failures: i64,
}

/// Parameters for [`upsert`].
pub struct UpsertParams<'a> {
    /// Source name (primary key).
    pub source_name: &'a str,
    /// ISO-8601 timestamp of this sync attempt.
    pub last_sync_at: Option<&'a str>,
    /// Status of this sync.
    pub last_sync_status: Option<&'a str>,
    /// Error message, if this sync failed.
    pub last_error_message: Option<&'a str>,
    /// Machine-readable error reason, if this sync failed.
    pub last_error_reason: Option<&'a str>,
    /// End of the synced window.
    pub last_synced_window_end: Option<&'a str>,
    /// Freshness frontier timestamp.
    pub freshness_frontier_at: Option<&'a str>,
    /// Wall-clock timestamp of when the frontier last advanced.
    pub frontier_last_advanced_at: Option<&'a str>,
    /// Consecutive sync failures.
    pub consecutive_sync_failures: i64,
}

/// Insert or update a `source_state` row.
///
/// On conflict all fields are replaced. This is the adapter-owned write path:
/// it is the sole writer of the frontier fields (`freshness_frontier_at`,
/// `frontier_last_advanced_at`) and `last_synced_window_end`. The daemon tick
/// uses [`upsert_sync_status`] instead, which preserves those fields.
///
/// # Errors
///
/// Returns `sqlx::Error` if the upsert fails.
pub async fn upsert(pool: &SqlitePool, params: UpsertParams<'_>) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO source_state (
            source_name, last_sync_at, last_sync_status,
            last_error_message, last_error_reason,
            last_synced_window_end, freshness_frontier_at,
            frontier_last_advanced_at, consecutive_sync_failures
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_name) DO UPDATE SET
            last_sync_at = excluded.last_sync_at,
            last_sync_status = excluded.last_sync_status,
            last_error_message = excluded.last_error_message,
            last_error_reason = excluded.last_error_reason,
            last_synced_window_end = excluded.last_synced_window_end,
            freshness_frontier_at = excluded.freshness_frontier_at,
            frontier_last_advanced_at = excluded.frontier_last_advanced_at,
            consecutive_sync_failures = excluded.consecutive_sync_failures
        "#,
        params.source_name,
        params.last_sync_at,
        params.last_sync_status,
        params.last_error_message,
        params.last_error_reason,
        params.last_synced_window_end,
        params.freshness_frontier_at,
        params.frontier_last_advanced_at,
        params.consecutive_sync_failures,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Parameters for [`upsert_sync_status`].
pub struct UpsertSyncStatusParams<'a> {
    /// Source name (primary key).
    pub source_name: &'a str,
    /// ISO-8601 timestamp of this sync attempt.
    pub last_sync_at: Option<&'a str>,
    /// Status of this sync (e.g. `"success"`, `"error"`).
    pub last_sync_status: Option<&'a str>,
    /// Error message, if this sync failed.
    pub last_error_message: Option<&'a str>,
    /// Machine-readable error reason, if this sync failed.
    pub last_error_reason: Option<&'a str>,
    /// Consecutive sync failures.
    pub consecutive_sync_failures: i64,
}

/// Record only the sync-status fields, preserving adapter-owned state.
///
/// This is the daemon tick's write path. Unlike [`upsert`], it never touches
/// `freshness_frontier_at`, `frontier_last_advanced_at`, or
/// `last_synced_window_end` — those are owned by the adapter's `sync()` and
/// would otherwise be clobbered back to `NULL` by the post-sync status write.
/// On insert (no prior row) those columns take their table defaults.
///
/// # Errors
///
/// Returns `sqlx::Error` if the upsert fails.
pub async fn upsert_sync_status(
    pool: &SqlitePool,
    params: UpsertSyncStatusParams<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO source_state (
            source_name, last_sync_at, last_sync_status,
            last_error_message, last_error_reason, consecutive_sync_failures
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_name) DO UPDATE SET
            last_sync_at = excluded.last_sync_at,
            last_sync_status = excluded.last_sync_status,
            last_error_message = excluded.last_error_message,
            last_error_reason = excluded.last_error_reason,
            consecutive_sync_failures = excluded.consecutive_sync_failures
        "#,
        params.source_name,
        params.last_sync_at,
        params.last_sync_status,
        params.last_error_message,
        params.last_error_reason,
        params.consecutive_sync_failures,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Fetch a `source_state` row by source name.
///
/// Returns `Ok(None)` if no row matches.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails for any reason other than the
/// row being absent.
pub async fn get(pool: &SqlitePool, source_name: &str) -> Result<Option<SourceState>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT source_name AS "source_name!: String",
               last_sync_at,
               last_sync_status,
               last_error_message,
               last_error_reason,
               last_synced_window_end,
               freshness_frontier_at,
               frontier_last_advanced_at,
               consecutive_sync_failures AS "consecutive_sync_failures!: i64"
        FROM source_state
        WHERE source_name = ?
        "#,
        source_name,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| SourceState {
        source_name: r.source_name,
        last_sync_at: r.last_sync_at,
        last_sync_status: r.last_sync_status,
        last_error_message: r.last_error_message,
        last_error_reason: r.last_error_reason,
        last_synced_window_end: r.last_synced_window_end,
        freshness_frontier_at: r.freshness_frontier_at,
        frontier_last_advanced_at: r.frontier_last_advanced_at,
        consecutive_sync_failures: r.consecutive_sync_failures,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::open_pool;

    async fn fresh_pool() -> SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        // Leak the temp dir so the file lives as long as the pool.
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    #[tokio::test]
    async fn upsert_then_get_round_trips() {
        let pool = fresh_pool().await;

        upsert(
            &pool,
            UpsertParams {
                source_name: "fitbit",
                last_sync_at: Some("2026-01-15T10:00:00Z"),
                last_sync_status: Some("ok"),
                last_error_message: None,
                last_error_reason: None,
                last_synced_window_end: Some("2026-01-15"),
                freshness_frontier_at: Some("2026-01-14T00:00:00Z"),
                frontier_last_advanced_at: Some("2026-01-15T10:00:00Z"),
                consecutive_sync_failures: 0,
            },
        )
        .await
        .expect("first upsert");

        let row = get(&pool, "fitbit")
            .await
            .expect("get succeeds")
            .expect("row exists");
        assert_eq!(row.source_name, "fitbit");
        assert_eq!(row.last_sync_at.as_deref(), Some("2026-01-15T10:00:00Z"));
        assert_eq!(row.last_sync_status.as_deref(), Some("ok"));
        assert!(row.last_error_message.is_none());
        assert_eq!(
            row.frontier_last_advanced_at.as_deref(),
            Some("2026-01-15T10:00:00Z")
        );
        assert_eq!(row.consecutive_sync_failures, 0);

        // Second upsert replaces all fields.
        upsert(
            &pool,
            UpsertParams {
                source_name: "fitbit",
                last_sync_at: Some("2026-01-15T11:00:00Z"),
                last_sync_status: Some("error"),
                last_error_message: Some("rate limited"),
                last_error_reason: Some("rate_limit"),
                last_synced_window_end: Some("2026-01-15"),
                freshness_frontier_at: Some("2026-01-14T00:00:00Z"),
                frontier_last_advanced_at: Some("2026-01-15T10:00:00Z"),
                consecutive_sync_failures: 1,
            },
        )
        .await
        .expect("second upsert");

        let row = get(&pool, "fitbit")
            .await
            .expect("get succeeds")
            .expect("row exists");
        assert_eq!(row.last_sync_status.as_deref(), Some("error"));
        assert_eq!(row.last_error_message.as_deref(), Some("rate limited"));
        assert_eq!(row.consecutive_sync_failures, 1);
    }

    #[tokio::test]
    async fn upsert_sync_status_preserves_frontier_fields() {
        let pool = fresh_pool().await;

        // Adapter writes the full row, including frontier state.
        upsert(
            &pool,
            UpsertParams {
                source_name: "fitbit",
                last_sync_at: Some("2026-01-15T10:00:00Z"),
                last_sync_status: Some("ok"),
                last_error_message: None,
                last_error_reason: None,
                last_synced_window_end: Some("2026-01-15"),
                freshness_frontier_at: Some("2026-01-14T00:00:00Z"),
                frontier_last_advanced_at: Some("2026-01-15T10:00:00Z"),
                consecutive_sync_failures: 0,
            },
        )
        .await
        .expect("adapter upsert");

        // Status-only write must leave the frontier/window fields untouched.
        upsert_sync_status(
            &pool,
            UpsertSyncStatusParams {
                source_name: "fitbit",
                last_sync_at: Some("2026-01-15T10:05:00Z"),
                last_sync_status: Some("success"),
                last_error_message: None,
                last_error_reason: None,
                consecutive_sync_failures: 0,
            },
        )
        .await
        .expect("status upsert");

        let row = get(&pool, "fitbit")
            .await
            .expect("get succeeds")
            .expect("row exists");
        assert_eq!(row.last_sync_status.as_deref(), Some("success"));
        assert_eq!(row.last_sync_at.as_deref(), Some("2026-01-15T10:05:00Z"));
        assert_eq!(
            row.freshness_frontier_at.as_deref(),
            Some("2026-01-14T00:00:00Z")
        );
        assert_eq!(
            row.frontier_last_advanced_at.as_deref(),
            Some("2026-01-15T10:00:00Z")
        );
        assert_eq!(row.last_synced_window_end.as_deref(), Some("2026-01-15"));
    }

    #[tokio::test]
    async fn upsert_sync_status_inserts_when_no_row_exists() {
        let pool = fresh_pool().await;

        // No prior adapter write (e.g. sync failed before writing state).
        upsert_sync_status(
            &pool,
            UpsertSyncStatusParams {
                source_name: "fitbit",
                last_sync_at: Some("2026-01-15T10:00:00Z"),
                last_sync_status: Some("error"),
                last_error_message: Some("boom"),
                last_error_reason: Some("transient"),
                consecutive_sync_failures: 1,
            },
        )
        .await
        .expect("status upsert inserts");

        let row = get(&pool, "fitbit")
            .await
            .expect("get succeeds")
            .expect("row exists");
        assert_eq!(row.last_sync_status.as_deref(), Some("error"));
        assert_eq!(row.consecutive_sync_failures, 1);
        assert!(row.freshness_frontier_at.is_none());
        assert!(row.frontier_last_advanced_at.is_none());
    }

    #[tokio::test]
    async fn get_returns_none_for_missing_source() {
        let pool = fresh_pool().await;
        let row = get(&pool, "nonexistent").await.expect("query succeeds");
        assert!(row.is_none());
    }
}
