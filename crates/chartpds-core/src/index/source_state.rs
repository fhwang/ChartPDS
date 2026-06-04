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
    /// Number of successful sync ticks since the frontier last advanced.
    pub successful_ticks_since_frontier_advance: i64,
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
    /// Successful ticks since frontier advance.
    pub successful_ticks_since_frontier_advance: i64,
    /// Consecutive sync failures.
    pub consecutive_sync_failures: i64,
}

/// Insert or update a `source_state` row.
///
/// On conflict all fields are replaced.
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
            successful_ticks_since_frontier_advance, consecutive_sync_failures
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_name) DO UPDATE SET
            last_sync_at = excluded.last_sync_at,
            last_sync_status = excluded.last_sync_status,
            last_error_message = excluded.last_error_message,
            last_error_reason = excluded.last_error_reason,
            last_synced_window_end = excluded.last_synced_window_end,
            freshness_frontier_at = excluded.freshness_frontier_at,
            successful_ticks_since_frontier_advance = excluded.successful_ticks_since_frontier_advance,
            consecutive_sync_failures = excluded.consecutive_sync_failures
        "#,
        params.source_name,
        params.last_sync_at,
        params.last_sync_status,
        params.last_error_message,
        params.last_error_reason,
        params.last_synced_window_end,
        params.freshness_frontier_at,
        params.successful_ticks_since_frontier_advance,
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
               successful_ticks_since_frontier_advance AS "successful_ticks_since_frontier_advance!: i64",
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
        successful_ticks_since_frontier_advance: r.successful_ticks_since_frontier_advance,
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
                successful_ticks_since_frontier_advance: 3,
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
        assert_eq!(row.successful_ticks_since_frontier_advance, 3);
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
                successful_ticks_since_frontier_advance: 3,
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
    async fn get_returns_none_for_missing_source() {
        let pool = fresh_pool().await;
        let row = get(&pool, "nonexistent").await.expect("query succeeds");
        assert!(row.is_none());
    }
}
