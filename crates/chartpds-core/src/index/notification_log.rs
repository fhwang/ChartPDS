//! `notification_log` table: append-only log of fired notifications.

use sqlx::SqlitePool;

/// A row from the `notification_log` table.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NotificationLogEntry {
    /// Auto-incremented row id.
    pub id: i64,
    /// Condition that fired (e.g. `"auth_expired:fitbit"`).
    pub condition_id: String,
    /// ISO-8601 timestamp when the notification was fired.
    pub fired_at: String,
    /// Severity level (`"critical"`, `"warning"`, etc.).
    pub severity: String,
    /// Short human-readable title.
    pub title: String,
    /// Longer descriptive message.
    pub message: String,
}

/// Append a notification entry to the log.
///
/// # Errors
///
/// Returns `sqlx::Error` if the insert fails.
pub async fn append(
    pool: &SqlitePool,
    condition_id: &str,
    fired_at: &str,
    severity: &str,
    title: &str,
    message: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO notification_log (condition_id, fired_at, severity, title, message)
        VALUES (?, ?, ?, ?, ?)
        "#,
        condition_id,
        fired_at,
        severity,
        title,
        message,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// List the most recent notification log entries, newest first.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn list_recent(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<NotificationLogEntry>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT id AS "id!: i64",
               condition_id,
               fired_at,
               severity,
               title,
               message
        FROM notification_log
        ORDER BY fired_at DESC
        LIMIT ?
        "#,
        limit,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| NotificationLogEntry {
            id: r.id,
            condition_id: r.condition_id,
            fired_at: r.fired_at,
            severity: r.severity,
            title: r.title,
            message: r.message,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::open_pool;

    async fn fresh_pool() -> SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    #[tokio::test]
    async fn append_then_list_round_trips() {
        let pool = fresh_pool().await;

        append(
            &pool,
            "auth_expired:fitbit",
            "2026-01-15T10:00:00Z",
            "critical",
            "ChartPDS: Fitbit re-authorization required",
            "The Fitbit adapter needs re-authorization.",
        )
        .await
        .expect("append");

        append(
            &pool,
            "sync_failures:fitbit",
            "2026-01-15T11:00:00Z",
            "warning",
            "ChartPDS: Fitbit sync failing",
            "Fitbit sync has failed 3 consecutive times.",
        )
        .await
        .expect("append");

        let entries = list_recent(&pool, 10).await.expect("list_recent");
        assert_eq!(entries.len(), 2);
        // Newest first.
        assert_eq!(entries[0].condition_id, "sync_failures:fitbit");
        assert_eq!(entries[1].condition_id, "auth_expired:fitbit");
    }
}
