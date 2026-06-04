//! List recent notification log entries.

use sqlx::SqlitePool;

use crate::index::NotificationLogEntry;

/// Fetch the most recent notification log entries, newest first.
///
/// Returns an empty `Vec` when the `notification_log` table is empty.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn list_recent_notifications(
    pool: &SqlitePool,
    limit: i64,
) -> Result<Vec<NotificationLogEntry>, sqlx::Error> {
    crate::index::list_recent_notification_log(pool, limit).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{append_notification_log, open_pool};

    #[tokio::test]
    async fn returns_seeded_entries_newest_first() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");

        append_notification_log(
            &pool,
            "auth_expired:fitbit",
            "2026-01-15T10:00:00Z",
            "critical",
            "ChartPDS: Fitbit re-authorization required",
            "The Fitbit adapter needs re-authorization.",
        )
        .await
        .expect("append");

        append_notification_log(
            &pool,
            "sync_failures:fitbit",
            "2026-01-15T11:00:00Z",
            "warning",
            "ChartPDS: Fitbit sync failing",
            "Fitbit sync has failed 3 consecutive times.",
        )
        .await
        .expect("append");

        let entries = list_recent_notifications(&pool, 10).await.expect("list");
        assert_eq!(entries.len(), 2);
        // Newest first.
        assert_eq!(entries[0].condition_id, "sync_failures:fitbit");
        assert_eq!(entries[1].condition_id, "auth_expired:fitbit");
    }
}
