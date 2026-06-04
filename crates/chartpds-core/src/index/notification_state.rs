//! `notification_state` table: per-condition firing state for re-fire cadence.

use sqlx::SqlitePool;

/// A row from the `notification_state` table.
#[derive(Debug, Clone)]
pub struct NotificationStateRow {
    /// Condition identifier (primary key, e.g. `"auth_expired:fitbit"`).
    pub condition_id: String,
    /// ISO-8601 timestamp of the last time this condition fired, if ever.
    pub last_fired_at: Option<String>,
    /// Current state: `"firing"` or `"resolved"`.
    pub last_state: String,
}

/// Insert or update a `notification_state` row.
///
/// On conflict all fields are replaced.
///
/// # Errors
///
/// Returns `sqlx::Error` if the upsert fails.
pub async fn upsert(
    pool: &SqlitePool,
    condition_id: &str,
    last_fired_at: Option<&str>,
    last_state: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO notification_state (condition_id, last_fired_at, last_state)
        VALUES (?, ?, ?)
        ON CONFLICT(condition_id) DO UPDATE SET
            last_fired_at = excluded.last_fired_at,
            last_state = excluded.last_state
        "#,
        condition_id,
        last_fired_at,
        last_state,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Fetch a `notification_state` row by condition id.
///
/// Returns `Ok(None)` if no row matches.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn get(
    pool: &SqlitePool,
    condition_id: &str,
) -> Result<Option<NotificationStateRow>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT condition_id AS "condition_id!: String",
               last_fired_at,
               last_state AS "last_state!: String"
        FROM notification_state
        WHERE condition_id = ?
        "#,
        condition_id,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| NotificationStateRow {
        condition_id: r.condition_id,
        last_fired_at: r.last_fired_at,
        last_state: r.last_state,
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
            "auth_expired:fitbit",
            Some("2026-01-15T10:00:00Z"),
            "firing",
        )
        .await
        .expect("first upsert");

        let row = get(&pool, "auth_expired:fitbit")
            .await
            .expect("get succeeds")
            .expect("row exists");
        assert_eq!(row.condition_id, "auth_expired:fitbit");
        assert_eq!(row.last_fired_at.as_deref(), Some("2026-01-15T10:00:00Z"));
        assert_eq!(row.last_state, "firing");

        // Second upsert replaces state.
        upsert(&pool, "auth_expired:fitbit", None, "resolved")
            .await
            .expect("second upsert");

        let row = get(&pool, "auth_expired:fitbit")
            .await
            .expect("get succeeds")
            .expect("row exists");
        assert!(row.last_fired_at.is_none());
        assert_eq!(row.last_state, "resolved");

        // Missing condition returns None.
        let missing = get(&pool, "nonexistent").await.expect("query succeeds");
        assert!(missing.is_none());
    }
}
