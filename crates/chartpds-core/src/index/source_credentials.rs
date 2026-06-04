//! `source_credentials` table: OAuth tokens and refresh tokens per source.

use sqlx::SqlitePool;

/// A row from the `source_credentials` table.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SourceCredentials {
    /// Source name (primary key, e.g. `"fitbit"`, `"oura"`).
    pub source_name: String,
    /// JSON-serialised credentials blob (tokens, refresh tokens, etc.).
    pub credentials_json: String,
    /// Monotonically increasing revision counter, bumped on every upsert.
    pub revision: i64,
    /// ISO-8601 timestamp of the last credential update.
    pub updated_at: String,
}

/// Parameters for [`upsert`].
pub struct UpsertParams<'a> {
    /// Source name (primary key).
    pub source_name: &'a str,
    /// JSON-serialised credentials blob.
    pub credentials_json: &'a str,
    /// ISO-8601 timestamp of this update.
    pub updated_at: &'a str,
}

/// Insert or update a `source_credentials` row.
///
/// On conflict the credentials and timestamp are replaced and the revision
/// counter is incremented.
///
/// # Errors
///
/// Returns `sqlx::Error` if the upsert fails.
pub async fn upsert(pool: &SqlitePool, params: UpsertParams<'_>) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO source_credentials (source_name, credentials_json, revision, updated_at)
        VALUES (?, ?, 1, ?)
        ON CONFLICT(source_name) DO UPDATE SET
            credentials_json = excluded.credentials_json,
            revision = source_credentials.revision + 1,
            updated_at = excluded.updated_at
        "#,
        params.source_name,
        params.credentials_json,
        params.updated_at,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Fetch a `source_credentials` row by source name.
///
/// Returns `Ok(None)` if no row matches.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails for any reason other than the
/// row being absent.
pub async fn get(
    pool: &SqlitePool,
    source_name: &str,
) -> Result<Option<SourceCredentials>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT source_name AS "source_name!: String",
               credentials_json,
               revision AS "revision!: i64",
               updated_at
        FROM source_credentials
        WHERE source_name = ?
        "#,
        source_name,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| SourceCredentials {
        source_name: r.source_name,
        credentials_json: r.credentials_json,
        revision: r.revision,
        updated_at: r.updated_at,
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
                credentials_json: r#"{"access_token":"abc"}"#,
                updated_at: "2026-01-15T10:00:00Z",
            },
        )
        .await
        .expect("first upsert");

        let row = get(&pool, "fitbit")
            .await
            .expect("get succeeds")
            .expect("row exists");
        assert_eq!(row.source_name, "fitbit");
        assert_eq!(row.credentials_json, r#"{"access_token":"abc"}"#);
        assert_eq!(row.revision, 1);
        assert_eq!(row.updated_at, "2026-01-15T10:00:00Z");

        // Second upsert bumps revision.
        upsert(
            &pool,
            UpsertParams {
                source_name: "fitbit",
                credentials_json: r#"{"access_token":"def"}"#,
                updated_at: "2026-01-15T11:00:00Z",
            },
        )
        .await
        .expect("second upsert");

        let row = get(&pool, "fitbit")
            .await
            .expect("get succeeds")
            .expect("row exists");
        assert_eq!(row.credentials_json, r#"{"access_token":"def"}"#);
        assert_eq!(row.revision, 2);
        assert_eq!(row.updated_at, "2026-01-15T11:00:00Z");
    }

    #[tokio::test]
    async fn get_returns_none_for_missing_source() {
        let pool = fresh_pool().await;
        let row = get(&pool, "nonexistent").await.expect("query succeeds");
        assert!(row.is_none());
    }
}
