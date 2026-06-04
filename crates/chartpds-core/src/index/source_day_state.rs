//! `source_day_state` table: per-source per-day ingestion bookkeeping.

use sqlx::SqlitePool;

/// A row from the `source_day_state` table.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SourceDayState {
    /// Source name (e.g. `"fitbit"`, `"oura"`).
    pub source_name: String,
    /// ISO-8601 date string (e.g. `"2026-01-15"`).
    pub date: String,
    /// Number of samples ingested for this source on this date.
    pub samples_count: i64,
    /// Previous samples count (before the latest pull), used for change detection.
    pub samples_count_prev: Option<i64>,
    /// ISO-8601 timestamp of the last pull for this source-day.
    pub last_pulled_at: String,
}

/// Parameters for [`upsert`].
pub struct UpsertParams<'a> {
    /// Source name.
    pub source_name: &'a str,
    /// ISO-8601 date string.
    pub date: &'a str,
    /// Number of samples ingested.
    pub samples_count: i64,
    /// Previous samples count, if known.
    pub samples_count_prev: Option<i64>,
    /// ISO-8601 timestamp of this pull.
    pub last_pulled_at: &'a str,
}

/// Insert or update a `source_day_state` row.
///
/// On conflict all fields are replaced.
///
/// # Errors
///
/// Returns `sqlx::Error` if the upsert fails.
pub async fn upsert(pool: &SqlitePool, params: UpsertParams<'_>) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO source_day_state (source_name, date, samples_count, samples_count_prev, last_pulled_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(source_name, date) DO UPDATE SET
            samples_count = excluded.samples_count,
            samples_count_prev = excluded.samples_count_prev,
            last_pulled_at = excluded.last_pulled_at
        "#,
        params.source_name,
        params.date,
        params.samples_count,
        params.samples_count_prev,
        params.last_pulled_at,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Fetch a `source_day_state` row by source name and date.
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
    date: &str,
) -> Result<Option<SourceDayState>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT source_name AS "source_name!: String",
               date AS "date!: String",
               samples_count AS "samples_count!: i64",
               samples_count_prev,
               last_pulled_at
        FROM source_day_state
        WHERE source_name = ? AND date = ?
        "#,
        source_name,
        date,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| SourceDayState {
        source_name: r.source_name,
        date: r.date,
        samples_count: r.samples_count,
        samples_count_prev: r.samples_count_prev,
        last_pulled_at: r.last_pulled_at,
    }))
}

/// List all `source_day_state` rows for a given source, ordered by date.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn list_by_source(
    pool: &SqlitePool,
    source_name: &str,
) -> Result<Vec<SourceDayState>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT source_name AS "source_name!: String",
               date AS "date!: String",
               samples_count AS "samples_count!: i64",
               samples_count_prev,
               last_pulled_at
        FROM source_day_state
        WHERE source_name = ?
        ORDER BY date
        "#,
        source_name,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| SourceDayState {
            source_name: r.source_name,
            date: r.date,
            samples_count: r.samples_count,
            samples_count_prev: r.samples_count_prev,
            last_pulled_at: r.last_pulled_at,
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
        // Leak the temp dir so the file lives as long as the pool.
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    #[tokio::test]
    async fn upsert_get_and_list_round_trip() {
        let pool = fresh_pool().await;

        upsert(
            &pool,
            UpsertParams {
                source_name: "fitbit",
                date: "2026-01-15",
                samples_count: 42,
                samples_count_prev: None,
                last_pulled_at: "2026-01-15T10:00:00Z",
            },
        )
        .await
        .expect("first upsert");

        upsert(
            &pool,
            UpsertParams {
                source_name: "fitbit",
                date: "2026-01-16",
                samples_count: 50,
                samples_count_prev: Some(48),
                last_pulled_at: "2026-01-16T10:00:00Z",
            },
        )
        .await
        .expect("second upsert");

        // get returns a single row.
        let row = get(&pool, "fitbit", "2026-01-15")
            .await
            .expect("get succeeds")
            .expect("row exists");
        assert_eq!(row.source_name, "fitbit");
        assert_eq!(row.date, "2026-01-15");
        assert_eq!(row.samples_count, 42);
        assert!(row.samples_count_prev.is_none());

        // list_by_source returns all rows for the source, ordered by date.
        let rows = list_by_source(&pool, "fitbit").await.expect("list");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].date, "2026-01-15");
        assert_eq!(rows[1].date, "2026-01-16");
        assert_eq!(rows[1].samples_count_prev, Some(48));

        // Upserting the same key replaces the row.
        upsert(
            &pool,
            UpsertParams {
                source_name: "fitbit",
                date: "2026-01-15",
                samples_count: 45,
                samples_count_prev: Some(42),
                last_pulled_at: "2026-01-15T12:00:00Z",
            },
        )
        .await
        .expect("upsert update");

        let row = get(&pool, "fitbit", "2026-01-15")
            .await
            .expect("get succeeds")
            .expect("row exists");
        assert_eq!(row.samples_count, 45);
        assert_eq!(row.samples_count_prev, Some(42));
    }

    #[tokio::test]
    async fn get_returns_none_for_missing_row() {
        let pool = fresh_pool().await;
        let row = get(&pool, "fitbit", "2026-01-15")
            .await
            .expect("query succeeds");
        assert!(row.is_none());
    }
}
