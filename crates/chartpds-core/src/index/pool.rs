//! Connection pool helpers.
//!
//! Wraps `sqlx::SqlitePool` with the project-standard runtime configuration:
//! WAL journal mode (concurrent reads + serialized writes), 5-second busy
//! timeout (retries on lock contention), and automatic migration application
//! at open time.

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::str::FromStr;
use thiserror::Error;

use crate::index::run_migrations;

/// Open a `SqlitePool` against the given database URL with `ChartPDS`'s
/// standard configuration applied (WAL mode, 5s busy timeout, migrations
/// applied).
///
/// `database_url` should be a `sqlite://...` URL. To create the file if it
/// does not exist, include `?mode=rwc`.
///
/// # Errors
///
/// Returns [`OpenError`] if connecting, applying PRAGMAs, or running
/// migrations fails.
pub async fn open_pool(database_url: &str) -> Result<SqlitePool, OpenError> {
    let options = SqliteConnectOptions::from_str(database_url)
        .map_err(OpenError::ConnectOptions)?
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .busy_timeout(std::time::Duration::from_secs(5))
        .foreign_keys(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await
        .map_err(OpenError::Connect)?;

    run_migrations(&pool).await.map_err(OpenError::Migrate)?;

    Ok(pool)
}

/// Errors returned by [`open_pool`].
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OpenError {
    /// Failed to parse the database URL into connect options.
    #[error("invalid database URL")]
    ConnectOptions(#[source] sqlx::Error),

    /// Failed to open the connection pool.
    #[error("could not connect to database")]
    Connect(#[source] sqlx::Error),

    /// Failed to apply migrations.
    #[error("migration failed")]
    Migrate(#[source] sqlx::migrate::MigrateError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn open_pool_runs_migrations_and_returns_a_working_pool() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());

        let pool = open_pool(&url).await.expect("open pool");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .expect("query migrations table");
        assert!(count >= 1, "expected at least one applied migration");
    }

    #[tokio::test]
    async fn open_pool_sets_wal_journal_mode() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());

        let pool = open_pool(&url).await.expect("open pool");
        let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&pool)
            .await
            .expect("query journal_mode");
        assert_eq!(journal_mode.to_lowercase(), "wal");
    }

    #[tokio::test]
    async fn open_pool_sets_busy_timeout() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());

        let pool = open_pool(&url).await.expect("open pool");
        let timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
            .fetch_one(&pool)
            .await
            .expect("query busy_timeout");
        assert_eq!(timeout, 5000);
    }

    #[tokio::test]
    async fn open_pool_enables_foreign_keys() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());

        let pool = open_pool(&url).await.expect("open pool");
        let fk: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
            .fetch_one(&pool)
            .await
            .expect("query foreign_keys");
        assert_eq!(fk, 1);
    }
}
