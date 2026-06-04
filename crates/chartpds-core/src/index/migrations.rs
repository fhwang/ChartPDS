//! Compile-time-embedded migration runner.
//!
//! Migrations live in `crates/chartpds-core/migrations/*.sql` and are
//! embedded into the binary via `sqlx::migrate!()`. Call [`run_migrations`]
//! after opening a pool to apply any pending migrations.

use sqlx::SqlitePool;

/// Apply all pending migrations to the given pool.
///
/// # Errors
///
/// Returns `sqlx::migrate::MigrateError` if any migration fails to apply
/// (typically a syntax error or constraint violation in a new migration).
pub async fn run_migrations(pool: &SqlitePool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("./migrations").run(pool).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    #[tokio::test]
    async fn migrations_apply_cleanly_to_fresh_in_memory_db() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("connect to in-memory db");

        run_migrations(&pool).await.expect("migrations apply");
    }
}
