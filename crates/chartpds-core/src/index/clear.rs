//! Clear ingested data from the index.

use sqlx::SqlitePool;

/// Delete all rows from `source_documents`.
///
/// Foreign-key `CASCADE` rules remove the dependent `observations`,
/// `problems`, and `medications` rows automatically. Source tables
/// (`source_credentials`, `source_state`, `source_day_state`) are
/// left untouched.
///
/// Returns the number of `source_documents` rows deleted.
///
/// # Errors
///
/// Returns `sqlx::Error` if the DELETE statement fails.
pub async fn clear_ingested_data(pool: &SqlitePool) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!("DELETE FROM source_documents")
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::BlobKey;
    use crate::index::{
        insert_observation, insert_source_document, open_pool, upsert_source_credentials,
        NewObservation, NewSourceCredentials, NewSourceDocument,
    };
    use time::macros::datetime;
    use time::OffsetDateTime;

    async fn fresh_pool() -> SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    #[tokio::test]
    async fn clear_removes_source_documents_and_cascades_observations() {
        let pool = fresh_pool().await;

        let key = BlobKey::from_hex_str(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .expect("valid key");

        let doc_id = insert_source_document(
            &pool,
            NewSourceDocument {
                archive_key: &key,
                kind: "ccda",
                source: "test",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
                document_date: None,
            },
        )
        .await
        .expect("insert doc");

        insert_observation(
            &pool,
            NewObservation {
                source_document_id: doc_id,
                coding_system: "http://loinc.org",
                coding_code: "29463-7",
                coding_display: Some("Body Weight"),
                effective_start: datetime!(2026-01-01 12:00:00 UTC),
                effective_end: None,
                value_quantity: Some(72.5),
                value_string: None,
                value_unit: Some("kg"),
            },
        )
        .await
        .expect("insert obs");

        // Also insert source_credentials to verify it survives.
        upsert_source_credentials(
            &pool,
            NewSourceCredentials {
                source_name: "fitbit",
                credentials_json: r#"{"token":"abc"}"#,
                updated_at: "2026-01-15T10:00:00Z",
            },
        )
        .await
        .expect("upsert creds");

        let deleted = clear_ingested_data(&pool).await.expect("clear");
        assert_eq!(deleted, 1);

        // Verify source_documents is empty.
        let doc_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM source_documents")
            .fetch_one(&pool)
            .await
            .expect("count docs");
        assert_eq!(doc_count.0, 0);

        // Verify observations were cascade-deleted.
        let obs_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM observations")
            .fetch_one(&pool)
            .await
            .expect("count obs");
        assert_eq!(obs_count.0, 0);

        // Verify source_credentials survived.
        let creds = crate::index::get_source_credentials(&pool, "fitbit")
            .await
            .expect("get creds");
        assert!(creds.is_some());
    }

    #[tokio::test]
    async fn clear_on_empty_table_returns_zero() {
        let pool = fresh_pool().await;
        let deleted = clear_ingested_data(&pool).await.expect("clear");
        assert_eq!(deleted, 0);
    }
}
