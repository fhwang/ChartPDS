//! `narrative_texts` table: extracted plain text of narrative documents.
//!
//! One row per narrative `source_documents` row. A parallel FTS5 table
//! (`narrative_texts_fts`) is kept in sync by SQL triggers declared in the
//! migration — inserts, updates, and cascade deletes all propagate without
//! write-path code here.

use sqlx::SqlitePool;

/// A row from the `narrative_texts` table.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NarrativeText {
    /// Foreign key into `source_documents` (also the primary key here).
    pub source_document_id: i64,
    /// Short human-readable label from the extraction artifact, if any.
    pub title: Option<String>,
    /// Full extracted document text.
    pub text: String,
}

/// Parameters for [`upsert`].
pub struct UpsertParams<'a> {
    /// Foreign key into `source_documents`.
    pub source_document_id: i64,
    /// Optional title.
    pub title: Option<&'a str>,
    /// Full extracted text.
    pub text: &'a str,
}

/// Insert or replace the narrative text for a source document.
///
/// # Errors
///
/// Returns `sqlx::Error` if the statement fails (typically a foreign-key
/// violation on `source_document_id`).
pub async fn upsert(pool: &SqlitePool, params: UpsertParams<'_>) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO narrative_texts (source_document_id, title, text)
        VALUES (?, ?, ?)
        ON CONFLICT(source_document_id) DO UPDATE SET
            title = excluded.title,
            text = excluded.text
        "#,
        params.source_document_id,
        params.title,
        params.text,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Fetch the narrative text for a source document, if present.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn get_by_source_document(
    pool: &SqlitePool,
    source_document_id: i64,
) -> Result<Option<NarrativeText>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT source_document_id AS "source_document_id!: i64", title, text
        FROM narrative_texts
        WHERE source_document_id = ?
        "#,
        source_document_id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| NarrativeText {
        source_document_id: r.source_document_id,
        title: r.title,
        text: r.text,
    }))
}

/// Set the title on an existing narrative text row.
///
/// # Errors
///
/// Returns `sqlx::Error` if the statement fails.
pub async fn set_title(
    pool: &SqlitePool,
    source_document_id: i64,
    title: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        "UPDATE narrative_texts SET title = ? WHERE source_document_id = ?",
        title,
        source_document_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::BlobKey;
    use crate::index::{insert_source_document, open_pool, InsertSourceDocumentParams};
    use time::OffsetDateTime;

    async fn fresh_pool() -> SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    async fn doc(pool: &SqlitePool, hex: &str) -> i64 {
        let key = BlobKey::from_hex_str(hex).expect("key");
        insert_source_document(
            pool,
            InsertSourceDocumentParams {
                archive_key: &key,
                kind: "clinical-pdf",
                source: "test",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
                document_date: None,
            },
        )
        .await
        .expect("doc")
    }

    async fn fts_match_count(pool: &SqlitePool, query: &str) -> i64 {
        let row: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM narrative_texts_fts WHERE narrative_texts_fts MATCH ?",
        )
        .bind(query)
        .fetch_one(pool)
        .await
        .expect("fts query");
        row.0
    }

    #[tokio::test]
    async fn upsert_and_get_round_trips() {
        let pool = fresh_pool().await;
        let id = doc(
            &pool,
            "1111111111111111111111111111111111111111111111111111111111111111",
        )
        .await;
        upsert(
            &pool,
            UpsertParams {
                source_document_id: id,
                title: None,
                text: "RECTAL MUCOSA SHOWING NO SIGNIFICANT FINDINGS",
            },
        )
        .await
        .expect("upsert");

        let row = get_by_source_document(&pool, id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(row.title, None);
        assert!(row.text.contains("RECTAL MUCOSA"));

        set_title(&pool, id, "GI Pathology Report")
            .await
            .expect("set title");
        let row = get_by_source_document(&pool, id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(row.title.as_deref(), Some("GI Pathology Report"));
    }

    #[tokio::test]
    async fn fts_index_matches_inserted_text() {
        let pool = fresh_pool().await;
        let id = doc(
            &pool,
            "2222222222222222222222222222222222222222222222222222222222222222",
        )
        .await;
        upsert(
            &pool,
            UpsertParams {
                source_document_id: id,
                title: None,
                text: "BIOPSY TAKEN TO RULE OUT PROCTITIS",
            },
        )
        .await
        .expect("upsert");

        assert_eq!(fts_match_count(&pool, "proctitis").await, 1);
        assert_eq!(fts_match_count(&pool, "cardiology").await, 0);
    }

    #[tokio::test]
    async fn fts_index_follows_update_and_cascade_delete() {
        let pool = fresh_pool().await;
        let id = doc(
            &pool,
            "3333333333333333333333333333333333333333333333333333333333333333",
        )
        .await;
        upsert(
            &pool,
            UpsertParams {
                source_document_id: id,
                title: None,
                text: "initial proctitis text",
            },
        )
        .await
        .expect("upsert");

        // Update replaces the indexed text.
        upsert(
            &pool,
            UpsertParams {
                source_document_id: id,
                title: None,
                text: "replacement colitis text",
            },
        )
        .await
        .expect("re-upsert");
        assert_eq!(fts_match_count(&pool, "proctitis").await, 0);
        assert_eq!(fts_match_count(&pool, "colitis").await, 1);

        // Deleting the parent source_documents row must cascade through
        // narrative_texts AND the FTS index (delete trigger fires on cascade).
        sqlx::query("DELETE FROM source_documents WHERE id = ?")
            .bind(id)
            .execute(&pool)
            .await
            .expect("delete doc");
        assert_eq!(fts_match_count(&pool, "colitis").await, 0);
    }
}
