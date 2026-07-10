//! `problems` table: diagnoses extracted from source documents.

use sqlx::SqlitePool;

/// A row from the `problems` table.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Problem {
    /// Auto-increment row id.
    pub id: i64,
    /// Foreign key into `source_documents`.
    pub source_document_id: i64,
    /// Coding system URI (e.g. `"http://snomed.info/sct"`).
    pub coding_system: String,
    /// Code within the coding system.
    pub coding_code: String,
    /// Human-readable label, if extracted.
    pub coding_display: Option<String>,
    /// Clinical status (e.g. `"active"`, `"resolved"`).
    pub status: String,
    /// Date of onset, if known (ISO-8601 date string).
    pub onset_date: Option<String>,
}

/// A problem (diagnosis) ready to be inserted: a [`Problem`] minus its `id`.
pub struct NewProblem<'a> {
    /// Foreign key into `source_documents`.
    pub source_document_id: i64,
    /// Coding system URI.
    pub coding_system: &'a str,
    /// Code within the coding system.
    pub coding_code: &'a str,
    /// Optional human-readable label.
    pub coding_display: Option<&'a str>,
    /// Clinical status.
    pub status: &'a str,
    /// Date of onset, if known.
    pub onset_date: Option<&'a str>,
}

/// Insert a new problem row.
///
/// # Errors
///
/// Returns `sqlx::Error` if the insert fails (typically a foreign-key
/// violation on `source_document_id`).
pub async fn insert(pool: &SqlitePool, problem: NewProblem<'_>) -> Result<i64, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        INSERT INTO problems (
            source_document_id, coding_system, coding_code, coding_display,
            status, onset_date
        )
        VALUES (?, ?, ?, ?, ?, ?)
        RETURNING id AS "id!: i64"
        "#,
        problem.source_document_id,
        problem.coding_system,
        problem.coding_code,
        problem.coding_display,
        problem.status,
        problem.onset_date,
    )
    .fetch_one(pool)
    .await?;
    Ok(row.id)
}

/// List all problems belonging to a given source document.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn list_by_source_document(
    pool: &SqlitePool,
    source_document_id: i64,
) -> Result<Vec<Problem>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT id AS "id!: i64", source_document_id AS "source_document_id!: i64",
               coding_system, coding_code, coding_display,
               status, onset_date
        FROM problems
        WHERE source_document_id = ?
        ORDER BY onset_date
        "#,
        source_document_id,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| Problem {
            id: r.id,
            source_document_id: r.source_document_id,
            coding_system: r.coding_system,
            coding_code: r.coding_code,
            coding_display: r.coding_display,
            status: r.status,
            onset_date: r.onset_date,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::BlobKey;
    use crate::index::{insert_source_document, open_pool, NewSourceDocument};
    use time::OffsetDateTime;

    async fn fresh_pool_with_doc() -> (sqlx::SqlitePool, i64) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        // Leak the temp dir so the file lives as long as the pool.
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let archive_key = BlobKey::from_hex_str(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .expect("valid key");
        let id = insert_source_document(
            &pool,
            NewSourceDocument {
                archive_key: &archive_key,
                kind: "ccda",
                source: "test",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
                document_date: None,
            },
        )
        .await
        .expect("insert doc");
        (pool, id)
    }

    #[tokio::test]
    async fn insert_and_list_for_source_document_round_trips() {
        let (pool, doc_id) = fresh_pool_with_doc().await;

        insert(
            &pool,
            NewProblem {
                source_document_id: doc_id,
                coding_system: "http://snomed.info/sct",
                coding_code: "44054006",
                coding_display: Some("Type 2 diabetes mellitus"),
                status: "active",
                onset_date: Some("2020-03-15"),
            },
        )
        .await
        .expect("insert problem");

        let rows = list_by_source_document(&pool, doc_id).await.expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].coding_code, "44054006");
        assert_eq!(
            rows[0].coding_display.as_deref(),
            Some("Type 2 diabetes mellitus")
        );
        assert_eq!(rows[0].status, "active");
        assert_eq!(rows[0].onset_date.as_deref(), Some("2020-03-15"));
    }
}
