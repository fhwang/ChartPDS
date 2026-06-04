//! List all problems in the index.

use sqlx::SqlitePool;

use crate::index::Problem;

/// Fetch every problem row, ordered by onset date then coding code.
///
/// Returns an empty `Vec` when the `problems` table is empty.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn list_problems(pool: &SqlitePool) -> Result<Vec<Problem>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT id AS "id!: i64",
               source_document_id AS "source_document_id!: i64",
               coding_system, coding_code, coding_display,
               status, onset_date
        FROM problems
        ORDER BY onset_date, coding_code
        "#,
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
    use crate::index::{
        insert_problem, insert_source_document, open_pool, InsertProblemParams,
        InsertSourceDocumentParams,
    };
    use time::OffsetDateTime;

    #[tokio::test]
    async fn returns_seeded_problem() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");

        let archive_key = BlobKey::from_hex_str(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .expect("valid key");
        let doc_id = insert_source_document(
            &pool,
            InsertSourceDocumentParams {
                archive_key: &archive_key,
                kind: "ccda",
                source: "test",
                original_filename: None,
                ingested_at: OffsetDateTime::now_utc(),
            },
        )
        .await
        .expect("insert doc");

        insert_problem(
            &pool,
            InsertProblemParams {
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

        let problems = list_problems(&pool).await.expect("query");
        assert_eq!(problems.len(), 1);
        assert_eq!(problems[0].coding_code, "44054006");
        assert_eq!(
            problems[0].coding_display.as_deref(),
            Some("Type 2 diabetes mellitus")
        );
        assert_eq!(problems[0].status, "active");
        assert_eq!(problems[0].onset_date.as_deref(), Some("2020-03-15"));
    }
}
