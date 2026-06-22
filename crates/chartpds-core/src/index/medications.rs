//! `medications` table: prescriptions/administrations extracted from source documents.

use sqlx::SqlitePool;

/// A row from the `medications` table.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Medication {
    /// Auto-increment row id.
    pub id: i64,
    /// Foreign key into `source_documents`.
    pub source_document_id: i64,
    /// Coding system URI (e.g. `"http://www.nlm.nih.gov/research/umls/rxnorm"`).
    pub coding_system: String,
    /// Code within the coding system.
    pub coding_code: String,
    /// Human-readable label, if extracted.
    pub coding_display: Option<String>,
    /// Prescription status (e.g. `"active"`, `"completed"`).
    pub status: String,
    /// Dose description (e.g. `"500 mg"`).
    pub dose: Option<String>,
    /// Route of administration (e.g. `"oral"`).
    pub route: Option<String>,
    /// Frequency description (e.g. `"twice daily"`).
    pub frequency: Option<String>,
    /// Start date of the medication (ISO-8601 date string).
    pub start_date: Option<String>,
    /// End date of the medication (ISO-8601 date string).
    pub end_date: Option<String>,
}

/// Parameters for [`insert`].
pub struct InsertParams<'a> {
    /// Foreign key into `source_documents`.
    pub source_document_id: i64,
    /// Coding system URI.
    pub coding_system: &'a str,
    /// Code within the coding system.
    pub coding_code: &'a str,
    /// Optional human-readable label.
    pub coding_display: Option<&'a str>,
    /// Prescription status.
    pub status: &'a str,
    /// Dose description, if known.
    pub dose: Option<&'a str>,
    /// Route of administration, if known.
    pub route: Option<&'a str>,
    /// Frequency description, if known.
    pub frequency: Option<&'a str>,
    /// Start date, if known.
    pub start_date: Option<&'a str>,
    /// End date, if known.
    pub end_date: Option<&'a str>,
}

/// Insert a new medication row.
///
/// # Errors
///
/// Returns `sqlx::Error` if the insert fails (typically a foreign-key
/// violation on `source_document_id`).
pub async fn insert(pool: &SqlitePool, params: InsertParams<'_>) -> Result<i64, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        INSERT INTO medications (
            source_document_id, coding_system, coding_code, coding_display,
            status, dose, route, frequency, start_date, end_date
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING id AS "id!: i64"
        "#,
        params.source_document_id,
        params.coding_system,
        params.coding_code,
        params.coding_display,
        params.status,
        params.dose,
        params.route,
        params.frequency,
        params.start_date,
        params.end_date,
    )
    .fetch_one(pool)
    .await?;
    Ok(row.id)
}

/// List all medications belonging to a given source document.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn list_by_source_document(
    pool: &SqlitePool,
    source_document_id: i64,
) -> Result<Vec<Medication>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT id AS "id!: i64", source_document_id AS "source_document_id!: i64",
               coding_system, coding_code, coding_display,
               status, dose, route, frequency, start_date, end_date
        FROM medications
        WHERE source_document_id = ?
        ORDER BY start_date
        "#,
        source_document_id,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| Medication {
            id: r.id,
            source_document_id: r.source_document_id,
            coding_system: r.coding_system,
            coding_code: r.coding_code,
            coding_display: r.coding_display,
            status: r.status,
            dose: r.dose,
            route: r.route,
            frequency: r.frequency,
            start_date: r.start_date,
            end_date: r.end_date,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::BlobKey;
    use crate::index::{insert_source_document, open_pool, InsertSourceDocumentParams};
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
            InsertSourceDocumentParams {
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
            InsertParams {
                source_document_id: doc_id,
                coding_system: "http://www.nlm.nih.gov/research/umls/rxnorm",
                coding_code: "860975",
                coding_display: Some("Metformin 500 MG Oral Tablet"),
                status: "active",
                dose: Some("500 mg"),
                route: Some("oral"),
                frequency: None,
                start_date: Some("2021-06-01"),
                end_date: None,
            },
        )
        .await
        .expect("insert medication");

        let rows = list_by_source_document(&pool, doc_id).await.expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].coding_code, "860975");
        assert_eq!(
            rows[0].coding_display.as_deref(),
            Some("Metformin 500 MG Oral Tablet")
        );
        assert_eq!(rows[0].status, "active");
        assert_eq!(rows[0].dose.as_deref(), Some("500 mg"));
        assert_eq!(rows[0].route.as_deref(), Some("oral"));
        assert_eq!(rows[0].start_date.as_deref(), Some("2021-06-01"));
    }
}
