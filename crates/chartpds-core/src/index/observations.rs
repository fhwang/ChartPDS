//! `observations` table: structured measurements extracted from documents.

use sqlx::SqlitePool;
use time::OffsetDateTime;

/// A row from the `observations` table.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Observation {
    /// Auto-increment row id.
    pub id: i64,
    /// Foreign key into `source_documents`.
    pub source_document_id: i64,
    /// FHIR system URI for the coding.
    pub coding_system: String,
    /// FHIR code within the coding system.
    pub coding_code: String,
    /// Human-readable label, if extracted.
    pub coding_display: Option<String>,
    /// Effective start time of the measurement.
    pub effective_start: OffsetDateTime,
    /// Effective end time, if the measurement spans a range.
    pub effective_end: Option<OffsetDateTime>,
    /// Numeric value, if this is a quantitative observation.
    pub value_quantity: Option<f64>,
    /// String value, if this is a categorical observation.
    pub value_string: Option<String>,
    /// Unit for `value_quantity` (e.g. `"kg"`, `"mmHg"`).
    pub value_unit: Option<String>,
}

/// Parameters for [`insert`].
pub struct InsertParams<'a> {
    /// Foreign key into `source_documents`.
    pub source_document_id: i64,
    /// FHIR system URI.
    pub coding_system: &'a str,
    /// FHIR code.
    pub coding_code: &'a str,
    /// Optional human-readable label.
    pub coding_display: Option<&'a str>,
    /// Effective start time.
    pub effective_start: OffsetDateTime,
    /// Effective end time, if applicable.
    pub effective_end: Option<OffsetDateTime>,
    /// Numeric value, if applicable.
    pub value_quantity: Option<f64>,
    /// String value, if applicable.
    pub value_string: Option<&'a str>,
    /// Unit for `value_quantity`, if applicable.
    pub value_unit: Option<&'a str>,
}

/// Insert a new observation row.
///
/// # Errors
///
/// Returns `sqlx::Error` if the insert fails (typically a foreign-key
/// violation on `source_document_id`).
pub async fn insert(pool: &SqlitePool, params: InsertParams<'_>) -> Result<i64, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        INSERT INTO observations (
            source_document_id, coding_system, coding_code, coding_display,
            effective_start, effective_end,
            value_quantity, value_string, value_unit
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING id AS "id!: i64"
        "#,
        params.source_document_id,
        params.coding_system,
        params.coding_code,
        params.coding_display,
        params.effective_start,
        params.effective_end,
        params.value_quantity,
        params.value_string,
        params.value_unit,
    )
    .fetch_one(pool)
    .await?;
    Ok(row.id)
}

/// List all observations belonging to a given source document.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn list_by_source_document(
    pool: &SqlitePool,
    source_document_id: i64,
) -> Result<Vec<Observation>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT id AS "id!: i64", source_document_id AS "source_document_id!: i64",
               coding_system, coding_code, coding_display,
               effective_start AS "effective_start: OffsetDateTime",
               effective_end AS "effective_end?: OffsetDateTime",
               value_quantity, value_string, value_unit
        FROM observations
        WHERE source_document_id = ?
        ORDER BY effective_start
        "#,
        source_document_id,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| Observation {
            id: r.id,
            source_document_id: r.source_document_id,
            coding_system: r.coding_system,
            coding_code: r.coding_code,
            coding_display: r.coding_display,
            effective_start: r.effective_start,
            effective_end: r.effective_end,
            value_quantity: r.value_quantity,
            value_string: r.value_string,
            value_unit: r.value_unit,
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
            },
        )
        .await
        .expect("insert doc");
        (pool, id)
    }

    #[tokio::test]
    async fn insert_and_list_for_source_document_round_trips() {
        let (pool, doc_id) = fresh_pool_with_doc().await;
        let now = OffsetDateTime::now_utc();

        insert(
            &pool,
            InsertParams {
                source_document_id: doc_id,
                coding_system: "http://loinc.org",
                coding_code: "29463-7",
                coding_display: Some("Body Weight"),
                effective_start: now,
                effective_end: None,
                value_quantity: Some(72.5),
                value_string: None,
                value_unit: Some("kg"),
            },
        )
        .await
        .expect("insert observation");

        let rows = list_by_source_document(&pool, doc_id).await.expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].coding_code, "29463-7");
        assert_eq!(rows[0].value_quantity, Some(72.5));
        assert_eq!(rows[0].value_unit.as_deref(), Some("kg"));
    }

    #[tokio::test]
    async fn delete_of_source_document_cascades_to_observations() {
        let (pool, doc_id) = fresh_pool_with_doc().await;
        let now = OffsetDateTime::now_utc();

        insert(
            &pool,
            InsertParams {
                source_document_id: doc_id,
                coding_system: "http://loinc.org",
                coding_code: "8302-2",
                coding_display: Some("Body height"),
                effective_start: now,
                effective_end: None,
                value_quantity: Some(175.0),
                value_string: None,
                value_unit: Some("cm"),
            },
        )
        .await
        .expect("insert observation");

        sqlx::query!("DELETE FROM source_documents WHERE id = ?", doc_id)
            .execute(&pool)
            .await
            .expect("delete doc");

        let rows = list_by_source_document(&pool, doc_id)
            .await
            .expect("list after cascade");
        assert!(rows.is_empty());
    }
}
