//! List all medications in the index.

use sqlx::SqlitePool;

use crate::index::Medication;

/// Fetch every medication row, ordered by start date then coding code.
///
/// Returns an empty `Vec` when the `medications` table is empty.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails.
pub async fn list_medications(pool: &SqlitePool) -> Result<Vec<Medication>, sqlx::Error> {
    let rows = sqlx::query!(
        r#"
        SELECT id AS "id!: i64",
               source_document_id AS "source_document_id!: i64",
               coding_system, coding_code, coding_display,
               status, dose, route, frequency, start_date, end_date
        FROM medications
        ORDER BY start_date, coding_code
        "#,
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
    use crate::index::{
        insert_medication, insert_source_document, open_pool, InsertMedicationParams,
        InsertSourceDocumentParams,
    };
    use time::OffsetDateTime;

    #[tokio::test]
    async fn returns_seeded_medication() {
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
                archived_at: OffsetDateTime::now_utc(),
            },
        )
        .await
        .expect("insert doc");

        insert_medication(
            &pool,
            InsertMedicationParams {
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

        let meds = list_medications(&pool).await.expect("query");
        assert_eq!(meds.len(), 1);
        assert_eq!(meds[0].coding_code, "860975");
        assert_eq!(
            meds[0].coding_display.as_deref(),
            Some("Metformin 500 MG Oral Tablet")
        );
        assert_eq!(meds[0].status, "active");
        assert_eq!(meds[0].dose.as_deref(), Some("500 mg"));
        assert_eq!(meds[0].route.as_deref(), Some("oral"));
        assert_eq!(meds[0].start_date.as_deref(), Some("2021-06-01"));
    }
}
