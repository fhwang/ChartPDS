//! Deduped "current" medications with provenance.
//!
//! Same model as `current_problems`: one entry per `(coding_system,
//! coding_code)`, source-asserted fields from the newest document, provenance
//! attached. `status` is the raw, unreliable source value.

use sqlx::SqlitePool;

/// One deduped medication with provenance facts.
///
/// Note: `frequency` (present in `medications` table) is intentionally not
/// surfaced here — it matches the spec's field list and is currently always
/// NULL in CCDA ingestion.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CurrentMedication {
    /// Coding system URI.
    pub coding_system: String,
    /// Code within the system.
    pub coding_code: String,
    /// Human-readable label from the newest document, if any.
    pub coding_display: Option<String>,
    /// Source-asserted status from the newest document. UNRELIABLE.
    pub status: String,
    /// Dose from the newest document, if any.
    pub dose: Option<String>,
    /// Route from the newest document, if any.
    pub route: Option<String>,
    /// Start date from the newest document, if any.
    pub start_date: Option<String>,
    /// End date from the newest document, if any (a past `end_date` is a strong
    /// discontinuation signal).
    pub end_date: Option<String>,
    /// Number of documents that mention this code.
    pub document_count: i64,
    /// Earliest `document_date` mentioning this code.
    pub first_seen: Option<String>,
    /// Latest `document_date` mentioning this code.
    pub last_seen: Option<String>,
}

/// Deduped current medications plus the newest document date in the archive.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CurrentMedications {
    /// Newest `document_date` across all source documents. `None` if empty.
    pub latest_document_date: Option<String>,
    /// One entry per `(coding_system, coding_code)`.
    pub items: Vec<CurrentMedication>,
}

/// Dedupe medications to a current snapshot with provenance.
///
/// # Errors
///
/// Returns `sqlx::Error` if a query fails.
pub async fn current_medications(pool: &SqlitePool) -> Result<CurrentMedications, sqlx::Error> {
    let latest =
        sqlx::query!(r#"SELECT MAX(document_date) AS "latest?: String" FROM source_documents"#)
            .fetch_one(pool)
            .await?
            .latest;

    let rows = sqlx::query!(
        r#"
        WITH ranked AS (
            SELECT m.coding_system, m.coding_code, m.coding_display, m.status,
                   m.dose, m.route, m.start_date, m.end_date, sd.document_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY m.coding_system, m.coding_code
                       ORDER BY sd.document_date DESC, sd.id DESC
                   ) AS rn
            FROM medications m
            JOIN source_documents sd ON sd.id = m.source_document_id
        ),
        agg AS (
            SELECT m.coding_system, m.coding_code,
                   COUNT(DISTINCT m.source_document_id) AS document_count,
                   MIN(sd.document_date) AS first_seen,
                   MAX(sd.document_date) AS last_seen
            FROM medications m
            JOIN source_documents sd ON sd.id = m.source_document_id
            GROUP BY m.coding_system, m.coding_code
        )
        SELECT r.coding_system AS "coding_system!",
               r.coding_code AS "coding_code!",
               r.coding_display,
               r.status AS "status!",
               r.dose, r.route, r.start_date, r.end_date,
               a.document_count AS "document_count!: i64",
               a.first_seen AS "first_seen?: String",
               a.last_seen AS "last_seen?: String"
        FROM ranked r
        JOIN agg a ON a.coding_system = r.coding_system AND a.coding_code = r.coding_code
        WHERE r.rn = 1
        ORDER BY r.coding_system, r.coding_code
        "#,
    )
    .fetch_all(pool)
    .await?;

    let items = rows
        .into_iter()
        .map(|r| CurrentMedication {
            coding_system: r.coding_system,
            coding_code: r.coding_code,
            coding_display: r.coding_display,
            status: r.status,
            dose: r.dose,
            route: r.route,
            start_date: r.start_date,
            end_date: r.end_date,
            document_count: r.document_count,
            first_seen: r.first_seen,
            last_seen: r.last_seen,
        })
        .collect();

    Ok(CurrentMedications {
        latest_document_date: latest,
        items,
    })
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

    async fn pool() -> sqlx::SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    async fn doc(pool: &sqlx::SqlitePool, hex: &str, date: &str) -> i64 {
        let key = BlobKey::from_hex_str(hex).expect("key");
        insert_source_document(
            pool,
            InsertSourceDocumentParams {
                archive_key: &key,
                kind: "ccda",
                source: "test",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
                document_date: Some(date),
            },
        )
        .await
        .expect("doc")
    }

    #[tokio::test]
    async fn dedupes_meds_with_provenance_and_newest_fields() {
        let pool = pool().await;
        let old = doc(
            &pool,
            "1111111111111111111111111111111111111111111111111111111111111111",
            "2021-01-01",
        )
        .await;
        let new = doc(
            &pool,
            "2222222222222222222222222222222222222222222222222222222222222222",
            "2024-06-01",
        )
        .await;
        for (id, dose) in [(old, "10 mg"), (new, "20 mg")] {
            insert_medication(
                &pool,
                InsertMedicationParams {
                    source_document_id: id,
                    coding_system: "http://www.nlm.nih.gov/research/umls/rxnorm",
                    coding_code: "617314",
                    coding_display: Some("Atorvastatin 20 MG Oral Tablet"),
                    status: "active",
                    dose: Some(dose),
                    route: Some("oral"),
                    frequency: None,
                    start_date: Some("2021-01-01"),
                    end_date: None,
                },
            )
            .await
            .expect("med");
        }

        let result = current_medications(&pool).await.expect("query");
        assert_eq!(result.latest_document_date.as_deref(), Some("2024-06-01"));
        assert_eq!(result.items.len(), 1);
        let m = &result.items[0];
        assert_eq!(m.coding_code, "617314");
        assert_eq!(m.dose.as_deref(), Some("20 mg")); // newest doc wins
        assert_eq!(m.document_count, 2);
        assert_eq!(m.first_seen.as_deref(), Some("2021-01-01"));
        assert_eq!(m.last_seen.as_deref(), Some("2024-06-01"));
    }

    #[tokio::test]
    async fn document_count_counts_distinct_documents() {
        let pool = pool().await;
        let doc_id = doc(
            &pool,
            "4444444444444444444444444444444444444444444444444444444444444444",
            "2023-08-01",
        )
        .await;
        // Two rows for the same code under the SAME source document (different dose).
        for dose in ["5 mg", "10 mg"] {
            insert_medication(
                &pool,
                InsertMedicationParams {
                    source_document_id: doc_id,
                    coding_system: "http://www.nlm.nih.gov/research/umls/rxnorm",
                    coding_code: "861007",
                    coding_display: Some("Metformin"),
                    status: "active",
                    dose: Some(dose),
                    route: Some("oral"),
                    frequency: None,
                    start_date: Some("2023-01-01"),
                    end_date: None,
                },
            )
            .await
            .expect("med");
        }

        let result = current_medications(&pool).await.expect("query");
        assert_eq!(result.items.len(), 1);
        // One document, even though two rows were inserted.
        assert_eq!(result.items[0].document_count, 1);
    }
}
