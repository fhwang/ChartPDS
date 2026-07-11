//! Deduped "current" problems with provenance.
//!
//! Collapses `problems` rows to one per `(coding_system, coding_code)`, taking
//! source-asserted fields from the most-recent document (max `document_date`,
//! ties broken by max `source_documents.id`; a null date sorts oldest) and
//! attaching provenance (`document_count`, `first_seen`, `last_seen`). The
//! base layer does NOT judge active/resolved — `status` is the raw, unreliable
//! source value; the caller derives currency from provenance.

use sqlx::SqlitePool;

/// One deduped problem with provenance facts.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CurrentProblem {
    /// Coding system URI.
    pub coding_system: String,
    /// Code within the system.
    pub coding_code: String,
    /// Human-readable label from the newest document, if any.
    pub coding_display: Option<String>,
    /// Source-asserted CCDA status from the newest document. UNRELIABLE — do
    /// not treat as active/resolved truth; use provenance to judge currency.
    pub status: String,
    /// Onset date from the newest document, if any.
    pub onset_date: Option<String>,
    /// Number of documents that mention this code.
    pub document_count: i64,
    /// Earliest `document_date` mentioning this code.
    pub first_seen: Option<String>,
    /// Latest `document_date` mentioning this code.
    pub last_seen: Option<String>,
    /// Distinct verbatim section headings this code appeared under in
    /// narrative documents (e.g. `["Pre-Op Diagnosis/Indications"]`).
    /// Empty when the code only comes from CCDA problem sections.
    pub section_labels: Vec<String>,
}

/// Deduped current problems plus the newest document date in the archive.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CurrentProblems {
    /// Newest `document_date` across all source documents — the caller's
    /// reference point for judging recency. `None` if the archive is empty.
    pub latest_document_date: Option<String>,
    /// One entry per `(coding_system, coding_code)`.
    pub items: Vec<CurrentProblem>,
}

/// Dedupe problems to a current snapshot with provenance.
///
/// # Errors
///
/// Returns `sqlx::Error` if a query fails.
pub async fn current_problems(pool: &SqlitePool) -> Result<CurrentProblems, sqlx::Error> {
    let latest =
        sqlx::query!(r#"SELECT MAX(document_date) AS "latest?: String" FROM source_documents"#)
            .fetch_one(pool)
            .await?
            .latest;

    let rows = sqlx::query!(
        r#"
        WITH ranked AS (
            SELECT p.coding_system, p.coding_code, p.coding_display, p.status,
                   p.onset_date, sd.document_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY p.coding_system, p.coding_code
                       ORDER BY sd.document_date DESC, sd.id DESC
                   ) AS rn
            FROM problems p
            JOIN source_documents sd ON sd.id = p.source_document_id
        ),
        agg AS (
            SELECT p.coding_system, p.coding_code,
                   COUNT(DISTINCT p.source_document_id) AS document_count,
                   MIN(sd.document_date) AS first_seen,
                   MAX(sd.document_date) AS last_seen
            FROM problems p
            JOIN source_documents sd ON sd.id = p.source_document_id
            GROUP BY p.coding_system, p.coding_code
        )
        SELECT r.coding_system AS "coding_system!",
               r.coding_code AS "coding_code!",
               r.coding_display,
               r.status AS "status!",
               r.onset_date,
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

    let label_rows = sqlx::query!(
        r#"
        SELECT DISTINCT coding_system AS "coding_system!", coding_code AS "coding_code!",
               section_label AS "section_label!"
        FROM problems
        WHERE section_label IS NOT NULL
        ORDER BY coding_system, coding_code, section_label
        "#,
    )
    .fetch_all(pool)
    .await?;
    let mut labels: std::collections::HashMap<(String, String), Vec<String>> =
        std::collections::HashMap::new();
    for r in label_rows {
        labels
            .entry((r.coding_system, r.coding_code))
            .or_default()
            .push(r.section_label);
    }

    let items = rows
        .into_iter()
        .map(|r| {
            let section_labels = labels
                .remove(&(r.coding_system.clone(), r.coding_code.clone()))
                .unwrap_or_default();
            CurrentProblem {
                coding_system: r.coding_system,
                coding_code: r.coding_code,
                coding_display: r.coding_display,
                status: r.status,
                onset_date: r.onset_date,
                document_count: r.document_count,
                first_seen: r.first_seen,
                last_seen: r.last_seen,
                section_labels,
            }
        })
        .collect();

    Ok(CurrentProblems {
        latest_document_date: latest,
        items,
    })
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
    async fn dedupes_and_takes_newest_doc_with_provenance() {
        let pool = pool().await;
        // Older doc: status "active". Newer doc: status "resolved".
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
        for (id, status) in [(old, "active"), (new, "resolved")] {
            insert_problem(
                &pool,
                InsertProblemParams {
                    source_document_id: id,
                    coding_system: "http://snomed.info/sct",
                    coding_code: "44054006",
                    coding_display: Some("Type 2 diabetes mellitus"),
                    status,
                    onset_date: Some("2020-03-15"),
                    section_label: None,
                },
            )
            .await
            .expect("problem");
        }

        let result = current_problems(&pool).await.expect("query");
        assert_eq!(result.latest_document_date.as_deref(), Some("2024-06-01"));
        assert_eq!(result.items.len(), 1);
        let p = &result.items[0];
        assert_eq!(p.coding_code, "44054006");
        assert_eq!(p.status, "resolved"); // winning row = newest document
        assert_eq!(p.document_count, 2);
        assert_eq!(p.first_seen.as_deref(), Some("2021-01-01"));
        assert_eq!(p.last_seen.as_deref(), Some("2024-06-01"));
    }

    #[tokio::test]
    async fn dated_document_wins_over_null_document_date() {
        let pool = pool().await;
        // Doc with a real date (should win the "newest" slot under DESC ordering).
        let dated = doc(
            &pool,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "2023-01-01",
        )
        .await;
        // Doc with NULL document_date (sorts below any value under DESC — should lose).
        let null_dated = {
            let key = BlobKey::from_hex_str(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            )
            .expect("key");
            insert_source_document(
                &pool,
                InsertSourceDocumentParams {
                    archive_key: &key,
                    kind: "ccda",
                    source: "test",
                    original_filename: None,
                    archived_at: OffsetDateTime::now_utc(),
                    document_date: None,
                },
            )
            .await
            .expect("doc")
        };
        insert_problem(
            &pool,
            InsertProblemParams {
                source_document_id: dated,
                coding_system: "http://snomed.info/sct",
                coding_code: "73211009",
                coding_display: Some("Diabetes mellitus"),
                status: "active",
                onset_date: Some("2020-01-01"),
                section_label: None,
            },
        )
        .await
        .expect("problem");
        insert_problem(
            &pool,
            InsertProblemParams {
                source_document_id: null_dated,
                coding_system: "http://snomed.info/sct",
                coding_code: "73211009",
                coding_display: Some("Diabetes mellitus"),
                status: "resolved",
                onset_date: None,
                section_label: None,
            },
        )
        .await
        .expect("problem");

        let result = current_problems(&pool).await.expect("query");
        assert_eq!(result.items.len(), 1);
        // The dated doc (status "active") must win over the NULL-dated one.
        assert_eq!(result.items[0].status, "active");
        assert_eq!(result.items[0].document_count, 2);
    }

    #[tokio::test]
    async fn document_count_counts_distinct_documents() {
        let pool = pool().await;
        let doc_id = doc(
            &pool,
            "3333333333333333333333333333333333333333333333333333333333333333",
            "2023-05-01",
        )
        .await;
        // Two rows for the same code under the SAME source document.
        for onset in ["2020-01-01", "2021-06-15"] {
            insert_problem(
                &pool,
                InsertProblemParams {
                    source_document_id: doc_id,
                    coding_system: "http://snomed.info/sct",
                    coding_code: "73211009",
                    coding_display: Some("Diabetes mellitus"),
                    status: "active",
                    onset_date: Some(onset),
                    section_label: None,
                },
            )
            .await
            .expect("problem");
        }

        let result = current_problems(&pool).await.expect("query");
        assert_eq!(result.items.len(), 1);
        // One document, even though two rows were inserted.
        assert_eq!(result.items[0].document_count, 1);
    }

    #[tokio::test]
    async fn section_labels_are_collected_distinct() {
        let pool = pool().await;
        let d = doc(
            &pool,
            "4444444444444444444444444444444444444444444444444444444444444444",
            "2026-04-21",
        )
        .await;
        for label in [
            "Pre-Op Diagnosis/Indications",
            "Post-Op Diagnosis/ICD Codes",
        ] {
            insert_problem(
                &pool,
                InsertProblemParams {
                    source_document_id: d,
                    coding_system: "http://hl7.org/fhir/sid/icd-10-cm",
                    coding_code: "R10.9",
                    coding_display: Some("Abdominal pain, unspecified"),
                    status: "unknown",
                    onset_date: Some("2026-04-21"),
                    section_label: Some(label),
                },
            )
            .await
            .expect("problem");
        }
        let result = current_problems(&pool).await.expect("query");
        assert_eq!(result.items.len(), 1);
        assert_eq!(
            result.items[0].section_labels,
            vec![
                "Post-Op Diagnosis/ICD Codes".to_string(),
                "Pre-Op Diagnosis/Indications".to_string()
            ]
        );
    }
}
