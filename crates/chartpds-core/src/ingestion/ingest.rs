//! Orchestrator: archive blob + metadata -> index rows.

use bytes::Bytes;
use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::archive::{Archive, Manifest};
use crate::index::{
    insert_medication, insert_observation, insert_problem, insert_source_document,
    InsertMedicationParams, InsertObservationParams, InsertProblemParams,
    InsertSourceDocumentParams,
};
use crate::ingestion::ccda::parse::{extract_document_date, parse_xml};
use crate::ingestion::ccda::self_check::self_check;
use crate::ingestion::ccda::{
    extract_medications, extract_observations, extract_problems, extract_results,
};
use crate::ingestion::{Error, Result};

/// Ingest a CCDA document.
///
/// Steps:
/// 1. Write the blob + its provenance manifest to the archive
///    (content-addressed; idempotent).
/// 2. Parse the XML and run the CCDA self-check.
/// 3. Extract observations (vital signs + lab results), problems, and
///    medications.
/// 4. Insert a `source_documents` row.
/// 5. Insert observations, problems, and medications rows.
///
/// Returns the `source_documents.id`.
///
/// # Errors
///
/// Returns [`Error`] if any step fails. Failure modes include malformed
/// XML, non-CCDA input, archive backend errors, and database errors.
///
/// # Crash-consistency
///
/// This function does NOT use a transaction. If the process crashes between
/// inserting the `source_documents` row and finishing all `observations`
/// inserts, the index can end up partially populated. Recovery: re-ingest
/// from the archive (the bytes are durable). A transactional variant is
/// planned when sources/sync need it.
///
/// Note that the blob is archived BEFORE parse + self-check. If validation
/// fails, the blob is still in the archive but has no `source_documents`
/// row referencing it. This is deliberate: preserving the raw input even
/// for bad data is useful for debugging and re-validation if the parser
/// improves later. Orphan blobs are harmless given the content-addressed
/// model; a future GC pass can reconcile if storage cost matters.
pub async fn ingest(
    archive: &Archive,
    pool: &SqlitePool,
    content: Bytes,
    kind: &str,
    source: &str,
    original_filename: Option<&str>,
    archived_at: OffsetDateTime,
) -> Result<i64> {
    // 1. Archive the blob with its provenance manifest (CloudEvents-shaped).
    let manifest = Manifest::new(
        source,
        kind,
        "application/xml",
        None,
        archived_at,
        original_filename.map(str::to_owned),
    );
    let archive_key = archive.put_with_manifest(content.clone(), manifest).await?;

    // 2. Parse + self-check. `roxmltree` needs a `&str`.
    let xml = std::str::from_utf8(&content).map_err(|err| Error::NotCcda {
        reason: format!("input bytes not valid UTF-8: {err}"),
    })?;
    let doc = parse_xml(xml)?;
    self_check(&doc)?;
    let document_date = extract_document_date(&doc);

    // 3. Extract observations, problems, and medications. Vital signs and lab
    // results are both observations and share the observations table.
    let mut extracted = extract_observations(&doc)?;
    extracted.extend(extract_results(&doc)?);
    let problems = extract_problems(&doc)?;
    let medications = extract_medications(&doc)?;

    // 4. Insert source_documents row.
    let source_document_id = insert_source_document(
        pool,
        InsertSourceDocumentParams {
            archive_key: &archive_key,
            kind,
            source,
            original_filename,
            archived_at,
            document_date: document_date.as_deref(),
        },
    )
    .await?;

    // 5. Insert observations, problems, and medications.
    for obs in extracted {
        insert_observation(
            pool,
            InsertObservationParams {
                source_document_id,
                coding_system: &obs.coding_system,
                coding_code: &obs.coding_code,
                coding_display: obs.coding_display.as_deref(),
                effective_start: obs.effective_start,
                effective_end: obs.effective_end,
                value_quantity: obs.value_quantity,
                value_string: obs.value_string.as_deref(),
                value_unit: obs.value_unit.as_deref(),
            },
        )
        .await?;
    }

    for prob in problems {
        insert_problem(
            pool,
            InsertProblemParams {
                source_document_id,
                coding_system: &prob.coding_system,
                coding_code: &prob.coding_code,
                coding_display: prob.coding_display.as_deref(),
                status: &prob.status,
                onset_date: prob.onset_date.as_deref(),
                section_label: None,
            },
        )
        .await?;
    }

    for med in medications {
        insert_medication(
            pool,
            InsertMedicationParams {
                source_document_id,
                coding_system: &med.coding_system,
                coding_code: &med.coding_code,
                coding_display: med.coding_display.as_deref(),
                status: &med.status,
                dose: med.dose.as_deref(),
                route: med.route.as_deref(),
                frequency: med.frequency.as_deref(),
                start_date: med.start_date.as_deref(),
                end_date: med.end_date.as_deref(),
            },
        )
        .await?;
    }

    Ok(source_document_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::Archive;
    use crate::index::{
        list_medications_by_source_document, list_observations_by_source_document,
        list_problems_by_source_document, open_pool,
    };
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use std::sync::Arc;
    use time::OffsetDateTime;

    const VALID: &[u8] = include_bytes!("ccda/fixtures/valid_minimal.xml");
    const NOT_CCDA: &[u8] = include_bytes!("ccda/fixtures/not_ccda.xml");

    async fn fresh_pool_and_archive() -> (sqlx::SqlitePool, Archive) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        // Leak the temp dir so the file lives as long as the pool.
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        (pool, Archive::new(backend))
    }

    #[tokio::test]
    async fn ingest_valid_ccda_archives_blob_and_inserts_rows() {
        let (pool, archive) = fresh_pool_and_archive().await;

        let id = ingest(
            &archive,
            &pool,
            Bytes::from_static(VALID),
            "ccda",
            "test",
            Some("ccd.xml"),
            OffsetDateTime::now_utc(),
        )
        .await
        .expect("ingest succeeds");

        let observations = list_observations_by_source_document(&pool, id)
            .await
            .expect("list");
        // 2 vital signs (weight, height) + 2 lab results (HbA1c, LDL-C).
        assert_eq!(observations.len(), 4);
        assert!(observations.iter().any(|o| o.coding_code == "4548-4"));
        assert!(observations.iter().any(|o| o.coding_code == "13457-7"));

        let problems = list_problems_by_source_document(&pool, id)
            .await
            .expect("list problems");
        assert_eq!(problems.len(), 1);
        assert_eq!(problems[0].coding_code, "44054006");

        let medications = list_medications_by_source_document(&pool, id)
            .await
            .expect("list meds");
        assert_eq!(medications.len(), 1);
        assert_eq!(medications[0].coding_code, "860975");
    }

    #[tokio::test]
    async fn ingest_not_ccda_returns_not_ccda_error() {
        let (pool, archive) = fresh_pool_and_archive().await;

        let err = ingest(
            &archive,
            &pool,
            Bytes::from_static(NOT_CCDA),
            "ccda",
            "test",
            None,
            OffsetDateTime::now_utc(),
        )
        .await
        .expect_err("ingest should reject non-CCDA");

        assert!(matches!(err, crate::ingestion::Error::NotCcda { .. }));
    }

    #[tokio::test]
    async fn ingest_non_utf8_bytes_returns_not_ccda_error() {
        let (pool, archive) = fresh_pool_and_archive().await;
        // Two raw bytes that are valid as data but not as UTF-8 prefix.
        let invalid_utf8 = Bytes::from_static(&[0xFF, 0xFE]);

        let err = ingest(
            &archive,
            &pool,
            invalid_utf8,
            "ccda",
            "test",
            None,
            OffsetDateTime::now_utc(),
        )
        .await
        .expect_err("ingest should reject non-UTF-8 input");

        assert!(matches!(err, crate::ingestion::Error::NotCcda { .. }));
    }

    #[tokio::test]
    async fn ingest_stores_ccda_document_date() {
        let (pool, archive) = fresh_pool_and_archive().await;
        let id = ingest(
            &archive,
            &pool,
            Bytes::from_static(VALID),
            "ccda",
            "test",
            None,
            OffsetDateTime::now_utc(),
        )
        .await
        .expect("ingest");

        let row: (Option<String>,) =
            sqlx::query_as("SELECT document_date FROM source_documents WHERE id = ?")
                .bind(id)
                .fetch_one(&pool)
                .await
                .expect("row");
        assert_eq!(row.0.as_deref(), Some("2026-01-01"));
    }

    #[tokio::test]
    async fn re_ingest_of_same_archive_key_fails_on_unique_constraint() {
        // Documents a known limitation: today's ingest() does not handle
        // re-ingest. Same content -> same archive_key -> UNIQUE violation
        // on the second insert into source_documents. A future re-ingest
        // workflow will delete the existing source_documents row first
        // (the FK CASCADE cleans up the observations).
        let (pool, archive) = fresh_pool_and_archive().await;

        ingest(
            &archive,
            &pool,
            Bytes::from_static(VALID),
            "ccda",
            "test",
            None,
            OffsetDateTime::now_utc(),
        )
        .await
        .expect("first ingest");

        let err = ingest(
            &archive,
            &pool,
            Bytes::from_static(VALID),
            "ccda",
            "test",
            None,
            OffsetDateTime::now_utc(),
        )
        .await
        .expect_err("second ingest should collide on archive_key");

        assert!(matches!(err, crate::ingestion::Error::Database(_)));
    }
}
