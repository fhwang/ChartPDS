//! Narrative PDF ingestion: archive → deterministic text → one-time verified
//! LLM extraction (frozen as an artifact in the derived store) → index rows.

use bytes::Bytes;
use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::archive::{Archive, BlobKey, Manifest};
use crate::extraction::{
    extract_pdf_text, verify_extraction, ExtractionArtifact, ExtractorInfo, LlmExtractor,
    VerifiedExtraction, EXTRACTION_MODEL, PROMPT_VERSION,
};
use crate::index::{
    delete_source_document, fetch_source_document_by_archive_key, insert_problem,
    insert_source_document, set_narrative_title, set_source_document_date, upsert_narrative_text,
    InsertProblemParams, InsertSourceDocumentParams, UpsertNarrativeTextParams,
};
use crate::ingestion::{Error, Result};

/// `source_documents.kind` / manifest `type` for a narrative PDF blob.
pub const NARRATIVE_PDF_KIND: &str = "clinical-pdf";
/// Manifest `type` for the frozen extraction artifact blob.
pub const NARRATIVE_EXTRACTION_KIND: &str = "narrative-extraction";

/// What `ingest_narrative_pdf` did, reported in-band to the tool caller.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NarrativeIngestOutcome {
    /// The `source_documents.id` of the ingested narrative.
    pub source_document_id: i64,
    /// Extractor-authored title, if extraction ran.
    pub title: Option<String>,
    /// Verified document date, if extraction ran and the date verified.
    pub document_date: Option<String>,
    /// Number of verified codings indexed into `problems`.
    pub codings_indexed: u64,
    /// Always `"applied"`. Extraction is required, so the only way an
    /// ingest reports anything else is by failing outright (see
    /// [`ingest_narrative_pdf`]); the field stays so callers can key on it.
    pub extraction_status: String,
    /// Human-readable reasons for LLM claims dropped by verification.
    pub rejected: Vec<String>,
}

/// Caller-supplied provenance for one narrative PDF ingest.
#[derive(Debug, Clone, Copy)]
pub struct NarrativeIngestParams<'a> {
    /// Caller-supplied source label (e.g. `manual-upload`).
    pub source: &'a str,
    /// Original filename, carried into the blob manifest.
    pub original_filename: Option<&'a str>,
    /// Immutable time the bytes first entered the archive.
    pub archived_at: OffsetDateTime,
}

/// Ingest one narrative PDF.
///
/// Steps: extract text (fail fast on scans — nothing archived) → LLM
/// extraction + mechanical verification → archive the PDF blob (manifest
/// `subject` = verified date) → freeze the verified extraction as its own
/// JSON artifact blob in the derived store → upsert index rows (document,
/// narrative text, coded problems).
///
/// LLM extraction is required: whether the extractor is missing entirely
/// (no `ANTHROPIC_API_KEY`) or fails after its own bounded in-band retries,
/// the whole ingest fails before anything is archived or indexed. There is
/// no text-only fallback, so there is never partial state to reconcile —
/// the caller fixes the configuration or waits out the outage, then re-runs
/// the same ingest, which starts clean.
///
/// # Errors
///
/// Returns [`Error::ExtractorNotConfigured`] when no extractor is supplied,
/// [`Error::Extraction`] when the PDF has no text layer or cannot be parsed
/// or when the LLM extraction fails (nothing is persisted in any of these
/// cases), and [`Error::Archive`]/[`Error::Database`] on storage failures.
pub async fn ingest_narrative_pdf<E: LlmExtractor>(
    archive: &Archive,
    derived: &Archive,
    pool: &SqlitePool,
    content: Bytes,
    params: NarrativeIngestParams<'_>,
    extractor: Option<&E>,
) -> Result<NarrativeIngestOutcome> {
    let NarrativeIngestParams {
        source,
        original_filename,
        archived_at,
    } = params;
    // 1. Deterministic text extraction. A scan/no-text PDF is a hard error
    //    before anything is archived — the caller should hear "OCR
    //    unsupported" rather than accumulate unusable blobs.
    let text = extract_pdf_text(&content)?;

    // 2. LLM extraction + verification. Extraction is required: a missing
    //    extractor (no ANTHROPIC_API_KEY) or an extraction failure (after
    //    the extractor's own bounded retries) fails the ingest here, before
    //    anything is archived or indexed.
    let Some(extractor) = extractor else {
        return Err(Error::ExtractorNotConfigured);
    };
    let verified = verify_extraction(&text, extractor.extract(&text).await?);

    // 3-4. Archive the PDF blob and freeze the verified extraction as its
    //      own artifact blob in the derived store.
    let (pdf_key, artifact) = archive_narrative_blobs(
        archive,
        derived,
        content,
        source,
        original_filename,
        archived_at,
        &verified,
    )
    .await?;

    // 5. Upsert the document row: re-ingest of the same bytes replaces the
    //    prior rows (cascade cleans narrative_texts + problems, and the FTS
    //    delete trigger fires on the cascade). This ingest always carries a
    //    freshly verified extraction — the only kind that can reach this
    //    point — so replacing is always an upgrade, never a downgrade.
    if let Some(existing) = fetch_source_document_by_archive_key(pool, &pdf_key).await? {
        delete_source_document(pool, existing.id).await?;
    }
    let source_document_id = insert_source_document(
        pool,
        InsertSourceDocumentParams {
            archive_key: &pdf_key,
            kind: NARRATIVE_PDF_KIND,
            source,
            original_filename,
            archived_at,
            document_date: None, // applied from the artifact below
        },
    )
    .await?;

    // 6. Index the text (FTS via triggers).
    upsert_narrative_text(
        pool,
        UpsertNarrativeTextParams {
            source_document_id,
            title: None, // applied from the artifact below
            text: &text,
        },
    )
    .await?;

    // 7. Apply the artifact (date, title, coded problems).
    let codings_indexed = apply_extraction(pool, source_document_id, &artifact).await?;

    Ok(NarrativeIngestOutcome {
        source_document_id,
        title: artifact.title,
        document_date: artifact.document_date,
        codings_indexed,
        extraction_status: "applied".to_owned(),
        rejected: verified.rejected,
    })
}

/// Archive the PDF blob and freeze its verified extraction as its own JSON
/// artifact blob in the derived store, referencing the PDF's hash. Source
/// bytes and machine derivations live in separate stores: the archive holds
/// only bytes that arrived from outside, while the derived store holds
/// expensive-to-recreate derivations with their own lifecycle.
///
/// Split out of `ingest_narrative_pdf` to keep that function under the
/// line-count lint; has no independent meaning outside steps 3-4 of that
/// function's flow.
async fn archive_narrative_blobs(
    archive: &Archive,
    derived: &Archive,
    content: Bytes,
    source: &str,
    original_filename: Option<&str>,
    archived_at: OffsetDateTime,
    verified: &VerifiedExtraction,
) -> Result<(BlobKey, ExtractionArtifact)> {
    // 3. Archive the PDF blob. `subject` carries the verified document date
    //    so the blob is self-describing for text-only replay.
    let pdf_manifest = Manifest::new(
        source,
        NARRATIVE_PDF_KIND,
        "application/pdf",
        verified.document_date.clone(),
        archived_at,
        original_filename.map(str::to_owned),
    );
    let pdf_key = archive.put_with_manifest(content, pdf_manifest).await?;

    // 4. Freeze the verified extraction as its own artifact blob in the
    //    derived store.
    let artifact = ExtractionArtifact {
        document: pdf_key.to_string(),
        document_date: verified.document_date.clone(),
        document_date_quote: verified.document_date_quote.clone(),
        title: verified.title.clone(),
        codings: verified.codings.clone(),
        extractor: ExtractorInfo {
            model: EXTRACTION_MODEL.to_owned(),
            prompt_version: PROMPT_VERSION,
        },
        extracted_at: archived_at,
    };
    let bytes = serde_json::to_vec(&artifact).map_err(|err| {
        Error::Extraction(crate::extraction::Error::InvalidResponse {
            reason: format!("serializing artifact: {err}"),
        })
    })?;
    let manifest = Manifest::new(
        "chartpds",
        NARRATIVE_EXTRACTION_KIND,
        "application/json",
        Some(pdf_key.to_string()),
        archived_at,
        None,
    );
    derived
        .put_with_manifest(Bytes::from(bytes), manifest)
        .await?;

    Ok((pdf_key, artifact))
}

/// Apply a frozen extraction artifact to an indexed narrative document:
/// set the document date and title, insert one `problems` row per coding.
///
/// Shared by live ingestion and `rebuild_index` — this is the ONLY code path
/// that turns an artifact into index rows.
pub(crate) async fn apply_extraction(
    pool: &SqlitePool,
    source_document_id: i64,
    artifact: &ExtractionArtifact,
) -> Result<u64> {
    if let Some(date) = &artifact.document_date {
        set_source_document_date(pool, source_document_id, date).await?;
    }
    if let Some(title) = &artifact.title {
        set_narrative_title(pool, source_document_id, title).await?;
    }
    let mut count = 0u64;
    for c in &artifact.codings {
        insert_problem(
            pool,
            InsertProblemParams {
                source_document_id,
                coding_system: &c.system,
                coding_code: &c.code,
                coding_display: Some(&c.display),
                status: "unknown",
                onset_date: artifact.document_date.as_deref(),
                section_label: c.section_label.as_deref(),
            },
        )
        .await?;
        count += 1;
    }
    Ok(count)
}

/// Text-only replay of an archived narrative PDF blob during rebuild.
///
/// Re-derives the text deterministically and rebuilds the document +
/// narrative rows. The document date comes from the manifest `subject`
/// (also re-applied by the artifact pass, when an artifact exists).
/// Returns the new `source_documents.id`.
pub(crate) async fn replay_pdf(
    pool: &SqlitePool,
    key: &BlobKey,
    content: &Bytes,
    manifest: &Manifest,
) -> Result<i64> {
    let text = extract_pdf_text(content)?;
    let source_document_id = insert_source_document(
        pool,
        InsertSourceDocumentParams {
            archive_key: key,
            kind: NARRATIVE_PDF_KIND,
            source: &manifest.source,
            original_filename: manifest.original_filename.as_deref(),
            archived_at: manifest.archived_at,
            document_date: manifest.subject.as_deref(),
        },
    )
    .await?;
    upsert_narrative_text(
        pool,
        UpsertNarrativeTextParams {
            source_document_id,
            title: None,
            text: &text,
        },
    )
    .await?;
    Ok(source_document_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extraction::{RawCoding, RawExtraction};
    use crate::index::{list_problems_by_source_document, open_pool};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    const FIXTURE: &[u8] = include_bytes!("../extraction/fixtures/synthetic_pathology.pdf");

    async fn fresh_pool_and_stores() -> (SqlitePool, Archive, Archive) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        let derived_backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        (pool, Archive::new(backend), Archive::new(derived_backend))
    }

    /// Canned extractor: returns a fixed `RawExtraction` without any network.
    struct MockExtractor(RawExtraction);
    impl LlmExtractor for MockExtractor {
        async fn extract(
            &self,
            _text: &str,
        ) -> std::result::Result<RawExtraction, crate::extraction::Error> {
            Ok(self.0.clone())
        }
    }

    /// Canned failing extractor.
    struct FailingExtractor;
    impl LlmExtractor for FailingExtractor {
        async fn extract(
            &self,
            _text: &str,
        ) -> std::result::Result<RawExtraction, crate::extraction::Error> {
            Err(crate::extraction::Error::Api {
                reason: "simulated outage".to_owned(),
            })
        }
    }

    fn fixture_extraction() -> RawExtraction {
        RawExtraction {
            document_date: Some("2026-04-21".to_owned()),
            document_date_quote: Some("Order Date: 04/21/2026".to_owned()),
            title: Some("GI Pathology Report — colon biopsy".to_owned()),
            codings: vec![
                RawCoding {
                    code: "R10.9".to_owned(),
                    display: "Abdominal pain, unspecified".to_owned(),
                    quote: "Abdominal pain, unspecified - R10.9".to_owned(),
                    section_label: Some("Pre-Op Diagnosis/Indications".to_owned()),
                },
                // A hallucinated code the fixture does not contain: must be
                // rejected by verification and never reach the index.
                RawCoding {
                    code: "K62.5".to_owned(),
                    display: "Hemorrhage of anus and rectum".to_owned(),
                    quote: "Hemorrhage of anus and rectum - K62.5".to_owned(),
                    section_label: None,
                },
            ],
        }
    }

    #[tokio::test]
    async fn ingests_pdf_with_verified_extraction() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockExtractor(fixture_extraction());

        let outcome = ingest_narrative_pdf(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(FIXTURE),
            NarrativeIngestParams {
                source: "manual-upload",
                original_filename: Some("synthetic_pathology.pdf"),
                archived_at: OffsetDateTime::now_utc(),
            },
            Some(&extractor),
        )
        .await
        .expect("ingest");

        assert_eq!(outcome.extraction_status, "applied");
        assert_eq!(outcome.codings_indexed, 1);
        assert_eq!(outcome.rejected.len(), 1, "hallucinated coding rejected");
        assert_eq!(outcome.document_date.as_deref(), Some("2026-04-21"));

        // PDF blob in the archive; artifact blob in the derived store.
        assert_eq!(archive.list_keys().await.expect("keys").len(), 1);
        assert_eq!(derived.list_keys().await.expect("keys").len(), 1);

        // Problems row landed with section label and unknown status.
        let problems = list_problems_by_source_document(&pool, outcome.source_document_id)
            .await
            .expect("problems");
        assert_eq!(problems.len(), 1);
        assert_eq!(problems[0].coding_code, "R10.9");
        assert_eq!(problems[0].status, "unknown");
        assert_eq!(
            problems[0].section_label.as_deref(),
            Some("Pre-Op Diagnosis/Indications")
        );

        // Document row has the verified date.
        let doc = crate::index::get_source_document_by_id(&pool, outcome.source_document_id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(doc.kind, NARRATIVE_PDF_KIND);
        assert_eq!(doc.document_date.as_deref(), Some("2026-04-21"));
    }

    #[tokio::test]
    async fn no_extractor_fails_the_ingest_with_nothing_persisted() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let err = ingest_narrative_pdf(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(FIXTURE),
            NarrativeIngestParams {
                source: "manual-upload",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
            },
            None::<&crate::extraction::ClaudeExtractor>,
        )
        .await
        .expect_err("missing extractor must fail the ingest");
        assert!(matches!(err, Error::ExtractorNotConfigured));
        assert!(
            err.to_string().contains("ANTHROPIC_API_KEY"),
            "error must tell the caller how to fix it: {err}"
        );
        // Nothing persisted anywhere: the caller was told the ingest failed,
        // so no text-only residue may exist to diverge from that answer.
        assert!(archive.list_keys().await.expect("keys").is_empty());
        assert!(derived.list_keys().await.expect("keys").is_empty());
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM source_documents")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(count.0, 0, "nothing indexed");
    }

    #[tokio::test]
    async fn llm_failure_fails_the_ingest_with_nothing_persisted() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let err = ingest_narrative_pdf(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(FIXTURE),
            NarrativeIngestParams {
                source: "manual-upload",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
            },
            Some(&FailingExtractor),
        )
        .await
        .expect_err("LLM failure must fail the ingest");
        assert!(matches!(err, Error::Extraction(_)));
        assert!(err.to_string().contains("outage"), "{err}");
        // Nothing persisted anywhere: a failed ingest leaves no trace, so a
        // later retry of the same ingest starts clean (and rebuild cannot
        // resurrect a document the caller was told failed).
        assert!(archive.list_keys().await.expect("keys").is_empty());
        assert!(derived.list_keys().await.expect("keys").is_empty());
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM source_documents")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(count.0, 0, "nothing indexed");
    }

    #[tokio::test]
    async fn llm_failure_on_reingest_leaves_prior_verified_state_untouched() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockExtractor(fixture_extraction());
        let first = ingest_narrative_pdf(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(FIXTURE),
            NarrativeIngestParams {
                source: "manual-upload",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
            },
            Some(&extractor),
        )
        .await
        .expect("first ingest");

        // Re-ingest the same bytes during an LLM outage: the ingest fails,
        // and the prior verified state must survive intact.
        ingest_narrative_pdf(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(FIXTURE),
            NarrativeIngestParams {
                source: "manual-upload",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
            },
            Some(&FailingExtractor),
        )
        .await
        .expect_err("LLM failure must fail the re-ingest");

        let problems = list_problems_by_source_document(&pool, first.source_document_id)
            .await
            .expect("problems");
        assert_eq!(problems.len(), 1, "prior problems row must survive");
        let doc = crate::index::get_source_document_by_id(&pool, first.source_document_id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(doc.document_date.as_deref(), Some("2026-04-21"));
    }

    #[tokio::test]
    async fn re_ingest_of_same_pdf_upserts_without_duplicates() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockExtractor(fixture_extraction());
        let first = ingest_narrative_pdf(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(FIXTURE),
            NarrativeIngestParams {
                source: "manual-upload",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
            },
            Some(&extractor),
        )
        .await
        .expect("first");
        let second = ingest_narrative_pdf(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(FIXTURE),
            NarrativeIngestParams {
                source: "manual-upload",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
            },
            Some(&extractor),
        )
        .await
        .expect("second");
        // `source_documents.id` is a bare SQLite rowid (INTEGER PRIMARY KEY,
        // no AUTOINCREMENT): deleting the sole prior row before inserting its
        // replacement can make SQLite reuse the same rowid, so id equality
        // or inequality here is an incidental storage detail, not a
        // correctness property. What matters is that each ingest applied its
        // extraction to whichever row currently backs the archive key, and
        // that no rows accumulate (checked below).
        assert_eq!(first.codings_indexed, 1);
        assert_eq!(second.codings_indexed, 1);

        let doc_count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM source_documents WHERE kind = 'clinical-pdf'")
                .fetch_one(&pool)
                .await
                .expect("count");
        assert_eq!(doc_count.0, 1, "same bytes must not duplicate");
        let prob_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM problems")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(prob_count.0, 1, "problems must not accumulate");
    }

    #[tokio::test]
    async fn no_extractor_on_reingest_fails_and_leaves_prior_state_untouched() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockExtractor(fixture_extraction());

        // First ingest: verified extraction applies cleanly.
        let first = ingest_narrative_pdf(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(FIXTURE),
            NarrativeIngestParams {
                source: "manual-upload",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
            },
            Some(&extractor),
        )
        .await
        .expect("first ingest");
        assert_eq!(first.codings_indexed, 1);

        // Re-ingest the same bytes with no extractor available (e.g. no
        // ANTHROPIC_API_KEY). The ingest fails, and the prior verified
        // state — problems, title, document date — must survive intact.
        let err = ingest_narrative_pdf(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(FIXTURE),
            NarrativeIngestParams {
                source: "manual-upload",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
            },
            None::<&crate::extraction::ClaudeExtractor>,
        )
        .await
        .expect_err("missing extractor must fail the re-ingest");
        assert!(matches!(err, Error::ExtractorNotConfigured));

        // Prior problems row survived.
        let problems = list_problems_by_source_document(&pool, first.source_document_id)
            .await
            .expect("problems");
        assert_eq!(problems.len(), 1, "prior problems row must survive");
        assert_eq!(problems[0].coding_code, "R10.9");

        // Prior narrative title survived.
        let text = crate::index::get_narrative_text(&pool, first.source_document_id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(
            text.title.as_deref(),
            Some("GI Pathology Report — colon biopsy"),
            "prior narrative title must survive"
        );

        // Prior document_date survived.
        let doc = crate::index::get_source_document_by_id(&pool, first.source_document_id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(
            doc.document_date.as_deref(),
            Some("2026-04-21"),
            "prior document_date must survive"
        );
    }

    #[tokio::test]
    async fn non_pdf_bytes_fail_before_archiving() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let err = ingest_narrative_pdf(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(b"plain text, not a pdf"),
            NarrativeIngestParams {
                source: "manual-upload",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
            },
            None::<&crate::extraction::ClaudeExtractor>,
        )
        .await
        .expect_err("must fail");
        assert!(matches!(err, Error::Extraction(_)));
        assert!(archive.list_keys().await.expect("keys").is_empty());
        assert!(derived.list_keys().await.expect("keys").is_empty());
    }

    #[tokio::test]
    async fn replay_pdf_rebuilds_document_and_text_from_a_bare_blob() {
        // Direct unit coverage of the function itself; rebuild.rs has its own
        // end-to-end test that drives this through rebuild_index.
        let (pool, archive, _derived) = fresh_pool_and_stores().await;
        let content = Bytes::from_static(FIXTURE);
        let archived_at = OffsetDateTime::now_utc();
        let manifest = Manifest::new(
            "manual-upload",
            NARRATIVE_PDF_KIND,
            "application/pdf",
            Some("2026-04-21".to_owned()),
            archived_at,
            Some("synthetic_pathology.pdf".to_owned()),
        );
        let key = archive
            .put_with_manifest(content.clone(), manifest.clone())
            .await
            .expect("put_with_manifest");

        let source_document_id = replay_pdf(&pool, &key, &content, &manifest)
            .await
            .expect("replay_pdf");

        let doc = crate::index::get_source_document_by_id(&pool, source_document_id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(doc.kind, NARRATIVE_PDF_KIND);
        assert_eq!(doc.source, "manual-upload");
        assert_eq!(doc.document_date.as_deref(), Some("2026-04-21"));

        let text = crate::index::get_narrative_text(&pool, source_document_id)
            .await
            .expect("get")
            .expect("present");
        assert!(text.text.contains("DIAGNOSIS"));
        assert_eq!(text.title, None);
    }
}
