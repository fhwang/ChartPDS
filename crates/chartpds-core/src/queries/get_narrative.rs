//! Full narrative document read: metadata + extracted text + codings.
//!
//! "Codings" spans both tables a narrative can produce. A clinical PDF
//! quotes its codes, which land in `problems`. A journal entry states no
//! codes at all — an LLM infers them from prose and they land in
//! `observations` with `derivation = 'inferred'`. Reading only one table
//! made a journal entry look uncoded, which is exactly backwards: an
//! inferred code is the one most in need of a reader.

use sqlx::SqlitePool;

use crate::index::{
    get_narrative_text, get_source_document_by_id, list_observations_by_source_document,
    list_problems_by_source_document,
};

/// One coding extracted from this narrative.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NarrativeCoding {
    /// Coding system URI.
    pub coding_system: String,
    /// Code within the system.
    pub coding_code: String,
    /// Display text paired with the code in the document.
    pub coding_display: Option<String>,
    /// Verbatim section heading the code appeared under. Always `None` for
    /// an inferred coding: colloquial prose has no sections.
    pub section_label: Option<String>,
    /// How the claim was derived from the document: `"structured"`,
    /// `"verbatim"`, or `"inferred"` (see the `derivation` migration).
    pub derivation: String,
    /// The verbatim span the claim was derived from, filled in by the caller
    /// from [`crate::provenance::grounding_quotes`]. `None` until then, and
    /// for a coding whose document never had an extraction artifact.
    pub grounding_quote: Option<String>,
}

/// A narrative document with its full text and extracted codings.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NarrativeDetail {
    /// The `source_documents.id`.
    pub source_document_id: i64,
    /// Document kind.
    pub kind: String,
    /// Ingest source.
    pub source: String,
    /// Extractor-authored title, if any.
    pub title: Option<String>,
    /// Document date, if known.
    pub document_date: Option<String>,
    /// Original upload filename, if known.
    pub original_filename: Option<String>,
    /// Full extracted document text.
    pub text: String,
    /// Content-addressed key of the archived bytes this row indexes — the
    /// handle the derived store's artifacts name as their subject.
    pub archive_key: String,
    /// Codings extracted (and verified) from this document: quoted ones from
    /// `problems`, inferred ones from `observations`.
    pub codings: Vec<NarrativeCoding>,
}

/// Fetch a narrative by `source_documents.id`.
///
/// Returns `None` when the id does not exist or is not a narrative (has no
/// `narrative_texts` row).
///
/// # Errors
///
/// Returns `sqlx::Error` if a query fails.
pub async fn get_narrative(
    pool: &SqlitePool,
    source_document_id: i64,
) -> Result<Option<NarrativeDetail>, sqlx::Error> {
    let Some(doc) = get_source_document_by_id(pool, source_document_id).await? else {
        return Ok(None);
    };
    let Some(nt) = get_narrative_text(pool, source_document_id).await? else {
        return Ok(None);
    };
    let mut codings: Vec<NarrativeCoding> =
        list_problems_by_source_document(pool, source_document_id)
            .await?
            .into_iter()
            .map(|p| NarrativeCoding {
                coding_system: p.coding_system,
                coding_code: p.coding_code,
                coding_display: p.coding_display,
                section_label: p.section_label,
                derivation: p.derivation,
                grounding_quote: None,
            })
            .collect();
    codings.extend(
        list_observations_by_source_document(pool, source_document_id)
            .await?
            .into_iter()
            .map(|o| NarrativeCoding {
                coding_system: o.coding_system,
                coding_code: o.coding_code,
                coding_display: o.coding_display,
                section_label: None,
                derivation: o.derivation,
                grounding_quote: None,
            }),
    );
    Ok(Some(NarrativeDetail {
        source_document_id,
        kind: doc.kind,
        source: doc.source,
        title: nt.title,
        document_date: doc.document_date,
        original_filename: doc.original_filename,
        text: nt.text,
        archive_key: doc.archive_key.to_string(),
        codings,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::BlobKey;
    use crate::index::{
        insert_observation, insert_problem, insert_source_document, open_pool,
        upsert_narrative_text, InsertObservationParams, InsertProblemParams,
        InsertSourceDocumentParams, UpsertNarrativeTextParams,
    };
    use time::macros::datetime;
    use time::OffsetDateTime;

    #[tokio::test]
    async fn returns_metadata_text_and_codings() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");

        let key = BlobKey::from_hex_str(
            "5555555555555555555555555555555555555555555555555555555555555555",
        )
        .expect("key");
        let id = insert_source_document(
            &pool,
            InsertSourceDocumentParams {
                archive_key: &key,
                kind: "clinical-pdf",
                source: "manual-upload",
                original_filename: Some("report.pdf"),
                archived_at: OffsetDateTime::now_utc(),
                document_date: Some("2026-04-21"),
            },
        )
        .await
        .expect("doc");
        upsert_narrative_text(
            &pool,
            UpsertNarrativeTextParams {
                source_document_id: id,
                title: Some("GI Pathology Report"),
                text: "full document text here",
            },
        )
        .await
        .expect("text");
        insert_problem(
            &pool,
            InsertProblemParams {
                source_document_id: id,
                coding_system: "http://hl7.org/fhir/sid/icd-10-cm",
                coding_code: "R10.9",
                coding_display: Some("Abdominal pain, unspecified"),
                status: "unknown",
                onset_date: Some("2026-04-21"),
                section_label: Some("Pre-Op Diagnosis/Indications"),
                derivation: "structured",
            },
        )
        .await
        .expect("problem");

        let detail = get_narrative(&pool, id)
            .await
            .expect("query")
            .expect("present");
        assert_eq!(detail.title.as_deref(), Some("GI Pathology Report"));
        assert_eq!(detail.text, "full document text here");
        assert_eq!(detail.codings.len(), 1);
        assert_eq!(detail.codings[0].coding_code, "R10.9");
        assert_eq!(
            detail.codings[0].section_label.as_deref(),
            Some("Pre-Op Diagnosis/Indications")
        );

        assert!(get_narrative(&pool, id + 999)
            .await
            .expect("query")
            .is_none());
    }

    #[tokio::test]
    async fn returns_a_journal_entrys_inferred_codings() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");

        let key = BlobKey::from_hex_str(
            "6666666666666666666666666666666666666666666666666666666666666666",
        )
        .expect("key");
        let id = insert_source_document(
            &pool,
            InsertSourceDocumentParams {
                archive_key: &key,
                kind: "journal",
                source: "manual-upload",
                original_filename: Some("2026-09-15-journal.md"),
                archived_at: OffsetDateTime::now_utc(),
                document_date: Some("2026-09-15"),
            },
        )
        .await
        .expect("doc");
        upsert_narrative_text(
            &pool,
            UpsertNarrativeTextParams {
                source_document_id: id,
                title: Some("Journal"),
                text: "My left knee hurts a little, and I have a mild limp.",
            },
        )
        .await
        .expect("text");
        insert_observation(
            &pool,
            InsertObservationParams {
                source_document_id: id,
                coding_system: "http://hl7.org/fhir/sid/icd-10-cm",
                coding_code: "R26.0",
                coding_display: Some("Ataxic gait"),
                effective_start: datetime!(2026-09-15 00:00:00 UTC),
                effective_end: None,
                value_quantity: None,
                value_string: None,
                value_unit: None,
                derivation: "inferred",
            },
        )
        .await
        .expect("observation");

        let detail = get_narrative(&pool, id)
            .await
            .expect("query")
            .expect("present");
        assert_eq!(detail.archive_key, key.to_string());
        assert_eq!(detail.codings.len(), 1);
        assert_eq!(detail.codings[0].coding_code, "R26.0");
        assert_eq!(detail.codings[0].derivation, "inferred");
        assert_eq!(detail.codings[0].section_label, None);
        assert_eq!(detail.codings[0].grounding_quote, None);
    }
}
