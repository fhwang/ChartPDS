//! Full narrative document read: metadata + extracted text + codings.

use sqlx::SqlitePool;

use crate::index::{
    get_narrative_text, get_source_document_by_id, list_problems_by_source_document,
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
    /// Verbatim section heading the code appeared under.
    pub section_label: Option<String>,
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
    /// Codings extracted (and verified) from this document.
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
    let codings = list_problems_by_source_document(pool, source_document_id)
        .await?
        .into_iter()
        .map(|p| NarrativeCoding {
            coding_system: p.coding_system,
            coding_code: p.coding_code,
            coding_display: p.coding_display,
            section_label: p.section_label,
        })
        .collect();
    Ok(Some(NarrativeDetail {
        source_document_id,
        kind: doc.kind,
        source: doc.source,
        title: nt.title,
        document_date: doc.document_date,
        original_filename: doc.original_filename,
        text: nt.text,
        codings,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::BlobKey;
    use crate::index::{
        insert_problem, insert_source_document, open_pool, upsert_narrative_text,
        InsertProblemParams, InsertSourceDocumentParams, UpsertNarrativeTextParams,
    };
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
}
