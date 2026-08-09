//! Journal-entry ingestion: free colloquial `.md` text → one-time verified
//! LLM inference of ICD-10-CM codings (frozen as an artifact in the derived
//! store) → `observations` rows marked `derivation = 'inferred'`.
//!
//! Entry-date resolution is deterministic-first (a `YYYY-MM-DD` in the
//! filename, else a single dated markdown header, a year-less header
//! borrowing its year from the filename), falling back to verified LLM
//! date extraction. A file with two or more dated headers is a multi-entry
//! file and is rejected; an entry no path can date is rejected — see
//! [`crate::ingestion::Error::MultiEntryJournal`] and
//! [`crate::ingestion::Error::UndatedJournal`].

use bytes::Bytes;
use sqlx::SqlitePool;
use time::{Date, Month};

use super::narrative::NarrativeIngestParams;
use crate::archive::{Archive, BlobKey, Manifest};
use crate::extraction::{
    verify_journal_extraction, ExtractorInfo, JournalCoding, JournalExtractionArtifact,
    JournalExtractor, VerifiedJournalExtraction, EXTRACTION_MODEL, JOURNAL_PROMPT_VERSION,
};
use crate::index::{
    delete_source_document, fetch_source_document_by_archive_key, insert_observation,
    insert_source_document, set_narrative_title, set_source_document_date, upsert_narrative_text,
    InsertObservationParams, InsertSourceDocumentParams, UpsertNarrativeTextParams,
};
use crate::ingestion::{Error, Result};

/// Find a `YYYY-MM-DD` substring and parse it as a date.
fn iso_date_in(s: &str) -> Option<Date> {
    let b = s.as_bytes();
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    for i in 0..b.len().saturating_sub(9) {
        let w = &b[i..i + 10];
        let shaped = w.iter().enumerate().all(|(j, c)| {
            if matches!(j, 4 | 7) {
                *c == b'-'
            } else {
                c.is_ascii_digit()
            }
        });
        if shaped {
            // All-ASCII window, so the slice is on char boundaries.
            if let Ok(d) = Date::parse(&s[i..i + 10], &fmt) {
                return Some(d);
            }
        }
    }
    None
}

/// Find a standalone plausible 4-digit year (1900–2100).
fn year_in(s: &str) -> Option<i32> {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i].is_ascii_digit() {
            let start = i;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
            if i - start == 4 {
                if let Ok(y) = s[start..i].parse::<i32>() {
                    if (1900..=2100).contains(&y) {
                        return Some(y);
                    }
                }
            }
        } else {
            i += 1;
        }
    }
    None
}

/// Month number for an English month name or 3-letter abbreviation.
fn month_from_name(token: &str) -> Option<Month> {
    const MONTHS: [&str; 12] = [
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ];
    let lower = token.to_ascii_lowercase();
    MONTHS
        .iter()
        .position(|m| *m == lower || (lower.len() == 3 && m.starts_with(&lower)))
        .and_then(|idx| Month::try_from(u8::try_from(idx + 1).ok()?).ok())
}

/// Parse one markdown-header's content as a date. Accepts ISO anywhere in
/// the header, `Month D, YYYY` / `Mon D, YYYY`, `M/D/YYYY`, and year-less
/// `Month D` / `Mon D` completed by `filename_year`.
fn parse_header_date(content: &str, filename_year: Option<i32>) -> Option<Date> {
    if let Some(d) = iso_date_in(content) {
        return Some(d);
    }
    let tokens: Vec<&str> = content
        .split(|c: char| c.is_whitespace() || c == ',' || c == '/')
        .filter(|t| !t.is_empty())
        .collect();
    // Scan for a month-name token anywhere (headers may carry a prefix,
    // e.g. "Journal for Jul 26, 2026").
    for (i, tok) in tokens.iter().enumerate() {
        if let Some(month) = month_from_name(tok) {
            if let Some(Ok(day)) = tokens.get(i + 1).map(|t| t.parse::<u8>()) {
                let year = tokens
                    .get(i + 2)
                    .and_then(|t| t.parse::<i32>().ok())
                    .filter(|y| (1900..=2100).contains(y))
                    .or(filename_year);
                if let Some(y) = year {
                    if let Ok(d) = Date::from_calendar_date(y, month, day) {
                        return Some(d);
                    }
                }
            }
        }
    }
    // Numeric M/D/YYYY (the '/' split above turned it into three tokens).
    if let [m, d, y] = tokens.as_slice() {
        if let (Ok(m), Ok(d), Ok(y)) = (m.parse::<u8>(), d.parse::<u8>(), y.parse::<i32>()) {
            if (1900..=2100).contains(&y) {
                if let Ok(month) = Month::try_from(m) {
                    if let Ok(date) = Date::from_calendar_date(y, month, d) {
                        return Some(date);
                    }
                }
            }
        }
    }
    None
}

/// Deterministically resolve one journal entry's date from its filename and
/// markdown headers.
///
/// # Errors
///
/// Returns [`Error::MultiEntryJournal`] when two or more headers parse as
/// dates (a multi-entry file).
pub(crate) fn resolve_entry_date(
    original_filename: Option<&str>,
    text: &str,
) -> Result<Option<Date>> {
    let filename_year = original_filename.and_then(year_in);
    let header_dates: Vec<Date> = text
        .lines()
        .map(str::trim_start)
        .filter(|l| l.starts_with('#'))
        .filter_map(|l| parse_header_date(l.trim_start_matches('#').trim(), filename_year))
        .collect();
    if header_dates.len() >= 2 {
        return Err(Error::MultiEntryJournal);
    }
    if let Some(d) = original_filename.and_then(iso_date_in) {
        return Ok(Some(d));
    }
    Ok(header_dates.into_iter().next())
}

/// `source_documents.kind` / manifest `type` for a journal-entry blob.
pub const JOURNAL_KIND: &str = "journal";
/// Manifest `type` for the frozen journal extraction artifact blob.
pub const JOURNAL_EXTRACTION_KIND: &str = "journal-extraction";

/// What `ingest_journal` did, reported in-band to the tool caller.
///
/// `codings` carries each verified coding with its grounding quote so the
/// driving agent can echo the mappings back to the author — the cheapest
/// audit of an inferred code is the author reading it at ingest time.
#[derive(Debug, Clone, serde::Serialize)]
pub struct JournalIngestOutcome {
    /// The `source_documents.id` of the ingested entry.
    pub source_document_id: i64,
    /// Extractor-authored title.
    pub title: Option<String>,
    /// The entry's resolved calendar date (ISO-8601).
    pub entry_date: String,
    /// Verified codings, with quotes and any stated severity.
    pub codings: Vec<JournalCoding>,
    /// Human-readable reasons for claims dropped by verification (both
    /// attempts, when the invalid-code retry ran).
    pub rejected: Vec<String>,
}

/// Ingest one journal entry (markdown or plain text, UTF-8).
///
/// Steps: decode UTF-8 (fail fast) → resolve a deterministic entry date
/// (filename, then a single dated header) → LLM inference + mechanical
/// verification, with one corrective retry when codes fail the ICD-10-CM
/// table → finalize the date (deterministic wins; verified LLM date as
/// fallback, strict-parsed as a canonical `YYYY-MM-DD` calendar date —
/// verification only range-checks month/day, so an unparseable or
/// non-canonical LLM date fails the ingest here rather than persisting) →
/// archive the text blob (manifest `subject` = entry date) → freeze the
/// verified extraction in the derived store → upsert index rows (document,
/// narrative text, inferred observations).
///
/// LLM extraction is required, exactly as for narrative PDFs: no extractor
/// or an exhausted-retries failure fails the whole ingest before anything
/// is archived or indexed. Zero verified codings is NOT a failure — a
/// good-day entry has nothing to code and still earns FTS retrieval.
///
/// # Errors
///
/// [`Error::JournalNotUtf8`], [`Error::MultiEntryJournal`],
/// [`Error::UndatedJournal`], [`Error::ExtractorNotConfigured`],
/// [`Error::Extraction`] — all before anything persists — plus
/// [`Error::Archive`]/[`Error::Database`] on storage failures.
pub async fn ingest_journal<E: JournalExtractor>(
    archive: &Archive,
    derived: &Archive,
    pool: &SqlitePool,
    content: Bytes,
    params: NarrativeIngestParams<'_>,
    extractor: Option<&E>,
) -> Result<JournalIngestOutcome> {
    let NarrativeIngestParams {
        source,
        original_filename,
        archived_at,
    } = params;

    // 1. Decode. Journal entries are text by definition.
    let text = std::str::from_utf8(&content).map_err(|_| Error::JournalNotUtf8)?;

    // 2. Deterministic date first (may reject a multi-entry file).
    let deterministic_date = resolve_entry_date(original_filename, text)?;

    // 3. LLM inference + verification, with one corrective retry when the
    //    model proposed codes that are not in the ICD-10-CM table.
    let Some(extractor) = extractor else {
        return Err(Error::ExtractorNotConfigured);
    };
    let verified = extract_and_verify(extractor, text).await?;

    // 4. Final date: deterministic wins; verified LLM date is the fallback,
    //    but only after a strict canonical-ISO parse — `verify_journal_extraction`
    //    only range-checks month/day (so "2026-02-30" or "2026-7-6" can pass
    //    verification) and this is the last chance to reject that before any
    //    blob, artifact, or index row is written. A malformed date here must
    //    never persist: it would freeze a poison artifact that fails on
    //    every future `rebuild_index`.
    let entry_date = if let Some(d) = deterministic_date {
        format_iso_date(d)
    } else {
        let claimed = verified.entry_date.clone().ok_or(Error::UndatedJournal)?;
        let fmt = time::macros::format_description!("[year]-[month]-[day]");
        let d = Date::parse(&claimed, &fmt).map_err(|_| Error::UndatedJournal)?;
        format_iso_date(d)
    };

    // 5-6. Archive the text blob and freeze the artifact. `content` is
    //      cloned (a cheap refcount bump) because `text` still borrows the
    //      original buffer for the FTS upsert in step 8.
    let (key, artifact) = archive_journal_blobs(
        archive,
        derived,
        content.clone(),
        params,
        &entry_date,
        &verified,
    )
    .await?;

    // 7. Upsert the document row (re-ingest of the same bytes replaces the
    //    prior rows; cascade cleans narrative_texts + observations and the
    //    FTS delete trigger fires on the cascade).
    if let Some(existing) = fetch_source_document_by_archive_key(pool, &key).await? {
        delete_source_document(pool, existing.id).await?;
    }
    let source_document_id = insert_source_document(
        pool,
        InsertSourceDocumentParams {
            archive_key: &key,
            kind: JOURNAL_KIND,
            source,
            original_filename,
            archived_at,
            document_date: None, // applied from the artifact below
        },
    )
    .await?;

    // 8. Index the text (FTS via triggers).
    upsert_narrative_text(
        pool,
        UpsertNarrativeTextParams {
            source_document_id,
            title: None, // applied from the artifact below
            text,
        },
    )
    .await?;

    // 9. Apply the artifact (date, title, inferred observations).
    apply_journal_extraction(pool, source_document_id, &artifact).await?;

    Ok(JournalIngestOutcome {
        source_document_id,
        title: artifact.title,
        entry_date,
        codings: artifact.codings,
        rejected: verified.rejected,
    })
}

/// ISO-8601 (`YYYY-MM-DD`) rendering of a date.
fn format_iso_date(d: Date) -> String {
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    d.format(&fmt).unwrap_or_else(|_| d.to_string())
}

/// LLM inference + verification with at most one corrective retry, fired
/// only when the first attempt proposed codes missing from the ICD-10-CM
/// table. The retry's verification result wins; the first attempt's
/// rejection reasons are prepended (marked) so nothing dropped goes
/// unreported.
async fn extract_and_verify<E: JournalExtractor>(
    extractor: &E,
    text: &str,
) -> Result<VerifiedJournalExtraction> {
    let first = verify_journal_extraction(text, extractor.extract_journal(text, None).await?);
    if first.invalid_codes.is_empty() {
        return Ok(first);
    }
    let feedback = first
        .rejected
        .iter()
        .filter(|r| r.contains("not a valid ICD-10-CM code"))
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    let mut second = verify_journal_extraction(
        text,
        extractor.extract_journal(text, Some(&feedback)).await?,
    );
    let mut rejected: Vec<String> = first
        .rejected
        .into_iter()
        .map(|r| format!("(first attempt) {r}"))
        .collect();
    rejected.append(&mut second.rejected);
    second.rejected = rejected;
    Ok(second)
}

/// Archive the journal text blob and freeze its verified extraction as an
/// artifact blob in the derived store (same two-store split as
/// `archive_narrative_blobs` in `narrative.rs`).
///
/// Takes `params` (not its individual fields) to stay under the
/// too-many-arguments lint — `NarrativeIngestParams` is `Copy`, so the
/// caller keeps its own copy for the index-row inserts that follow.
async fn archive_journal_blobs(
    archive: &Archive,
    derived: &Archive,
    content: Bytes,
    params: NarrativeIngestParams<'_>,
    entry_date: &str,
    verified: &VerifiedJournalExtraction,
) -> Result<(BlobKey, JournalExtractionArtifact)> {
    let NarrativeIngestParams {
        source,
        original_filename,
        archived_at,
    } = params;

    let manifest = Manifest::new(
        source,
        JOURNAL_KIND,
        "text/markdown",
        Some(entry_date.to_owned()),
        archived_at,
        original_filename.map(str::to_owned),
    );
    let key = archive.put_with_manifest(content, manifest).await?;

    let artifact = JournalExtractionArtifact {
        document: key.to_string(),
        entry_date: entry_date.to_owned(),
        title: verified.title.clone(),
        codings: verified.codings.clone(),
        extractor: ExtractorInfo {
            model: EXTRACTION_MODEL.to_owned(),
            prompt_version: JOURNAL_PROMPT_VERSION,
        },
        extracted_at: archived_at,
    };
    let bytes = serde_json::to_vec(&artifact).map_err(|err| {
        Error::Extraction(crate::extraction::Error::InvalidResponse {
            reason: format!("serializing journal artifact: {err}"),
        })
    })?;
    let artifact_manifest = Manifest::new(
        "chartpds",
        JOURNAL_EXTRACTION_KIND,
        "application/json",
        Some(key.to_string()),
        archived_at,
        None,
    );
    derived
        .put_with_manifest(Bytes::from(bytes), artifact_manifest)
        .await?;

    Ok((key, artifact))
}

/// Apply a frozen journal extraction artifact to an indexed journal
/// document: set the document date and title, insert one inferred
/// `observations` row per coding (entry-date midnight UTC, severity as
/// `value_quantity`, `derivation = 'inferred'`).
///
/// Shared by live ingestion and `rebuild_index` — the ONLY code path that
/// turns a journal artifact into index rows.
pub(crate) async fn apply_journal_extraction(
    pool: &SqlitePool,
    source_document_id: i64,
    artifact: &JournalExtractionArtifact,
) -> Result<u64> {
    set_source_document_date(pool, source_document_id, &artifact.entry_date).await?;
    if let Some(title) = &artifact.title {
        set_narrative_title(pool, source_document_id, title).await?;
    }
    let fmt = time::macros::format_description!("[year]-[month]-[day]");
    let date = Date::parse(&artifact.entry_date, &fmt).map_err(|err| {
        Error::Extraction(crate::extraction::Error::InvalidResponse {
            reason: format!(
                "journal artifact entry_date {:?} unparseable: {err}",
                artifact.entry_date
            ),
        })
    })?;
    let effective_start = date.midnight().assume_utc();
    let mut count = 0u64;
    for c in &artifact.codings {
        insert_observation(
            pool,
            InsertObservationParams {
                source_document_id,
                coding_system: &c.system,
                coding_code: &c.code,
                coding_display: Some(&c.display),
                effective_start,
                effective_end: None,
                value_quantity: c.severity,
                value_string: None,
                value_unit: None,
                derivation: "inferred",
            },
        )
        .await?;
        count += 1;
    }
    Ok(count)
}

/// Text-only replay of an archived journal blob during rebuild. The
/// document date comes from the manifest `subject` (re-applied by the
/// artifact pass when an artifact exists). Returns the new
/// `source_documents.id`.
pub(crate) async fn replay_journal(
    pool: &SqlitePool,
    key: &BlobKey,
    content: &Bytes,
    manifest: &Manifest,
) -> Result<i64> {
    let text = std::str::from_utf8(content).map_err(|_| Error::JournalNotUtf8)?;
    let source_document_id = insert_source_document(
        pool,
        InsertSourceDocumentParams {
            archive_key: key,
            kind: JOURNAL_KIND,
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
            text,
        },
    )
    .await?;
    Ok(source_document_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::{Archive, Manifest};
    use crate::extraction::{JournalExtractor, RawJournalCoding, RawJournalExtraction};
    use crate::index::{list_observations_by_source_document, open_pool};
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use std::sync::{Arc, Mutex};
    use time::macros::date;

    #[test]
    fn filename_iso_date_wins() {
        let d = resolve_entry_date(Some("2026-07-26.md"), "no dates in text").expect("resolve");
        assert_eq!(d, Some(date!(2026 - 07 - 26)));
    }

    #[test]
    fn header_full_date_forms_parse() {
        for text in [
            "# 2026-07-26\n\nbody",
            "# Jul 26, 2026\n\nbody",
            "# July 26, 2026\n\nbody",
            "# 7/26/2026\n\nbody",
            "## Journal for Jul 26, 2026\n\nbody",
        ] {
            let d = resolve_entry_date(None, text).expect("resolve");
            assert_eq!(d, Some(date!(2026 - 07 - 26)), "failed for {text:?}");
        }
    }

    #[test]
    fn yearless_header_takes_year_from_filename() {
        let d = resolve_entry_date(Some("journal-2026.md"), "# Jul 26\n\nbody").expect("resolve");
        assert_eq!(d, Some(date!(2026 - 07 - 26)));
    }

    #[test]
    fn yearless_header_without_filename_year_is_not_deterministic() {
        let d = resolve_entry_date(None, "# Jul 26\n\nbody").expect("resolve");
        assert_eq!(d, None);
    }

    #[test]
    fn multiple_dated_headers_reject_as_multi_entry() {
        let err = resolve_entry_date(None, "# Jul 26, 2026\n\nfoo\n\n# Jul 27, 2026\n\nbar")
            .expect_err("multi-entry must reject");
        assert!(matches!(err, Error::MultiEntryJournal));
    }

    #[test]
    fn undated_headerless_text_is_not_deterministic() {
        let d = resolve_entry_date(None, "Shoulder still sore today.").expect("resolve");
        assert_eq!(d, None);
    }

    #[test]
    fn non_date_headers_are_ignored() {
        let d = resolve_entry_date(None, "# Morning notes\n\n# Evening notes\n\nbody")
            .expect("resolve");
        assert_eq!(d, None);
    }

    const ENTRY: &str = "# Jul 26, 2026\n\nLeft shoulder aching again after climbing. \
Pain was maybe a 6 today. Slept badly, kept waking up.\n";

    async fn fresh_pool_and_stores() -> (sqlx::SqlitePool, Archive, Archive) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        let derived_backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        (pool, Archive::new(backend), Archive::new(derived_backend))
    }

    /// Scripted journal extractor: pops canned responses in order and
    /// records the feedback each call received.
    struct MockJournalExtractor {
        responses: Mutex<Vec<RawJournalExtraction>>,
        feedback_seen: Mutex<Vec<Option<String>>>,
    }

    impl MockJournalExtractor {
        fn new(responses: Vec<RawJournalExtraction>) -> Self {
            Self {
                responses: Mutex::new(responses),
                feedback_seen: Mutex::new(Vec::new()),
            }
        }
    }

    impl JournalExtractor for MockJournalExtractor {
        async fn extract_journal(
            &self,
            _text: &str,
            feedback: Option<&str>,
        ) -> std::result::Result<RawJournalExtraction, crate::extraction::Error> {
            self.feedback_seen
                .lock()
                .expect("lock")
                .push(feedback.map(str::to_owned));
            Ok(self.responses.lock().expect("lock").remove(0))
        }
    }

    fn shoulder_extraction(code: &str) -> RawJournalExtraction {
        RawJournalExtraction {
            entry_date: None,
            entry_date_quote: None,
            title: Some("Journal — shoulder ache".to_owned()),
            codings: vec![RawJournalCoding {
                code: code.to_owned(),
                display: "Pain in left shoulder".to_owned(),
                quote: "Left shoulder aching again after climbing. Pain was maybe a 6 today."
                    .to_owned(),
                severity: Some(6.0),
            }],
        }
    }

    fn params(filename: Option<&'static str>) -> crate::ingestion::NarrativeIngestParams<'static> {
        crate::ingestion::NarrativeIngestParams {
            source: "journal",
            original_filename: filename,
            archived_at: time::macros::datetime!(2026-07-26 21:00:00 UTC),
        }
    }

    #[tokio::test]
    async fn ingests_entry_into_inferred_observations() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockJournalExtractor::new(vec![shoulder_extraction("M25.512")]);

        let outcome = ingest_journal(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(ENTRY.as_bytes()),
            params(Some("2026-07-26.md")),
            Some(&extractor),
        )
        .await
        .expect("ingest");

        assert_eq!(outcome.entry_date, "2026-07-26");
        assert_eq!(outcome.codings.len(), 1);
        assert_eq!(outcome.codings[0].code, "M25.512");
        assert_eq!(outcome.codings[0].severity, Some(6.0));
        assert!(
            !outcome.codings[0].quote.is_empty(),
            "outcome must echo the grounding quote for the author to audit"
        );

        // One blob per store.
        assert_eq!(archive.list_keys().await.expect("keys").len(), 1);
        assert_eq!(derived.list_keys().await.expect("keys").len(), 1);

        // The observation row: inferred, dated, severity as value_quantity.
        let obs = list_observations_by_source_document(&pool, outcome.source_document_id)
            .await
            .expect("observations");
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].coding_system, crate::extraction::ICD10_CM_SYSTEM);
        assert_eq!(obs[0].coding_code, "M25.512");
        assert_eq!(obs[0].derivation, "inferred");
        assert_eq!(obs[0].value_quantity, Some(6.0));
        assert_eq!(
            obs[0].effective_start,
            time::macros::datetime!(2026-07-26 00:00:00 UTC)
        );
        assert_eq!(obs[0].effective_end, None);

        // No problems rows — journal complaints stay out of the problem list.
        let problems =
            crate::index::list_problems_by_source_document(&pool, outcome.source_document_id)
                .await
                .expect("problems");
        assert!(problems.is_empty());

        // Document row and FTS text.
        let doc = crate::index::get_source_document_by_id(&pool, outcome.source_document_id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(doc.kind, JOURNAL_KIND);
        assert_eq!(doc.source, "journal");
        assert_eq!(doc.document_date.as_deref(), Some("2026-07-26"));
        let fts: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM narrative_texts_fts WHERE narrative_texts_fts MATCH 'climbing'",
        )
        .fetch_one(&pool)
        .await
        .expect("fts");
        assert_eq!(fts.0, 1);
    }

    #[tokio::test]
    async fn invalid_code_triggers_one_feedback_retry() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockJournalExtractor::new(vec![
            shoulder_extraction("M25.5129"), // invalid: not in the table
            shoulder_extraction("M25.512"),  // corrected on retry
        ]);

        let outcome = ingest_journal(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(ENTRY.as_bytes()),
            params(None),
            Some(&extractor),
        )
        .await
        .expect("ingest");

        let feedback = extractor.feedback_seen.lock().expect("lock").clone();
        assert_eq!(feedback.len(), 2, "exactly one semantic retry");
        assert_eq!(feedback[0], None);
        assert!(
            feedback[1]
                .as_deref()
                .is_some_and(|f| f.contains("M25.5129")),
            "retry feedback names the invalid code: {feedback:?}"
        );
        assert_eq!(outcome.codings.len(), 1);
        assert_eq!(outcome.codings[0].code, "M25.512");
        assert!(
            outcome.rejected.iter().any(|r| r.contains("M25.5129")),
            "first-attempt rejection stays visible: {:?}",
            outcome.rejected
        );
    }

    #[tokio::test]
    async fn zero_codings_is_accepted_and_text_indexed() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockJournalExtractor::new(vec![RawJournalExtraction {
            entry_date: None,
            entry_date_quote: None,
            title: Some("Journal — good day".to_owned()),
            codings: vec![],
        }]);

        let text = "# Jul 27, 2026\n\nFelt great. Long run, slept well.\n";
        let outcome = ingest_journal(
            &archive,
            &derived,
            &pool,
            Bytes::from(text.as_bytes().to_vec()),
            params(None),
            Some(&extractor),
        )
        .await
        .expect("zero codings must not fail the ingest");
        assert!(outcome.codings.is_empty());
        let fts: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM narrative_texts_fts WHERE narrative_texts_fts MATCH 'run'",
        )
        .fetch_one(&pool)
        .await
        .expect("fts");
        assert_eq!(fts.0, 1);
    }

    #[tokio::test]
    async fn undated_entry_fails_with_nothing_persisted() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockJournalExtractor::new(vec![RawJournalExtraction {
            entry_date: None,
            entry_date_quote: None,
            title: None,
            codings: vec![],
        }]);

        let err = ingest_journal(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(b"Shoulder still sore today.\n"),
            params(None),
            Some(&extractor),
        )
        .await
        .expect_err("undated entry must fail");
        assert!(matches!(err, Error::UndatedJournal));
        assert!(err.to_string().contains("YYYY-MM-DD"), "actionable: {err}");
        assert!(archive.list_keys().await.expect("keys").is_empty());
        assert!(derived.list_keys().await.expect("keys").is_empty());
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM source_documents")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(count.0, 0);
    }

    #[tokio::test]
    async fn malformed_llm_date_that_verifies_still_fails_with_nothing_persisted() {
        // `verify_journal_extraction` (extraction/verify.rs `date_candidates`)
        // only range-checks month 1..=12 / day 1..=31 — it accepts a
        // non-zero-padded ISO string like "2026-7-6" as long as some
        // rendering of it is quoted in the text. Malformed dates must never
        // reach the archive: `apply_journal_extraction` strict-parses the
        // date and would otherwise fail AFTER the blobs and index rows are
        // written, permanently poisoning `rebuild_index` on the frozen
        // artifact. This test pins the fix: the strict parse happens before
        // the first write.
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockJournalExtractor::new(vec![RawJournalExtraction {
            entry_date: Some("2026-7-6".to_owned()),
            entry_date_quote: Some("7/6/2026".to_owned()),
            title: None,
            codings: vec![],
        }]);

        let text = "Saw the doctor on 7/6/2026, nothing serious.\n";
        let err = ingest_journal(
            &archive,
            &derived,
            &pool,
            Bytes::from(text.as_bytes().to_vec()),
            params(None), // no filename date, no header — LLM date is the only candidate
            Some(&extractor),
        )
        .await
        .expect_err("non-canonical LLM date must fail, not persist");
        assert!(matches!(err, Error::UndatedJournal));
        assert!(archive.list_keys().await.expect("keys").is_empty());
        assert!(derived.list_keys().await.expect("keys").is_empty());
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM source_documents")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(count.0, 0);
    }

    #[tokio::test]
    async fn no_extractor_fails_with_nothing_persisted() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let err = ingest_journal(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(ENTRY.as_bytes()),
            params(None),
            None::<&crate::extraction::ClaudeExtractor>,
        )
        .await
        .expect_err("missing extractor must fail");
        assert!(matches!(err, Error::ExtractorNotConfigured));
        assert!(archive.list_keys().await.expect("keys").is_empty());
        assert!(derived.list_keys().await.expect("keys").is_empty());
    }

    #[tokio::test]
    async fn non_utf8_bytes_fail_before_anything_persists() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockJournalExtractor::new(vec![]);
        let err = ingest_journal(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(&[0xFF, 0xFE, 0x00]),
            params(None),
            Some(&extractor),
        )
        .await
        .expect_err("non-utf8 must fail");
        assert!(matches!(err, Error::JournalNotUtf8));
        assert!(archive.list_keys().await.expect("keys").is_empty());
    }

    #[tokio::test]
    async fn re_ingest_of_same_entry_upserts_without_duplicates() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;
        let extractor = MockJournalExtractor::new(vec![
            shoulder_extraction("M25.512"),
            shoulder_extraction("M25.512"),
        ]);
        for _ in 0..2 {
            ingest_journal(
                &archive,
                &derived,
                &pool,
                Bytes::from_static(ENTRY.as_bytes()),
                params(None),
                Some(&extractor),
            )
            .await
            .expect("ingest");
        }
        let doc_count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM source_documents WHERE kind = 'journal'")
                .fetch_one(&pool)
                .await
                .expect("count");
        assert_eq!(doc_count.0, 1, "same bytes must not duplicate");
        let obs_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM observations")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(obs_count.0, 1, "observations must not accumulate");
    }

    #[tokio::test]
    async fn replay_journal_rebuilds_document_and_text_from_a_bare_blob() {
        let (pool, archive, _derived) = fresh_pool_and_stores().await;
        let content = Bytes::from_static(ENTRY.as_bytes());
        let manifest = Manifest::new(
            "journal",
            JOURNAL_KIND,
            "text/markdown",
            Some("2026-07-26".to_owned()),
            time::macros::datetime!(2026-07-26 21:00:00 UTC),
            Some("2026-07-26.md".to_owned()),
        );
        let key = archive
            .put_with_manifest(content.clone(), manifest.clone())
            .await
            .expect("put");

        let id = replay_journal(&pool, &key, &content, &manifest)
            .await
            .expect("replay");
        let doc = crate::index::get_source_document_by_id(&pool, id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(doc.kind, JOURNAL_KIND);
        assert_eq!(doc.document_date.as_deref(), Some("2026-07-26"));
        let text = crate::index::get_narrative_text(&pool, id)
            .await
            .expect("get")
            .expect("present");
        assert!(text.text.contains("climbing"));
    }
}
