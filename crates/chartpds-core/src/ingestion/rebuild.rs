//! Archive-to-index rebuild pipeline.
//!
//! Replays two stores: the archive (source bytes from outside) and the
//! derived store (machine-generated derivations, currently extraction
//! artifacts). Neither is consulted for freshness — the index is dropped and
//! rebuilt from both wholesale.

use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::archive::{Archive, BlobKey, Manifest};
use crate::extraction::ExtractionArtifact;
use crate::index;
use crate::ingestion::{ingest, Error, Result};
use crate::sources;

use super::narrative;

/// Summary of a rebuild-index operation, broken down by source.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RebuildResult {
    /// Total blobs found across the archive and the derived store.
    pub blobs_found: u64,
    /// CCDA documents re-ingested.
    pub ccda_ingested: u64,
    /// Fitbit heart-rate days replayed.
    pub fitbit_ingested: u64,
    /// Oura sleep sessions replayed.
    pub oura_ingested: u64,
    /// Blobs skipped (unknown type, malformed payload, or manifest-less
    /// non-CCDA).
    pub blobs_skipped: u64,
    /// Narrative PDF documents replayed (text re-extracted deterministically).
    pub narratives_ingested: u64,
    /// Frozen extraction artifacts applied to their narrative documents.
    pub extractions_applied: u64,
}

/// Drop the index and rebuild it from the archive and the derived store,
/// replaying every source.
///
/// Each archive blob is routed by its sidecar [`Manifest`] `type`: CCDA
/// documents are re-ingested and Fitbit/Oura blobs are replayed by their
/// adapters. Blobs with no manifest (legacy) fall back to a best-effort CCDA
/// parse. Derived-store blobs are expected to be `narrative-extraction`
/// artifacts; extraction artifacts found in the archive (a legacy layout
/// predating the derived store) replay identically. Unknown types and
/// malformed payloads are counted as skipped. The blob's `archived_at` is
/// preserved (taken from the manifest), not rewritten to "now".
///
/// # Errors
///
/// Returns [`Error`] if a blob or its manifest cannot be read, the index cannot
/// be cleared, or a replay fails for a reason other than a malformed payload.
pub async fn rebuild_index(
    archive: &Archive,
    derived: &Archive,
    pool: &SqlitePool,
) -> Result<RebuildResult> {
    // 1. Clear existing ingested data.
    index::clear_ingested_data(pool).await?;

    // 2. List blobs in both stores.
    let archive_keys = archive.list_keys().await?;
    let derived_keys = derived.list_keys().await?;
    let blobs_found = (archive_keys.len() + derived_keys.len()) as u64;

    // 3. Phase one: replay each blob, routed by manifest type. Extraction
    //    artifacts are collected rather than applied immediately, since the
    //    narrative document they reference may not exist yet.
    let mut tally = ReplayTally::default();
    for key in &archive_keys {
        let content = archive.get(key).await?;
        tally.record(replay_blob(archive, pool, key, content).await?);
    }
    for key in &derived_keys {
        let content = derived.get(key).await?;
        tally.record(replay_derived_blob(derived, key, &content).await?);
    }

    // 4. Phase two: apply extraction artifacts, now that every narrative
    //    document exists.
    let (extractions_applied, artifact_skips) =
        apply_newest_extraction_artifacts(pool, tally.extraction_artifacts).await?;

    Ok(RebuildResult {
        blobs_found,
        ccda_ingested: tally.ccda_ingested,
        fitbit_ingested: tally.fitbit_ingested,
        oura_ingested: tally.oura_ingested,
        blobs_skipped: tally.blobs_skipped + artifact_skips,
        narratives_ingested: tally.narratives_ingested,
        extractions_applied,
    })
}

/// Phase-one counters plus the deferred artifact list, shared by the archive
/// and derived-store replay loops.
#[derive(Default)]
struct ReplayTally {
    ccda_ingested: u64,
    fitbit_ingested: u64,
    oura_ingested: u64,
    blobs_skipped: u64,
    narratives_ingested: u64,
    extraction_artifacts: Vec<(OffsetDateTime, ExtractionArtifact)>,
}

impl ReplayTally {
    fn record(&mut self, outcome: BlobOutcome) {
        match outcome {
            BlobOutcome::Ccda => self.ccda_ingested += 1,
            BlobOutcome::Fitbit => self.fitbit_ingested += 1,
            BlobOutcome::Oura => self.oura_ingested += 1,
            BlobOutcome::Narrative => self.narratives_ingested += 1,
            BlobOutcome::ExtractionArtifact(archived_at, artifact) => {
                self.extraction_artifacts.push((archived_at, artifact));
            }
            BlobOutcome::Skipped => self.blobs_skipped += 1,
        }
    }
}

/// What replaying one archived blob, routed by its manifest kind, produced.
enum BlobOutcome {
    /// A CCDA document was (re-)ingested.
    Ccda,
    /// A Fitbit intraday heart-rate day was replayed.
    Fitbit,
    /// An Oura sleep session was replayed.
    Oura,
    /// A narrative PDF's text was replayed.
    Narrative,
    /// A frozen extraction artifact, deferred to the phase-two apply pass.
    ExtractionArtifact(OffsetDateTime, ExtractionArtifact),
    /// The blob was unreadable, unrecognized, or malformed.
    Skipped,
}

/// Route one archived blob by its sidecar manifest `type` and replay it.
async fn replay_blob(
    archive: &Archive,
    pool: &SqlitePool,
    key: &BlobKey,
    content: bytes::Bytes,
) -> Result<BlobOutcome> {
    let Some(manifest) = archive.get_manifest(key).await? else {
        // Legacy blob with no manifest: best-effort CCDA parse.
        return match ingest(
            archive,
            pool,
            content,
            "ccda",
            "rebuild",
            None,
            OffsetDateTime::now_utc(),
        )
        .await
        {
            Ok(_) => Ok(BlobOutcome::Ccda),
            Err(Error::NotCcda { .. } | Error::Xml(_)) => Ok(BlobOutcome::Skipped),
            Err(err) => Err(err),
        };
    };

    match manifest.kind.as_str() {
        "ccda" => match reingest_ccda(archive, pool, content, &manifest).await {
            Ok(()) => Ok(BlobOutcome::Ccda),
            Err(Error::NotCcda { .. } | Error::Xml(_)) => Ok(BlobOutcome::Skipped),
            Err(err) => Err(err),
        },
        "fitbit-intraday-hr-day" => {
            match sources::fitbit::storage::replay(pool, key, &content, &manifest).await {
                Ok(_) => Ok(BlobOutcome::Fitbit),
                Err(sources::Error::Parse { reason }) => {
                    tracing::warn!(key = key.as_str(), %reason, "skipping malformed fitbit blob");
                    Ok(BlobOutcome::Skipped)
                }
                Err(err) => Err(Error::Adapter(err)),
            }
        }
        "oura-sleep-session" => {
            match sources::oura::storage::replay(pool, key, &content, &manifest).await {
                Ok(_) => Ok(BlobOutcome::Oura),
                Err(sources::Error::Parse { reason }) => {
                    tracing::warn!(key = key.as_str(), %reason, "skipping malformed oura blob");
                    Ok(BlobOutcome::Skipped)
                }
                Err(err) => Err(Error::Adapter(err)),
            }
        }
        narrative::NARRATIVE_PDF_KIND => {
            match narrative::replay_pdf(pool, key, &content, &manifest).await {
                Ok(_) => Ok(BlobOutcome::Narrative),
                Err(Error::Extraction(err)) => {
                    tracing::warn!(key = key.as_str(), %err, "skipping unreadable narrative pdf");
                    Ok(BlobOutcome::Skipped)
                }
                Err(err) => Err(err),
            }
        }
        // Legacy layout: artifacts written before the derived store existed
        // live in the archive and must keep replaying from there.
        narrative::NARRATIVE_EXTRACTION_KIND => {
            Ok(parse_extraction_artifact(key, &content, &manifest))
        }
        other => {
            tracing::warn!(
                key = key.as_str(),
                kind = other,
                "skipping unknown manifest type"
            );
            Ok(BlobOutcome::Skipped)
        }
    }
}

/// Route one derived-store blob. The derived store holds machine-generated
/// derivations only — currently just `narrative-extraction` artifacts — so
/// anything else (including a manifest-less blob) is skipped, never
/// type-sniffed as a source document.
async fn replay_derived_blob(
    derived: &Archive,
    key: &BlobKey,
    content: &bytes::Bytes,
) -> Result<BlobOutcome> {
    let Some(manifest) = derived.get_manifest(key).await? else {
        tracing::warn!(key = key.as_str(), "skipping manifest-less derived blob");
        return Ok(BlobOutcome::Skipped);
    };
    if manifest.kind == narrative::NARRATIVE_EXTRACTION_KIND {
        Ok(parse_extraction_artifact(key, content, &manifest))
    } else {
        tracing::warn!(
            key = key.as_str(),
            kind = manifest.kind.as_str(),
            "skipping unknown derived blob type"
        );
        Ok(BlobOutcome::Skipped)
    }
}

/// Parse a `narrative-extraction` blob into its deferred phase-two outcome;
/// malformed JSON is skipped, not fatal.
fn parse_extraction_artifact(
    key: &BlobKey,
    content: &bytes::Bytes,
    manifest: &Manifest,
) -> BlobOutcome {
    match serde_json::from_slice::<ExtractionArtifact>(content) {
        Ok(artifact) => BlobOutcome::ExtractionArtifact(manifest.archived_at, artifact),
        Err(err) => {
            tracing::warn!(key = key.as_str(), %err, "skipping malformed extraction artifact");
            BlobOutcome::Skipped
        }
    }
}

/// Apply the newest extraction artifact per referenced document (a retried
/// extraction can leave multiple artifacts pointing at the same PDF), now
/// that every narrative document from phase one exists. Returns
/// `(extractions_applied, blobs_skipped)`.
async fn apply_newest_extraction_artifacts(
    pool: &SqlitePool,
    extraction_artifacts: Vec<(OffsetDateTime, ExtractionArtifact)>,
) -> Result<(u64, u64)> {
    let mut newest: std::collections::HashMap<String, (OffsetDateTime, ExtractionArtifact)> =
        std::collections::HashMap::new();
    for (at, artifact) in extraction_artifacts {
        match newest.get(&artifact.document) {
            Some((existing_at, _)) if *existing_at >= at => {}
            _ => {
                newest.insert(artifact.document.clone(), (at, artifact));
            }
        }
    }

    let mut extractions_applied = 0u64;
    let mut blobs_skipped = 0u64;
    for (_at, artifact) in newest.into_values() {
        let Ok(pdf_key) = BlobKey::from_hex_str(&artifact.document) else {
            tracing::warn!(document = %artifact.document, "artifact references invalid blob key");
            blobs_skipped += 1;
            continue;
        };
        if let Some(doc) = index::fetch_source_document_by_archive_key(pool, &pdf_key).await? {
            narrative::apply_extraction(pool, doc.id, &artifact).await?;
            extractions_applied += 1;
        } else {
            tracing::warn!(document = %artifact.document, "artifact references missing document");
            blobs_skipped += 1;
        }
    }
    Ok((extractions_applied, blobs_skipped))
}

/// Re-ingest a CCDA blob, preserving its manifest provenance (`source`,
/// `original_filename`) and immutable `archived_at`.
async fn reingest_ccda(
    archive: &Archive,
    pool: &SqlitePool,
    content: bytes::Bytes,
    manifest: &Manifest,
) -> Result<()> {
    ingest(
        archive,
        pool,
        content,
        &manifest.kind,
        &manifest.source,
        manifest.original_filename.as_deref(),
        manifest.archived_at,
    )
    .await
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::Archive;
    use crate::index::open_pool;
    use crate::queries;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    const VALID_CCDA: &[u8] = include_bytes!("ccda/fixtures/valid_minimal.xml");

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

    #[tokio::test]
    async fn rebuild_ingests_ccda_and_skips_non_ccda() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;

        // Archive a valid CCDA and a non-CCDA JSON blob.
        archive
            .put(Bytes::from_static(VALID_CCDA))
            .await
            .expect("put ccda");
        archive
            .put(Bytes::from_static(b"{\"not\": \"ccda\"}"))
            .await
            .expect("put json");

        let result = rebuild_index(&archive, &derived, &pool)
            .await
            .expect("rebuild");

        assert_eq!(result.blobs_found, 2);
        assert_eq!(result.ccda_ingested, 1);
        assert_eq!(result.blobs_skipped, 1);

        // Verify observations from the CCDA made it through.
        let counts = queries::counts_per_code(&pool).await.expect("counts");
        assert!(!counts.is_empty(), "expected observations after rebuild");
    }

    #[tokio::test]
    async fn rebuild_clears_old_data_first() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;

        // First ingest a CCDA normally.
        ingest(
            &archive,
            &pool,
            Bytes::from_static(VALID_CCDA),
            "ccda",
            "test",
            None,
            OffsetDateTime::now_utc(),
        )
        .await
        .expect("initial ingest");

        // Verify data exists.
        let counts_before = queries::counts_per_code(&pool).await.expect("counts");
        assert!(!counts_before.is_empty());

        // Rebuild should clear and re-ingest. The same CCDA is still in the
        // archive so we should end up with the same observation count.
        let result = rebuild_index(&archive, &derived, &pool)
            .await
            .expect("rebuild");
        assert_eq!(result.ccda_ingested, 1);
        assert_eq!(result.blobs_skipped, 0);

        let counts_after = queries::counts_per_code(&pool).await.expect("counts");
        assert_eq!(counts_before.len(), counts_after.len());
    }

    #[tokio::test]
    async fn rebuild_on_empty_archive_returns_zero_counts() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;

        let result = rebuild_index(&archive, &derived, &pool)
            .await
            .expect("rebuild");
        assert_eq!(result.blobs_found, 0);
        assert_eq!(result.ccda_ingested, 0);
        assert_eq!(result.blobs_skipped, 0);
    }

    #[tokio::test]
    async fn rebuild_replays_all_sources_and_preserves_archived_at() {
        use crate::archive::compute_blob_key;
        use crate::index::fetch_source_document_by_archive_key;
        use crate::sources::fitbit::api::{HeartRateSample, IntradayResult};
        use crate::sources::oura::api::OuraSleepSession;
        use crate::sources::{fitbit, oura};

        let (pool, archive, derived) = fresh_pool_and_stores().await;

        // CCDA with an explicit (non-now) archived_at to prove preservation.
        let ccda_archived_at = time::macros::datetime!(2026-01-02 09:00:00 UTC);
        ingest(
            &archive,
            &pool,
            Bytes::from_static(VALID_CCDA),
            "ccda",
            "manual-upload",
            Some("ccd.xml"),
            ccda_archived_at,
        )
        .await
        .expect("ccda ingest");

        // Fitbit day: raw_pages carry the dataPoints so replay re-derives the
        // same samples the live path inserted.
        let page = serde_json::json!({
            "dataPoints": [
                {"heartRate": {"sampleTime": {"physicalTime": "2026-01-01T08:30:00.000Z"}, "beatsPerMinute": 72}},
                {"heartRate": {"sampleTime": {"physicalTime": "2026-01-01T08:31:00.000Z"}, "beatsPerMinute": 75}}
            ]
        });
        let fitbit_result = IntradayResult {
            samples: vec![
                HeartRateSample {
                    physical_time: "2026-01-01T08:30:00.000Z".to_owned(),
                    beats_per_minute: 72,
                },
                HeartRateSample {
                    physical_time: "2026-01-01T08:31:00.000Z".to_owned(),
                    beats_per_minute: 75,
                },
            ],
            raw_pages: vec![page],
        };
        fitbit::storage::ingest_day(&archive, &pool, "2026-01-01", &fitbit_result)
            .await
            .expect("fitbit ingest");

        // Oura session: archived bytes are the full session object.
        let session = OuraSleepSession {
            id: "s1".to_owned(),
            day: "2026-01-15".to_owned(),
            bedtime_start: "2026-01-14T22:00:00Z".to_owned(),
            bedtime_end: "2026-01-15T06:00:00Z".to_owned(),
            session_type: "long_sleep".to_owned(),
            sleep_phase_5_min: "4421".to_owned(),
            total_sleep_duration: Some(28800),
            rem_sleep_duration: Some(7200),
            deep_sleep_duration: Some(5400),
            light_sleep_duration: Some(12600),
        };
        let raw_json = serde_json::to_value(&session).expect("session to_value");
        oura::storage::ingest_session(&archive, &pool, &session, &raw_json)
            .await
            .expect("oura ingest");

        let counts_before = queries::counts_per_code(&pool).await.expect("counts");

        let result = rebuild_index(&archive, &derived, &pool)
            .await
            .expect("rebuild");
        assert_eq!(result.blobs_found, 3);
        assert_eq!(result.ccda_ingested, 1);
        assert_eq!(result.fitbit_ingested, 1);
        assert_eq!(result.oura_ingested, 1);
        assert_eq!(result.blobs_skipped, 0);

        // Same observations as before the rebuild — adapter data is replayed,
        // not lost.
        let counts_after = queries::counts_per_code(&pool).await.expect("counts");
        assert_eq!(counts_before.len(), counts_after.len());

        // archived_at survives the rebuild (copied from the manifest, not
        // stamped "now").
        let ccda_key = compute_blob_key(VALID_CCDA);
        let doc = fetch_source_document_by_archive_key(&pool, &ccda_key)
            .await
            .expect("fetch")
            .expect("ccda doc present");
        assert_eq!(doc.archived_at, ccda_archived_at);
        assert_eq!(doc.source, "manual-upload");
    }

    #[tokio::test]
    async fn rebuild_collapses_duplicate_fitbit_days_to_the_newest_pull() {
        use crate::sources::fitbit;
        use crate::sources::fitbit::api::{HeartRateSample, IntradayResult};

        let (pool, archive, derived) = fresh_pool_and_stores().await;

        // Two overlapping pulls of the SAME day land as two distinct archive
        // blobs (different bytes => different content hash). This is the
        // production scenario: many retried/grown syncs archived a fresh blob
        // for an overlapping window.
        // raw_pages must be valid Fitbit pages: rebuild's replay re-derives the
        // samples FROM the archived raw_pages (it never sees the live `samples`).
        let pull1 = IntradayResult {
            samples: vec![HeartRateSample {
                physical_time: "2026-01-01T08:00:00.000Z".to_owned(),
                beats_per_minute: 72,
            }],
            raw_pages: vec![serde_json::json!({
                "dataPoints": [
                    {"heartRate": {"sampleTime": {"physicalTime": "2026-01-01T08:00:00.000Z"}, "beatsPerMinute": 72}}
                ]
            })],
        };
        fitbit::storage::ingest_day(&archive, &pool, "2026-01-01", &pull1)
            .await
            .expect("ingest pull1");

        let pull2 = IntradayResult {
            samples: vec![
                HeartRateSample {
                    physical_time: "2026-01-01T08:00:00.000Z".to_owned(),
                    beats_per_minute: 72,
                },
                HeartRateSample {
                    physical_time: "2026-01-01T08:00:02.000Z".to_owned(),
                    beats_per_minute: 75,
                },
            ],
            raw_pages: vec![serde_json::json!({
                "dataPoints": [
                    {"heartRate": {"sampleTime": {"physicalTime": "2026-01-01T08:00:00.000Z"}, "beatsPerMinute": 72}},
                    {"heartRate": {"sampleTime": {"physicalTime": "2026-01-01T08:00:02.000Z"}, "beatsPerMinute": 75}}
                ]
            })],
        };
        fitbit::storage::ingest_day(&archive, &pool, "2026-01-01", &pull2)
            .await
            .expect("ingest pull2");

        // Both blobs are durably in the archive even though the index already
        // holds only the newest.
        assert_eq!(archive.list_keys().await.expect("keys").len(), 2);

        // Rebuild clears the index and replays both blobs. Supersession must
        // collapse them back to one document — the newest pull (two samples).
        let result = rebuild_index(&archive, &derived, &pool)
            .await
            .expect("rebuild");
        assert_eq!(result.blobs_found, 2);
        assert_eq!(result.fitbit_ingested, 2);

        let doc_count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM source_documents WHERE source = 'fitbit' AND document_date = '2026-01-01'",
        )
        .fetch_one(&pool)
        .await
        .expect("count docs");
        assert_eq!(
            doc_count.0, 1,
            "duplicate day must collapse to one document"
        );

        let obs_count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM observations WHERE coding_code = '8867-4'")
                .fetch_one(&pool)
                .await
                .expect("count obs");
        assert_eq!(
            obs_count.0, 2,
            "only the newest pull's observations survive"
        );
    }

    #[tokio::test]
    async fn rebuild_skips_manifest_less_non_ccda_blob() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;

        // A bare put (no manifest) of non-CCDA bytes: legacy fallback tries
        // CCDA, fails, and skips.
        archive
            .put(Bytes::from_static(b"{\"legacy\": \"json\"}"))
            .await
            .expect("put");

        let result = rebuild_index(&archive, &derived, &pool)
            .await
            .expect("rebuild");
        assert_eq!(result.blobs_found, 1);
        assert_eq!(result.ccda_ingested, 0);
        assert_eq!(result.blobs_skipped, 1);
    }

    const PDF_FIXTURE: &[u8] = include_bytes!("../extraction/fixtures/synthetic_pathology.pdf");

    /// Canned extractor for the narrative rebuild tests: fixed claims that
    /// verify against `PDF_FIXTURE`, no network.
    struct MockExtractor;
    impl crate::extraction::LlmExtractor for MockExtractor {
        async fn extract(
            &self,
            _text: &str,
        ) -> std::result::Result<crate::extraction::RawExtraction, crate::extraction::Error>
        {
            use crate::extraction::{RawCoding, RawExtraction};
            Ok(RawExtraction {
                document_date: Some("2026-04-21".to_owned()),
                document_date_quote: Some("Order Date: 04/21/2026".to_owned()),
                title: Some("GI Pathology Report".to_owned()),
                codings: vec![RawCoding {
                    code: "R10.9".to_owned(),
                    display: "Abdominal pain, unspecified".to_owned(),
                    quote: "Abdominal pain, unspecified - R10.9".to_owned(),
                    section_label: Some("Pre-Op Diagnosis/Indications".to_owned()),
                }],
            })
        }
    }

    #[tokio::test]
    async fn rebuild_replays_narrative_pdf_and_applies_artifact_without_llm() {
        use crate::ingestion::{ingest_narrative_pdf, NarrativeIngestParams, NARRATIVE_PDF_KIND};

        let (pool, archive, derived) = fresh_pool_and_stores().await;
        ingest_narrative_pdf(
            &archive,
            &derived,
            &pool,
            Bytes::from_static(PDF_FIXTURE),
            NarrativeIngestParams {
                source: "manual-upload",
                original_filename: Some("synthetic_pathology.pdf"),
                archived_at: time::macros::datetime!(2026-07-06 12:00:00 UTC),
            },
            Some(&MockExtractor),
        )
        .await
        .expect("live ingest");

        // The tiers separate cleanly: source PDF in the archive, extraction
        // artifact in the derived store.
        assert_eq!(archive.list_keys().await.expect("keys").len(), 1);
        assert_eq!(derived.list_keys().await.expect("keys").len(), 1);

        // Rebuild must reproduce everything from the two stores alone — the
        // extractor is NOT provided anywhere in the rebuild path.
        let result = rebuild_index(&archive, &derived, &pool)
            .await
            .expect("rebuild");
        assert_eq!(result.blobs_found, 2);
        assert_eq!(result.narratives_ingested, 1);
        assert_eq!(result.extractions_applied, 1);
        assert_eq!(result.blobs_skipped, 0);

        // The coded problem, title, date, and FTS text all survived.
        let doc_row: (i64, Option<String>) =
            sqlx::query_as("SELECT id, document_date FROM source_documents WHERE kind = ?")
                .bind(NARRATIVE_PDF_KIND)
                .fetch_one(&pool)
                .await
                .expect("doc row");
        assert_eq!(doc_row.1.as_deref(), Some("2026-04-21"));

        let text = crate::index::get_narrative_text(&pool, doc_row.0)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(text.title.as_deref(), Some("GI Pathology Report"));

        let problems = crate::index::list_problems_by_source_document(&pool, doc_row.0)
            .await
            .expect("problems");
        assert_eq!(problems.len(), 1);
        assert_eq!(problems[0].coding_code, "R10.9");

        let fts: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM narrative_texts_fts WHERE narrative_texts_fts MATCH 'dysplasia'",
        )
        .fetch_one(&pool)
        .await
        .expect("fts");
        assert_eq!(fts.0, 1);
    }

    #[tokio::test]
    async fn rebuild_applies_legacy_extraction_artifact_stored_in_the_archive() {
        use crate::ingestion::{ingest_narrative_pdf, NarrativeIngestParams};

        let (pool, archive, derived) = fresh_pool_and_stores().await;

        // Legacy layout: pass the archive as the derived store too, so the
        // artifact lands next to the source PDF — exactly what data dirs
        // written before the derived store existed look like.
        ingest_narrative_pdf(
            &archive,
            &archive,
            &pool,
            Bytes::from_static(PDF_FIXTURE),
            NarrativeIngestParams {
                source: "manual-upload",
                original_filename: Some("synthetic_pathology.pdf"),
                archived_at: time::macros::datetime!(2026-07-06 12:00:00 UTC),
            },
            Some(&MockExtractor),
        )
        .await
        .expect("live ingest");
        assert_eq!(archive.list_keys().await.expect("keys").len(), 2);

        let result = rebuild_index(&archive, &derived, &pool)
            .await
            .expect("rebuild");
        assert_eq!(result.blobs_found, 2);
        assert_eq!(result.narratives_ingested, 1);
        assert_eq!(
            result.extractions_applied, 1,
            "artifact in the archive must still replay"
        );
        assert_eq!(result.blobs_skipped, 0);
    }

    #[tokio::test]
    async fn rebuild_skips_stray_blobs_in_the_derived_store() {
        let (pool, archive, derived) = fresh_pool_and_stores().await;

        // A manifest-less blob and a wrong-kind blob in the derived store:
        // both are skipped, never type-sniffed as source documents.
        derived
            .put(Bytes::from_static(b"{\"stray\": true}"))
            .await
            .expect("bare put");
        derived
            .put_with_manifest(
                Bytes::from_static(VALID_CCDA),
                crate::archive::Manifest::new(
                    "test",
                    "ccda",
                    "application/xml",
                    None,
                    time::macros::datetime!(2026-07-06 12:00:00 UTC),
                    None,
                ),
            )
            .await
            .expect("put with manifest");

        let result = rebuild_index(&archive, &derived, &pool)
            .await
            .expect("rebuild");
        assert_eq!(result.blobs_found, 2);
        assert_eq!(result.blobs_skipped, 2);
        assert_eq!(result.ccda_ingested, 0, "derived blobs are never ingested");
    }
}
