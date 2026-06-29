//! Archive-to-index rebuild pipeline.

use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::archive::{Archive, Manifest};
use crate::index;
use crate::ingestion::{ingest, Error, Result};
use crate::sources;

/// Summary of a rebuild-index operation, broken down by source.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RebuildResult {
    /// Total blobs found in the archive.
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
}

/// Drop the index and rebuild it from the archive, replaying every source.
///
/// Each blob is routed by its sidecar [`Manifest`] `type`: CCDA documents are
/// re-ingested and Fitbit/Oura blobs are replayed by their adapters. Blobs with
/// no manifest (legacy) fall back to a best-effort CCDA parse. Unknown types
/// and malformed payloads are counted as skipped. The blob's `archived_at` is
/// preserved (taken from the manifest), not rewritten to "now".
///
/// # Errors
///
/// Returns [`Error`] if a blob or its manifest cannot be read, the index cannot
/// be cleared, or a replay fails for a reason other than a malformed payload.
pub async fn rebuild_index(archive: &Archive, pool: &SqlitePool) -> Result<RebuildResult> {
    // 1. Clear existing ingested data.
    index::clear_ingested_data(pool).await?;

    // 2. List all archive blobs.
    let keys = archive.list_keys().await?;
    let blobs_found = keys.len() as u64;

    // 3. Replay each, routed by manifest type.
    let mut ccda_ingested = 0u64;
    let mut fitbit_ingested = 0u64;
    let mut oura_ingested = 0u64;
    let mut blobs_skipped = 0u64;

    for key in &keys {
        let content = archive.get(key).await?;
        match archive.get_manifest(key).await? {
            Some(manifest) => match manifest.kind.as_str() {
                "ccda" => match reingest_ccda(archive, pool, content, &manifest).await {
                    Ok(()) => ccda_ingested += 1,
                    Err(Error::NotCcda { .. } | Error::Xml(_)) => blobs_skipped += 1,
                    Err(err) => return Err(err),
                },
                "fitbit-intraday-hr-day" => {
                    match sources::fitbit::storage::replay(pool, key, &content, &manifest).await {
                        Ok(_) => fitbit_ingested += 1,
                        Err(sources::Error::Parse { reason }) => {
                            tracing::warn!(key = key.as_str(), %reason, "skipping malformed fitbit blob");
                            blobs_skipped += 1;
                        }
                        Err(err) => return Err(Error::Adapter(err)),
                    }
                }
                "oura-sleep-session" => {
                    match sources::oura::storage::replay(pool, key, &content, &manifest).await {
                        Ok(_) => oura_ingested += 1,
                        Err(sources::Error::Parse { reason }) => {
                            tracing::warn!(key = key.as_str(), %reason, "skipping malformed oura blob");
                            blobs_skipped += 1;
                        }
                        Err(err) => return Err(Error::Adapter(err)),
                    }
                }
                other => {
                    tracing::warn!(
                        key = key.as_str(),
                        kind = other,
                        "skipping unknown manifest type"
                    );
                    blobs_skipped += 1;
                }
            },
            // Legacy blob with no manifest: best-effort CCDA parse.
            None => match ingest(
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
                Ok(_) => ccda_ingested += 1,
                Err(Error::NotCcda { .. } | Error::Xml(_)) => blobs_skipped += 1,
                Err(err) => return Err(err),
            },
        }
    }

    Ok(RebuildResult {
        blobs_found,
        ccda_ingested,
        fitbit_ingested,
        oura_ingested,
        blobs_skipped,
    })
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

    async fn fresh_pool_and_archive() -> (SqlitePool, Archive) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let backend = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;
        (pool, Archive::new(backend))
    }

    #[tokio::test]
    async fn rebuild_ingests_ccda_and_skips_non_ccda() {
        let (pool, archive) = fresh_pool_and_archive().await;

        // Archive a valid CCDA and a non-CCDA JSON blob.
        archive
            .put(Bytes::from_static(VALID_CCDA))
            .await
            .expect("put ccda");
        archive
            .put(Bytes::from_static(b"{\"not\": \"ccda\"}"))
            .await
            .expect("put json");

        let result = rebuild_index(&archive, &pool).await.expect("rebuild");

        assert_eq!(result.blobs_found, 2);
        assert_eq!(result.ccda_ingested, 1);
        assert_eq!(result.blobs_skipped, 1);

        // Verify observations from the CCDA made it through.
        let counts = queries::counts_per_code(&pool).await.expect("counts");
        assert!(!counts.is_empty(), "expected observations after rebuild");
    }

    #[tokio::test]
    async fn rebuild_clears_old_data_first() {
        let (pool, archive) = fresh_pool_and_archive().await;

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
        let result = rebuild_index(&archive, &pool).await.expect("rebuild");
        assert_eq!(result.ccda_ingested, 1);
        assert_eq!(result.blobs_skipped, 0);

        let counts_after = queries::counts_per_code(&pool).await.expect("counts");
        assert_eq!(counts_before.len(), counts_after.len());
    }

    #[tokio::test]
    async fn rebuild_on_empty_archive_returns_zero_counts() {
        let (pool, archive) = fresh_pool_and_archive().await;

        let result = rebuild_index(&archive, &pool).await.expect("rebuild");
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

        let (pool, archive) = fresh_pool_and_archive().await;

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

        let result = rebuild_index(&archive, &pool).await.expect("rebuild");
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

        let (pool, archive) = fresh_pool_and_archive().await;

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
        let result = rebuild_index(&archive, &pool).await.expect("rebuild");
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
        let (pool, archive) = fresh_pool_and_archive().await;

        // A bare put (no manifest) of non-CCDA bytes: legacy fallback tries
        // CCDA, fails, and skips.
        archive
            .put(Bytes::from_static(b"{\"legacy\": \"json\"}"))
            .await
            .expect("put");

        let result = rebuild_index(&archive, &pool).await.expect("rebuild");
        assert_eq!(result.blobs_found, 1);
        assert_eq!(result.ccda_ingested, 0);
        assert_eq!(result.blobs_skipped, 1);
    }
}
