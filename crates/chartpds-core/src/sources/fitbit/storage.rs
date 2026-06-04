//! Adapter-day archiving and index insertion for Fitbit heart-rate data.
//!
//! The non-CCDA ingestion path: raw JSON API responses get archived as blobs
//! and parsed observations get written to the index.

use bytes::Bytes;
use sqlx::SqlitePool;
use time::OffsetDateTime;

use super::api::{self, IntradayResult};
use super::parser;
use crate::archive::{Archive, BlobKey, Manifest};
use crate::index;
use crate::sources;

/// `CloudEvents` `type` for an archived Fitbit intraday heart-rate day.
const KIND: &str = "fitbit-intraday-hr-day";
/// `CloudEvents` `source` for the Fitbit adapter.
const SOURCE: &str = "fitbit";

/// Ingest one day's heart-rate data into the archive + index.
///
/// 1. Serialize the raw JSON pages and archive them with a provenance manifest
///    (the day is recorded as the manifest `subject` so the blob is replayable).
/// 2. Insert a `source_documents` row, observations, and `source_day_state`.
///
/// Returns the `source_documents.id`.
pub(crate) async fn ingest_day(
    archive: &Archive,
    pool: &SqlitePool,
    date: &str,
    result: &IntradayResult,
) -> sources::Result<i64> {
    // 1. Archive raw JSON pages with a sidecar manifest.
    let raw_bytes = serde_json::to_vec(&result.raw_pages).map_err(|err| sources::Error::Parse {
        reason: format!("serializing raw pages: {err}"),
    })?;
    let archived_at = OffsetDateTime::now_utc();
    let original_filename = format!("fitbit-hr-{date}.json");
    let manifest = Manifest::new(
        SOURCE,
        KIND,
        "application/json",
        Some(date.to_owned()),
        archived_at,
        Some(original_filename.clone()),
    );
    let archive_key = archive
        .put_with_manifest(Bytes::from(raw_bytes), manifest)
        .await?;

    // 2. Project into the index.
    index_intraday_day(
        pool,
        &archive_key,
        date,
        result,
        archived_at,
        &original_filename,
    )
    .await
}

/// Rebuild the index rows for an already-archived Fitbit blob.
///
/// Reads the raw JSON pages from `content`, re-derives the samples (via the
/// same page parser used at fetch time), and re-projects into the index — no
/// network call. The day is taken from the manifest `subject`.
///
/// # Errors
///
/// Returns [`sources::Error::Parse`] if the bytes are not the expected
/// `Vec<page>` JSON, if a page fails to parse, or if the manifest lacks the
/// `subject` (date) needed to key `source_day_state`.
pub(crate) async fn replay(
    pool: &SqlitePool,
    archive_key: &BlobKey,
    content: &[u8],
    manifest: &Manifest,
) -> sources::Result<i64> {
    let date = manifest
        .subject
        .as_deref()
        .ok_or_else(|| sources::Error::Parse {
            reason: "fitbit manifest missing subject (date) for replay".to_owned(),
        })?;

    let raw_pages: Vec<serde_json::Value> =
        serde_json::from_slice(content).map_err(|err| sources::Error::Parse {
            reason: format!("deserializing fitbit raw pages: {err}"),
        })?;

    let mut samples = Vec::new();
    for page in &raw_pages {
        let (page_samples, _next) = api::parse_data_points_page(page)?;
        samples.extend(page_samples);
    }
    let result = IntradayResult { samples, raw_pages };

    let original_filename = manifest
        .original_filename
        .clone()
        .unwrap_or_else(|| format!("fitbit-hr-{date}.json"));

    index_intraday_day(
        pool,
        archive_key,
        date,
        &result,
        manifest.archived_at,
        &original_filename,
    )
    .await
}

/// Shared index-write tail: insert the `source_documents` row, the synthesized
/// heart-rate observations, and the `source_day_state`. Used by both live sync
/// ([`ingest_day`]) and archive [`replay`] so the projection logic cannot drift.
async fn index_intraday_day(
    pool: &SqlitePool,
    archive_key: &BlobKey,
    date: &str,
    result: &IntradayResult,
    archived_at: OffsetDateTime,
    original_filename: &str,
) -> sources::Result<i64> {
    let source_document_id = index::insert_source_document(
        pool,
        index::InsertSourceDocumentParams {
            archive_key,
            kind: KIND,
            source: SOURCE,
            original_filename: Some(original_filename),
            archived_at,
        },
    )
    .await?;

    let observations = parser::parse_intraday_day(result)?;

    for obs in &observations {
        index::insert_observation(
            pool,
            index::InsertObservationParams {
                source_document_id,
                coding_system: "http://loinc.org",
                coding_code: "8867-4",
                coding_display: Some("Heart rate"),
                effective_start: obs.effective_start,
                effective_end: Some(obs.effective_end),
                value_quantity: Some(obs.beats_per_minute),
                value_string: None,
                value_unit: Some("/min"),
            },
        )
        .await?;
    }

    // Update source_day_state, carrying prior count for stability tracking.
    let prior = index::get_source_day_state(pool, SOURCE, date)
        .await
        .ok()
        .flatten();
    let prev_count = prior.map(|s| s.samples_count);
    let now_str = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    index::upsert_source_day_state(
        pool,
        index::UpsertSourceDayStateParams {
            source_name: SOURCE,
            date,
            #[allow(
                clippy::cast_possible_wrap,
                reason = "observation count is always small"
            )]
            samples_count: observations.len() as i64,
            samples_count_prev: prev_count,
            last_pulled_at: &now_str,
        },
    )
    .await?;

    Ok(source_document_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::Archive;
    use crate::index::{list_observations_by_source_document, open_pool};
    use crate::sources::fitbit::api::{HeartRateSample, IntradayResult};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    #[tokio::test]
    async fn ingest_day_archives_and_inserts_observations() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);

        let result = IntradayResult {
            samples: vec![
                HeartRateSample {
                    physical_time: "2026-01-01T08:00:00.000Z".to_owned(),
                    beats_per_minute: 72,
                },
                HeartRateSample {
                    physical_time: "2026-01-01T08:00:30.000Z".to_owned(),
                    beats_per_minute: 75,
                },
            ],
            raw_pages: vec![serde_json::json!({"dataPoints": []})],
        };

        let doc_id = ingest_day(&archive, &pool, "2026-01-01", &result)
            .await
            .expect("ingest_day");

        let observations = list_observations_by_source_document(&pool, doc_id)
            .await
            .expect("list");
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0].coding_code, "8867-4");
        assert_eq!(observations[0].value_quantity, Some(72.0));
        assert_eq!(observations[0].value_unit.as_deref(), Some("/min"));
    }
}
