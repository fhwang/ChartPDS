//! Adapter-day archiving and index insertion for Fitbit heart-rate data.
//!
//! The non-CCDA ingestion path: raw JSON API responses get archived as blobs
//! and parsed observations get written to the index.

use bytes::Bytes;
use sqlx::SqlitePool;

use super::api::IntradayResult;
use super::parser;
use crate::archive::Archive;
use crate::index;
use crate::sources;

/// Ingest one day's heart-rate data into the archive + index.
///
/// 1. Serialize the raw JSON pages to bytes and archive them.
/// 2. Insert a `source_documents` row linking the archive key.
/// 3. Parse the samples into observations (interval synthesis).
/// 4. Insert each observation row.
/// 5. Update `source_day_state` with the new sample count.
///
/// Returns the `source_documents.id`.
pub(crate) async fn ingest_day(
    archive: &Archive,
    pool: &SqlitePool,
    date: &str,
    result: &IntradayResult,
) -> sources::Result<i64> {
    // 1. Archive raw JSON.
    let raw_bytes = serde_json::to_vec(&result.raw_pages).map_err(|err| sources::Error::Parse {
        reason: format!("serializing raw pages: {err}"),
    })?;
    let content = Bytes::from(raw_bytes);
    let archive_key = archive.put(content).await?;

    // 2. Insert source_documents row.
    let source_document_id = index::insert_source_document(
        pool,
        index::InsertSourceDocumentParams {
            archive_key: &archive_key,
            kind: "fitbit-intraday-hr-day",
            source: "fitbit",
            original_filename: Some(&format!("fitbit-hr-{date}.json")),
            ingested_at: time::OffsetDateTime::now_utc(),
        },
    )
    .await?;

    // 3. Parse samples.
    let observations = parser::parse_intraday_day(result)?;

    // 4. Insert observations.
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

    // 5. Update source_day_state, carrying prior count for stability tracking.
    let prior = index::get_source_day_state(pool, "fitbit", date)
        .await
        .ok()
        .flatten();
    let prev_count = prior.map(|s| s.samples_count);
    let now_str = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    index::upsert_source_day_state(
        pool,
        index::UpsertSourceDayStateParams {
            source_name: "fitbit",
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
