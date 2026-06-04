//! Adapter-day archiving and index insertion for Oura sleep data.
//!
//! The non-CCDA ingestion path: raw JSON API responses get archived as blobs
//! and parsed observations get written to the index. Follows the same pattern
//! as the Fitbit adapter's `storage.rs`.

use bytes::Bytes;
use sqlx::SqlitePool;

use super::api::OuraSleepSession;
use super::parser::{self, ParsedSleepObservation};
use crate::archive::Archive;
use crate::clinical::{AASM_SLEEP_STAGE_CODE, AASM_SLEEP_STAGE_SYSTEM};
use crate::index;
use crate::sources;

/// Ingest one Oura sleep session into the archive + index.
///
/// 1. Serialize the raw JSON to bytes and archive it.
/// 2. Insert a `source_documents` row linking the archive key.
/// 3. Parse the sleep-phase string into per-epoch observations.
/// 4. Insert each observation row with AASM sleep-stage coding.
/// 5. Update `source_day_state` with the observation count.
///
/// Returns the `source_documents.id`.
pub(crate) async fn ingest_session(
    archive: &Archive,
    pool: &SqlitePool,
    session: &OuraSleepSession,
    raw_json: &serde_json::Value,
) -> sources::Result<i64> {
    // 1. Archive raw JSON.
    let raw_bytes = serde_json::to_vec(raw_json).map_err(|err| sources::Error::Parse {
        reason: format!("serializing oura raw JSON: {err}"),
    })?;
    let content = Bytes::from(raw_bytes);
    let archive_key = archive.put(content).await?;

    // 2. Insert source_documents row.
    let source_document_id = index::insert_source_document(
        pool,
        index::InsertSourceDocumentParams {
            archive_key: &archive_key,
            kind: "oura-sleep-session",
            source: "oura",
            original_filename: Some(&format!("oura-sleep-{}-{}.json", session.day, session.id)),
            ingested_at: time::OffsetDateTime::now_utc(),
        },
    )
    .await?;

    // 3. Parse sleep epochs.
    let observations =
        parser::parse_sleep_epochs(&session.bedtime_start, &session.sleep_phase_5_min)?;

    // 4. Insert observations.
    for obs in &observations {
        insert_sleep_observation(pool, source_document_id, obs).await?;
    }

    // 5. Update source_day_state.
    let now_str = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    #[allow(
        clippy::cast_possible_wrap,
        reason = "observation count is always small"
    )]
    let count = observations.len() as i64;
    index::upsert_source_day_state(
        pool,
        index::UpsertSourceDayStateParams {
            source_name: "oura",
            date: &session.day,
            samples_count: count,
            samples_count_prev: None,
            last_pulled_at: &now_str,
        },
    )
    .await?;

    Ok(source_document_id)
}

/// Insert a single sleep-stage observation into the index.
async fn insert_sleep_observation(
    pool: &SqlitePool,
    source_document_id: i64,
    obs: &ParsedSleepObservation,
) -> Result<(), sources::Error> {
    let stage_display = obs.stage.to_string();
    let stage_value = f64::from(obs.stage as u8);

    index::insert_observation(
        pool,
        index::InsertObservationParams {
            source_document_id,
            coding_system: AASM_SLEEP_STAGE_SYSTEM,
            coding_code: AASM_SLEEP_STAGE_CODE,
            coding_display: Some("Sleep stage"),
            effective_start: obs.effective_start,
            effective_end: Some(obs.effective_end),
            value_quantity: Some(stage_value),
            value_string: Some(&stage_display),
            value_unit: None,
        },
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::Archive;
    use crate::index::{list_observations_by_source_document, open_pool};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    #[tokio::test]
    async fn ingest_session_archives_and_inserts_observations() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);

        let session = OuraSleepSession {
            id: "test-session-1".to_owned(),
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

        let raw = serde_json::json!({
            "id": "test-session-1",
            "day": "2026-01-15",
            "sleep_phase_5_min": "4421"
        });

        let doc_id = ingest_session(&archive, &pool, &session, &raw)
            .await
            .expect("ingest_session");

        let observations = list_observations_by_source_document(&pool, doc_id)
            .await
            .expect("list");
        assert_eq!(observations.len(), 4);

        // First observation: Wake (stage 0)
        assert_eq!(observations[0].coding_system, AASM_SLEEP_STAGE_SYSTEM);
        assert_eq!(observations[0].coding_code, AASM_SLEEP_STAGE_CODE);
        assert_eq!(observations[0].value_string.as_deref(), Some("wake"));
        assert_eq!(observations[0].value_quantity, Some(0.0));

        // Third observation: N2 (stage 2)
        assert_eq!(observations[2].value_string.as_deref(), Some("n2"));
        assert_eq!(observations[2].value_quantity, Some(2.0));

        // Fourth observation: N3 (stage 3)
        assert_eq!(observations[3].value_string.as_deref(), Some("n3"));
        assert_eq!(observations[3].value_quantity, Some(3.0));
    }

    #[tokio::test]
    async fn ingest_session_updates_source_day_state() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);

        let session = OuraSleepSession {
            id: "test-2".to_owned(),
            day: "2026-01-15".to_owned(),
            bedtime_start: "2026-01-14T23:00:00Z".to_owned(),
            bedtime_end: "2026-01-15T07:00:00Z".to_owned(),
            session_type: "long_sleep".to_owned(),
            sleep_phase_5_min: "12".to_owned(),
            total_sleep_duration: None,
            rem_sleep_duration: None,
            deep_sleep_duration: None,
            light_sleep_duration: None,
        };

        let raw = serde_json::json!({"id": "test-2"});
        ingest_session(&archive, &pool, &session, &raw)
            .await
            .expect("ingest");

        let state = crate::index::get_source_day_state(&pool, "oura", "2026-01-15")
            .await
            .expect("get")
            .expect("row exists");
        assert_eq!(state.samples_count, 2);
    }
}
