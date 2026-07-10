//! Adapter-day archiving and index insertion for Oura sleep data.
//!
//! The non-CCDA ingestion path: raw JSON API responses get archived as blobs
//! and parsed observations get written to the index. Follows the same pattern
//! as the Fitbit adapter's `storage.rs`.

use bytes::Bytes;
use sqlx::SqlitePool;
use time::OffsetDateTime;

use super::api::OuraSleepSession;
use super::parser::{self, ParsedSleepObservation};
use crate::archive::{Archive, BlobKey, Manifest};
use crate::clinical::{
    AASM_SLEEP_STAGE_CODE, AASM_SLEEP_STAGE_SYSTEM, LOINC_SLEEP_DURATION, LOINC_WASO, SYSTEM_LOINC,
};
use crate::index;
use crate::sources;

/// `CloudEvents` `type` for an archived Oura sleep session.
const KIND: &str = "oura-sleep-session";
/// `CloudEvents` `source` for the Oura adapter.
const SOURCE: &str = "oura";

/// Ingest one Oura sleep session into the archive + index.
///
/// 1. Serialize the raw JSON and archive it with a provenance manifest (the
///    sleep day is recorded as the manifest `subject`). The archived bytes are
///    the full API session object, so the blob is self-contained for replay.
/// 2. Insert a `source_documents` row, observations, and `source_day_state`.
///
/// Returns the `source_documents.id`.
pub(crate) async fn ingest_session(
    archive: &Archive,
    pool: &SqlitePool,
    session: &OuraSleepSession,
    raw_json: &serde_json::Value,
) -> sources::Result<i64> {
    // 1. Archive raw JSON with a sidecar manifest.
    let raw_bytes = serde_json::to_vec(raw_json).map_err(|err| sources::Error::Parse {
        reason: format!("serializing oura raw JSON: {err}"),
    })?;
    let archived_at = OffsetDateTime::now_utc();
    let original_filename = format!("oura-sleep-{}-{}.json", session.day, session.id);
    let manifest = Manifest::new(
        SOURCE,
        KIND,
        "application/json",
        Some(session.day.clone()),
        archived_at,
        Some(original_filename.clone()),
    );
    let archive_key = archive
        .put_with_manifest(Bytes::from(raw_bytes), manifest)
        .await?;

    // 2. Project into the index.
    index_sleep_session(pool, &archive_key, session, archived_at, &original_filename).await
}

/// Rebuild the index rows for an already-archived Oura blob.
///
/// The archived bytes are the full API session object, so the session is simply
/// deserialized back out and re-projected — no network call.
///
/// # Errors
///
/// Returns [`sources::Error::Parse`] if the bytes do not deserialize into an
/// [`OuraSleepSession`].
pub(crate) async fn replay(
    pool: &SqlitePool,
    archive_key: &BlobKey,
    content: &[u8],
    manifest: &Manifest,
) -> sources::Result<i64> {
    let session: OuraSleepSession =
        serde_json::from_slice(content).map_err(|err| sources::Error::Parse {
            reason: format!("deserializing oura session: {err}"),
        })?;

    let original_filename = manifest
        .original_filename
        .clone()
        .unwrap_or_else(|| format!("oura-sleep-{}-{}.json", session.day, session.id));

    index_sleep_session(
        pool,
        archive_key,
        &session,
        manifest.archived_at,
        &original_filename,
    )
    .await
}

/// Shared index-write tail: insert the `source_documents` row, the per-epoch
/// sleep-stage observations, and the `source_day_state`. Used by both live sync
/// ([`ingest_session`]) and archive [`replay`] so projection logic cannot drift.
async fn index_sleep_session(
    pool: &SqlitePool,
    archive_key: &BlobKey,
    session: &OuraSleepSession,
    archived_at: OffsetDateTime,
    original_filename: &str,
) -> sources::Result<i64> {
    let source_document_id = index::insert_source_document(
        pool,
        index::NewSourceDocument {
            archive_key,
            kind: KIND,
            source: SOURCE,
            original_filename: Some(original_filename),
            archived_at,
            document_date: Some(&session.day),
        },
    )
    .await?;

    let observations =
        parser::parse_sleep_epochs(&session.bedtime_start, &session.sleep_phase_5_min)?;

    for obs in &observations {
        insert_sleep_observation(pool, source_document_id, obs).await?;
    }

    if let Some(nightly) = parser::nightly_sleep_duration(session)? {
        index::insert_observation(
            pool,
            index::NewObservation {
                source_document_id,
                coding_system: SYSTEM_LOINC,
                coding_code: LOINC_SLEEP_DURATION,
                coding_display: Some("Sleep duration"),
                effective_start: nightly.effective_start,
                effective_end: Some(nightly.effective_end),
                value_quantity: Some(nightly.minutes),
                value_string: None,
                value_unit: Some("min"),
            },
        )
        .await?;
    }

    if let Some(waso) = parser::wake_after_sleep_onset(session)? {
        index::insert_observation(
            pool,
            index::NewObservation {
                source_document_id,
                coding_system: SYSTEM_LOINC,
                coding_code: LOINC_WASO,
                coding_display: Some("Wake after sleep onset"),
                effective_start: waso.effective_start,
                effective_end: Some(waso.effective_end),
                value_quantity: Some(waso.minutes),
                value_string: None,
                value_unit: Some("min"),
            },
        )
        .await?;
    }

    let now_str = OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    #[allow(
        clippy::cast_possible_wrap,
        reason = "observation count is always small"
    )]
    let count = observations.len() as i64;
    index::upsert_source_day_state(
        pool,
        index::NewSourceDayState {
            source_name: SOURCE,
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
        index::NewObservation {
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
    async fn ingest_session_stores_document_date() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);
        let session = OuraSleepSession {
            id: "doc-date-1".to_owned(),
            day: "2026-01-15".to_owned(),
            bedtime_start: "2026-01-14T22:00:00Z".to_owned(),
            bedtime_end: "2026-01-15T06:00:00Z".to_owned(),
            session_type: "long_sleep".to_owned(),
            sleep_phase_5_min: "12".to_owned(),
            total_sleep_duration: None,
            rem_sleep_duration: None,
            deep_sleep_duration: None,
            light_sleep_duration: None,
        };
        let raw = serde_json::json!({"id": "doc-date-1"});
        let id = ingest_session(&archive, &pool, &session, &raw)
            .await
            .expect("ingest");
        let row: (Option<String>,) =
            sqlx::query_as("SELECT document_date FROM source_documents WHERE id = ?")
                .bind(id)
                .fetch_one(&pool)
                .await
                .expect("row");
        assert_eq!(row.0.as_deref(), Some("2026-01-15"));
    }

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

        // 4 per-epoch AASM rows + 1 nightly LOINC sleep-duration row.
        let epochs: Vec<_> = observations
            .iter()
            .filter(|o| o.coding_code == AASM_SLEEP_STAGE_CODE)
            .collect();
        assert_eq!(epochs.len(), 4);
        assert_eq!(epochs[0].value_string.as_deref(), Some("wake"));
        assert_eq!(epochs[3].value_string.as_deref(), Some("n3"));

        let nightly: Vec<_> = observations
            .iter()
            .filter(|o| o.coding_code == LOINC_SLEEP_DURATION)
            .collect();
        assert_eq!(nightly.len(), 1);
        assert_eq!(nightly[0].coding_system, SYSTEM_LOINC);
        // 28800 s / 60 = 480 min.
        assert_eq!(nightly[0].value_quantity, Some(480.0));
        assert_eq!(nightly[0].value_unit.as_deref(), Some("min"));
    }

    #[tokio::test]
    async fn nap_session_emits_no_nightly_duration() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);

        let session = OuraSleepSession {
            id: "nap-1".to_owned(),
            day: "2026-01-15".to_owned(),
            bedtime_start: "2026-01-15T13:00:00Z".to_owned(),
            bedtime_end: "2026-01-15T13:30:00Z".to_owned(),
            session_type: "late_nap".to_owned(),
            sleep_phase_5_min: "22".to_owned(),
            total_sleep_duration: Some(1800),
            rem_sleep_duration: None,
            deep_sleep_duration: None,
            light_sleep_duration: None,
        };
        let raw = serde_json::json!({ "id": "nap-1" });
        let doc_id = ingest_session(&archive, &pool, &session, &raw)
            .await
            .expect("ingest");

        let observations = list_observations_by_source_document(&pool, doc_id)
            .await
            .expect("list");
        assert!(observations
            .iter()
            .all(|o| o.coding_code == AASM_SLEEP_STAGE_CODE));
    }

    #[tokio::test]
    async fn ingest_emits_waso_observation() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        let pool = open_pool(&url).await.expect("open pool");
        let archive = Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>);

        // W W N2 N2 W N1 REM -> one interior wake epoch -> WASO 5 min.
        let session = OuraSleepSession {
            id: "s1".to_owned(),
            day: "2026-01-15".to_owned(),
            bedtime_start: "2026-01-14T22:00:00Z".to_owned(),
            bedtime_end: "2026-01-15T06:00:00Z".to_owned(),
            session_type: "long_sleep".to_owned(),
            sleep_phase_5_min: "4422413".to_owned(),
            total_sleep_duration: Some(28800),
            rem_sleep_duration: None,
            deep_sleep_duration: None,
            light_sleep_duration: None,
        };
        let raw = serde_json::json!({ "data": [ { "id": "s1" } ] });

        let doc_id = ingest_session(&archive, &pool, &session, &raw)
            .await
            .expect("ingest");

        let obs = list_observations_by_source_document(&pool, doc_id)
            .await
            .expect("list");
        let waso: Vec<_> = obs.iter().filter(|o| o.coding_code == "103215-0").collect();
        assert_eq!(waso.len(), 1);
        assert_eq!(waso[0].coding_system, "http://loinc.org");
        assert_eq!(waso[0].value_quantity, Some(5.0));
        assert_eq!(waso[0].value_unit.as_deref(), Some("min"));
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
