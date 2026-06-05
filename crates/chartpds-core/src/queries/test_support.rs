//! Test fixtures and helpers shared across query tests.
//!
//! Each query test seeds a fresh pool with a small set of observations via
//! [`seed_observations`], then runs the query under test. Keeping the
//! fixture API tight avoids per-test duplication and keeps the body of
//! each test focused on the query's contract.

#![cfg(test)]

use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::archive::BlobKey;
use crate::index::{
    insert_observation, insert_source_document, open_pool, InsertObservationParams,
    InsertSourceDocumentParams,
};

/// Minimal spec for one observation to seed into the test pool.
///
/// `coding_system` defaults to `http://loinc.org`; tests that need a
/// different system insert via the lower-level `index` API instead.
#[derive(Clone)]
pub(crate) struct ObsSpec {
    pub(crate) coding_code: &'static str,
    pub(crate) coding_display: Option<&'static str>,
    pub(crate) effective_start: OffsetDateTime,
    pub(crate) value_quantity: Option<f64>,
    pub(crate) value_unit: Option<&'static str>,
}

/// Open a fresh tempdir-backed pool and seed it with the given observations.
///
/// All observations are tied to a single `source_documents` row. Returns
/// the pool plus the source-document id (in case the test wants to
/// reference it).
pub(crate) async fn seed_observations(observations: &[ObsSpec]) -> (SqlitePool, i64) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("test.db");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    // Leak the temp dir so the file lives as long as the pool.
    std::mem::forget(dir);
    let pool = open_pool(&url).await.expect("open pool");

    let archive_key =
        BlobKey::from_hex_str("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .expect("valid key");

    let source_document_id = insert_source_document(
        &pool,
        InsertSourceDocumentParams {
            archive_key: &archive_key,
            kind: "ccda",
            source: "test",
            original_filename: None,
            archived_at: OffsetDateTime::now_utc(),
        },
    )
    .await
    .expect("seed source_document");

    for spec in observations {
        insert_observation(
            &pool,
            InsertObservationParams {
                source_document_id,
                coding_system: "http://loinc.org",
                coding_code: spec.coding_code,
                coding_display: spec.coding_display,
                effective_start: spec.effective_start,
                effective_end: None,
                value_quantity: spec.value_quantity,
                value_string: None,
                value_unit: spec.value_unit,
            },
        )
        .await
        .expect("seed observation");
    }

    (pool, source_document_id)
}
