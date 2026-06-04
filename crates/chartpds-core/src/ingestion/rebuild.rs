//! Archive-to-index rebuild pipeline.

use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::archive::Archive;
use crate::index;
use crate::ingestion::{ingest, Error, Result};

/// Summary of a rebuild-index operation.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RebuildResult {
    /// Total blobs found in the archive.
    pub blobs_found: u64,
    /// Number of blobs successfully ingested as CCDA documents.
    pub documents_ingested: u64,
    /// Number of blobs skipped (not valid CCDA).
    pub blobs_skipped: u64,
}

/// Drop the index and re-ingest every archived blob as CCDA.
///
/// Non-CCDA blobs (adapter JSON dumps, etc.) are silently skipped.
/// Run `sync_fitbit` after to restore adapter data.
///
/// # Errors
///
/// Returns [`Error`] if a blob cannot be read from the archive, if the
/// index cannot be cleared, or if a CCDA blob fails to ingest for a
/// reason other than being non-CCDA.
pub async fn rebuild_index(archive: &Archive, pool: &SqlitePool) -> Result<RebuildResult> {
    // 1. Clear existing ingested data.
    index::clear_ingested_data(pool).await?;

    // 2. List all archive blobs.
    let keys = archive.list_keys().await?;
    let blobs_found = keys.len() as u64;

    // 3. Re-ingest each.
    let mut documents_ingested = 0u64;
    let mut blobs_skipped = 0u64;

    for key in &keys {
        let content = archive.get(key).await?;
        match ingest(
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
            Ok(_) => documents_ingested += 1,
            Err(Error::NotCcda { .. } | Error::Xml(_)) => {
                blobs_skipped += 1;
            }
            Err(err) => return Err(err),
        }
    }

    Ok(RebuildResult {
        blobs_found,
        documents_ingested,
        blobs_skipped,
    })
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
        assert_eq!(result.documents_ingested, 1);
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
        assert_eq!(result.documents_ingested, 1);
        assert_eq!(result.blobs_skipped, 0);

        let counts_after = queries::counts_per_code(&pool).await.expect("counts");
        assert_eq!(counts_before.len(), counts_after.len());
    }

    #[tokio::test]
    async fn rebuild_on_empty_archive_returns_zero_counts() {
        let (pool, archive) = fresh_pool_and_archive().await;

        let result = rebuild_index(&archive, &pool).await.expect("rebuild");
        assert_eq!(result.blobs_found, 0);
        assert_eq!(result.documents_ingested, 0);
        assert_eq!(result.blobs_skipped, 0);
    }
}
