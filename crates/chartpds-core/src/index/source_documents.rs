//! `source_documents` table: archive-blob to ingestion-event index rows.

use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::archive::BlobKey;

/// A row from the `source_documents` table.
#[derive(Debug, Clone)]
pub struct SourceDocument {
    /// Auto-increment row id.
    pub id: i64,
    /// Content-addressed key of the archived blob this row indexes.
    pub archive_key: BlobKey,
    /// Document kind (e.g. `"ccda"`).
    pub kind: String,
    /// Originating source (e.g. `"manual-upload"`, `"fitbit"`).
    pub source: String,
    /// Original filename at ingestion time, if known.
    pub original_filename: Option<String>,
    /// Wall-clock time the blob's bytes first entered the archive. Immutable;
    /// preserved across index rebuilds (sourced from the blob's sidecar
    /// manifest), not stamped at projection time.
    pub archived_at: OffsetDateTime,
    /// The calendar date this document pertains to (`YYYY-MM-DD`): CCDA authored
    /// date, Fitbit day, or Oura sleep day. `None` when unknown. Distinct from
    /// `archived_at`.
    pub document_date: Option<String>,
}

/// Parameters for [`insert`].
pub struct InsertParams<'a> {
    /// Content-addressed key of the archived blob.
    pub archive_key: &'a BlobKey,
    /// Document kind.
    pub kind: &'a str,
    /// Originating source.
    pub source: &'a str,
    /// Original filename if known.
    pub original_filename: Option<&'a str>,
    /// Wall-clock time the blob entered the archive (archive-entry time).
    pub archived_at: OffsetDateTime,
    /// The calendar date this document pertains to (`YYYY-MM-DD`), if known.
    pub document_date: Option<&'a str>,
}

/// Insert a new `source_documents` row.
///
/// Returns the auto-generated row id.
///
/// # Errors
///
/// Returns `sqlx::Error` if the insert fails — typically a unique-constraint
/// violation on `archive_key`.
pub async fn insert(pool: &SqlitePool, params: InsertParams<'_>) -> Result<i64, sqlx::Error> {
    let archive_key = params.archive_key.as_str();
    let row = sqlx::query!(
        r#"
        INSERT INTO source_documents (archive_key, kind, source, original_filename, archived_at, document_date)
        VALUES (?, ?, ?, ?, ?, ?)
        RETURNING id AS "id!: i64"
        "#,
        archive_key,
        params.kind,
        params.source,
        params.original_filename,
        params.archived_at,
        params.document_date,
    )
    .fetch_one(pool)
    .await?;
    Ok(row.id)
}

/// Fetch a `source_documents` row by its archive key.
///
/// Returns `Ok(None)` if no row matches.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails for any reason other than
/// the row being absent.
///
/// # Panics
///
/// Panics if the `archive_key` column contains a value that is not a valid
/// `BlobKey` hex string. This is an invariant of the table — only validated
/// `BlobKey` hex is inserted — so a panic here indicates schema corruption.
pub async fn fetch_by_archive_key(
    pool: &SqlitePool,
    archive_key: &BlobKey,
) -> Result<Option<SourceDocument>, sqlx::Error> {
    let key_str = archive_key.as_str();
    let row = sqlx::query!(
        r#"
        SELECT id AS "id!: i64", archive_key, kind, source, original_filename, archived_at AS "archived_at: OffsetDateTime", document_date
        FROM source_documents
        WHERE archive_key = ?
        "#,
        key_str,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| SourceDocument {
        id: r.id,
        archive_key: BlobKey::from_hex_str(&r.archive_key)
            .expect("archive_key column always contains a valid BlobKey hex"),
        kind: r.kind,
        source: r.source,
        original_filename: r.original_filename,
        archived_at: r.archived_at,
        document_date: r.document_date,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::BlobKey;
    use crate::index::open_pool;
    use time::OffsetDateTime;

    async fn fresh_pool() -> sqlx::SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        // Leak the temp dir so the file lives as long as the pool.
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    #[tokio::test]
    async fn insert_and_fetch_round_trips_a_source_document() {
        let pool = fresh_pool().await;
        let archive_key = BlobKey::from_hex_str(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .expect("valid key");
        let archived_at = OffsetDateTime::now_utc();

        let id = insert(
            &pool,
            InsertParams {
                archive_key: &archive_key,
                kind: "ccda",
                source: "manual-upload",
                original_filename: Some("ccd.xml"),
                archived_at,
                document_date: None,
            },
        )
        .await
        .expect("insert succeeds");

        let row = fetch_by_archive_key(&pool, &archive_key)
            .await
            .expect("fetch succeeds")
            .expect("row exists");

        assert_eq!(row.id, id);
        assert_eq!(row.archive_key, archive_key);
        assert_eq!(row.kind, "ccda");
        assert_eq!(row.source, "manual-upload");
        assert_eq!(row.original_filename.as_deref(), Some("ccd.xml"));
    }

    #[tokio::test]
    async fn fetch_by_archive_key_returns_none_for_missing() {
        let pool = fresh_pool().await;
        let unknown = BlobKey::from_hex_str(
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .expect("valid key");
        let row = fetch_by_archive_key(&pool, &unknown)
            .await
            .expect("query succeeds");
        assert!(row.is_none());
    }

    #[tokio::test]
    async fn insert_persists_document_date() {
        let pool = fresh_pool().await;
        let archive_key = BlobKey::from_hex_str(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .expect("valid key");
        insert(
            &pool,
            InsertParams {
                archive_key: &archive_key,
                kind: "ccda",
                source: "test",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
                document_date: Some("2026-01-01"),
            },
        )
        .await
        .expect("insert");
        let row = fetch_by_archive_key(&pool, &archive_key)
            .await
            .expect("fetch")
            .expect("row");
        assert_eq!(row.document_date.as_deref(), Some("2026-01-01"));
    }

    #[tokio::test]
    async fn insert_rejects_duplicate_archive_key() {
        let pool = fresh_pool().await;
        let archive_key = BlobKey::from_hex_str(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .expect("valid key");
        let archived_at = OffsetDateTime::now_utc();

        insert(
            &pool,
            InsertParams {
                archive_key: &archive_key,
                kind: "ccda",
                source: "src",
                original_filename: None,
                archived_at,
                document_date: None,
            },
        )
        .await
        .expect("first insert");

        let result = insert(
            &pool,
            InsertParams {
                archive_key: &archive_key,
                kind: "ccda",
                source: "src",
                original_filename: None,
                archived_at,
                document_date: None,
            },
        )
        .await;

        assert!(result.is_err(), "duplicate archive_key must fail");
    }
}
