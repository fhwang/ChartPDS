//! Free-form search over narrative document text (FTS5/BM25), with a
//! catalog-listing mode when no query is given.

use sqlx::SqlitePool;

/// One search hit (or catalog entry).
#[derive(Debug, Clone, serde::Serialize)]
pub struct NarrativeSearchHit {
    /// The narrative's `source_documents.id` — pass to `get_narrative`.
    pub source_document_id: i64,
    /// Extractor-authored title, if any.
    pub title: Option<String>,
    /// Document kind (e.g. `"clinical-pdf"`).
    pub kind: String,
    /// Ingest source (e.g. `"manual-upload"`).
    pub source: String,
    /// The document's calendar date, if known.
    pub document_date: Option<String>,
    /// Matching excerpt (FTS snippet) or the document's opening text.
    pub snippet: String,
}

/// Search narrative texts, or list them newest-first when `query` is `None`.
///
/// # Errors
///
/// Returns `sqlx::Error` if the query fails — including FTS5 `MATCH` syntax
/// errors from a malformed query string (surface those to the caller as an
/// invalid parameter).
pub async fn search_narratives(
    pool: &SqlitePool,
    query: Option<&str>,
    limit: i64,
) -> Result<Vec<NarrativeSearchHit>, sqlx::Error> {
    if let Some(q) = query {
        // runtime query: sqlx cannot prepare against the FTS5 virtual table
        let rows =
            sqlx::query_as::<_, (i64, Option<String>, String, String, Option<String>, String)>(
                r"
            SELECT nt.source_document_id,
                   nt.title,
                   sd.kind, sd.source,
                   sd.document_date,
                   snippet(narrative_texts_fts, 0, '[', ']', ' … ', 16)
            FROM narrative_texts_fts
            JOIN narrative_texts nt ON nt.source_document_id = narrative_texts_fts.rowid
            JOIN source_documents sd ON sd.id = nt.source_document_id
            WHERE narrative_texts_fts MATCH ?
            ORDER BY bm25(narrative_texts_fts)
            LIMIT ?
            ",
            )
            .bind(q)
            .bind(limit)
            .fetch_all(pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(
                |(source_document_id, title, kind, source, document_date, snippet)| {
                    NarrativeSearchHit {
                        source_document_id,
                        title,
                        kind,
                        source,
                        document_date,
                        snippet,
                    }
                },
            )
            .collect())
    } else {
        let rows = sqlx::query!(
            r#"
            SELECT nt.source_document_id AS "source_document_id!: i64",
                   nt.title,
                   sd.kind AS "kind!", sd.source AS "source!",
                   sd.document_date,
                   substr(nt.text, 1, 200) AS "snippet!: String"
            FROM narrative_texts nt
            JOIN source_documents sd ON sd.id = nt.source_document_id
            ORDER BY sd.document_date DESC, sd.id DESC
            LIMIT ?
            "#,
            limit,
        )
        .fetch_all(pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| NarrativeSearchHit {
                source_document_id: r.source_document_id,
                title: r.title,
                kind: r.kind,
                source: r.source,
                document_date: r.document_date,
                snippet: r.snippet,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::BlobKey;
    use crate::index::{
        insert_source_document, open_pool, upsert_narrative_text, InsertSourceDocumentParams,
        UpsertNarrativeTextParams,
    };
    use time::OffsetDateTime;

    async fn pool() -> SqlitePool {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        std::mem::forget(dir);
        open_pool(&url).await.expect("open pool")
    }

    async fn narrative(pool: &SqlitePool, hex: &str, date: Option<&str>, text: &str) -> i64 {
        let key = BlobKey::from_hex_str(hex).expect("key");
        let id = insert_source_document(
            pool,
            InsertSourceDocumentParams {
                archive_key: &key,
                kind: "clinical-pdf",
                source: "manual-upload",
                original_filename: None,
                archived_at: OffsetDateTime::now_utc(),
                document_date: date,
            },
        )
        .await
        .expect("doc");
        upsert_narrative_text(
            pool,
            UpsertNarrativeTextParams {
                source_document_id: id,
                title: None,
                text,
            },
        )
        .await
        .expect("text");
        id
    }

    #[tokio::test]
    async fn query_matches_and_ranks_with_snippet() {
        let pool = pool().await;
        narrative(
            &pool,
            "1111111111111111111111111111111111111111111111111111111111111111",
            Some("2026-04-21"),
            "GI PATHOLOGY REPORT: BIOPSY TAKEN TO RULE OUT PROCTITIS",
        )
        .await;
        narrative(
            &pool,
            "2222222222222222222222222222222222222222222222222222222222222222",
            Some("2026-05-01"),
            "CARDIOLOGY VISIT NOTE: NORMAL SINUS RHYTHM",
        )
        .await;

        let hits = search_narratives(&pool, Some("proctitis"), 10)
            .await
            .expect("search");
        assert_eq!(hits.len(), 1);
        assert!(hits[0].snippet.contains("[PROCTITIS]"));
        assert_eq!(hits[0].document_date.as_deref(), Some("2026-04-21"));
    }

    #[tokio::test]
    async fn no_query_lists_catalog_newest_first() {
        let pool = pool().await;
        narrative(
            &pool,
            "3333333333333333333333333333333333333333333333333333333333333333",
            Some("2026-01-01"),
            "older document",
        )
        .await;
        let newer = narrative(
            &pool,
            "4444444444444444444444444444444444444444444444444444444444444444",
            Some("2026-06-01"),
            "newer document",
        )
        .await;

        let hits = search_narratives(&pool, None, 10).await.expect("list");
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].source_document_id, newer);
        assert_eq!(hits[1].snippet, "older document");
    }
}
