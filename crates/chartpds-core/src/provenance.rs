//! Grounding quotes: the verbatim span a coded claim was derived from.
//!
//! The index keeps codes. The text a code rests on survives only in the
//! frozen extraction artifact in the derived store, so after ingest there is
//! no way to ask what a code was read out of. That gap matters most for an
//! *inferred* coding — the journal path, where an LLM maps colloquial prose
//! onto a code that appears nowhere in the source. Nothing mechanical can
//! tell a sound mapping from a wrong one there; only a reader can, and only
//! if the quote is still readable.
//!
//! Lookup is a scan of the derived store's sidecar manifests for the
//! artifact whose `subject` is the document's blob key. When more than one
//! artifact describes a document the newest wins, the same rule
//! [`rebuild_index`](crate::ingestion::rebuild_index) applies when it
//! projects artifacts into index rows.

use time::OffsetDateTime;

use crate::archive::{Archive, BlobKey};
use crate::extraction::{ExtractionArtifact, JournalExtractionArtifact};
use crate::ingestion::{JOURNAL_EXTRACTION_KIND, NARRATIVE_EXTRACTION_KIND};

/// One coding and the span it was derived from.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Grounded {
    system: String,
    code: String,
    quote: String,
}

/// The quotes one document's codings rest on.
///
/// Empty when the document has no artifact — a CCDA's structured codings
/// were never read out of prose, and a pre-artifact ingest left none behind.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GroundingQuotes(Vec<Grounded>);

impl GroundingQuotes {
    /// The span the given coding was derived from, if the artifact named one.
    #[must_use]
    pub fn get(&self, coding_system: &str, coding_code: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|g| g.system == coding_system && g.code == coding_code)
            .map(|g| g.quote.as_str())
    }

    /// Whether no artifact contributed a quote.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Parse an artifact blob into its quotes, dispatching on the manifest kind.
///
/// The two artifact shapes share field names, so the kind decides — never
/// the bytes. A malformed artifact reads as "no quotes", matching
/// `rebuild_index`, which skips one rather than failing the whole replay.
fn quotes_of(kind: &str, content: &[u8]) -> Option<GroundingQuotes> {
    match kind {
        NARRATIVE_EXTRACTION_KIND => serde_json::from_slice::<ExtractionArtifact>(content)
            .ok()
            .map(|artifact| {
                GroundingQuotes(
                    artifact
                        .codings
                        .into_iter()
                        .map(|c| Grounded {
                            system: c.system,
                            code: c.code,
                            quote: c.quote,
                        })
                        .collect(),
                )
            }),
        JOURNAL_EXTRACTION_KIND => serde_json::from_slice::<JournalExtractionArtifact>(content)
            .ok()
            .map(|artifact| {
                GroundingQuotes(
                    artifact
                        .codings
                        .into_iter()
                        .map(|c| Grounded {
                            system: c.system,
                            code: c.code,
                            quote: c.quote,
                        })
                        .collect(),
                )
            }),
        _ => None,
    }
}

/// The grounding quotes for the document archived under `document`.
///
/// A document with no artifact, or one whose artifact no longer parses,
/// yields an empty set rather than an error: a missing quote narrows what
/// can be shown about a document, and must never fail the read of a document
/// that is indexed and whole.
///
/// # Errors
///
/// Returns [`crate::archive::Error`] when the derived store cannot be listed
/// or read.
pub async fn grounding_quotes(
    derived: &Archive,
    document: &BlobKey,
) -> Result<GroundingQuotes, crate::archive::Error> {
    let mut newest: Option<(OffsetDateTime, GroundingQuotes)> = None;
    for key in derived.list_keys().await? {
        let Some(manifest) = derived.get_manifest(&key).await? else {
            continue;
        };
        if manifest.subject.as_deref() != Some(document.as_str()) {
            continue;
        }
        let content = derived.get(&key).await?;
        let Some(quotes) = quotes_of(&manifest.kind, &content) else {
            continue;
        };
        let newer = match &newest {
            Some((at, _)) => manifest.archived_at >= *at,
            None => true,
        };
        if newer {
            newest = Some((manifest.archived_at, quotes));
        }
    }
    Ok(newest.map_or_else(GroundingQuotes::default, |(_, quotes)| quotes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::Manifest;
    use crate::extraction::{ExtractorInfo, JournalCoding, ICD10_CM_SYSTEM};
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use std::sync::Arc;
    use time::macros::datetime;

    fn archive() -> Archive {
        Archive::new(Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>)
    }

    fn journal_artifact(document: &BlobKey, code: &str, quote: &str) -> Bytes {
        let artifact = JournalExtractionArtifact {
            document: document.to_string(),
            entry_date: "2026-09-15".to_owned(),
            title: Some("Journal".to_owned()),
            codings: vec![JournalCoding {
                system: ICD10_CM_SYSTEM.to_owned(),
                code: code.to_owned(),
                display: "Ataxic gait".to_owned(),
                quote: quote.to_owned(),
                severity: None,
            }],
            extractor: ExtractorInfo {
                model: "test".to_owned(),
                prompt_version: 1,
            },
            extracted_at: datetime!(2026-09-15 12:00:00 UTC),
        };
        Bytes::from(serde_json::to_vec(&artifact).expect("serialize"))
    }

    fn document_key() -> BlobKey {
        BlobKey::from_hex_str("1111111111111111111111111111111111111111111111111111111111111111")
            .expect("key")
    }

    #[tokio::test]
    async fn finds_the_quote_a_journal_coding_rests_on() {
        let derived = archive();
        let document = document_key();
        derived
            .put_with_manifest(
                journal_artifact(&document, "R26.0", "walking around with a mild limp"),
                Manifest::new(
                    "chartpds",
                    JOURNAL_EXTRACTION_KIND,
                    "application/json",
                    Some(document.to_string()),
                    datetime!(2026-09-15 12:00:00 UTC),
                    None,
                ),
            )
            .await
            .expect("put");

        let quotes = grounding_quotes(&derived, &document).await.expect("lookup");
        assert_eq!(
            quotes.get(ICD10_CM_SYSTEM, "R26.0"),
            Some("walking around with a mild limp")
        );
        assert_eq!(quotes.get(ICD10_CM_SYSTEM, "M25.562"), None);
    }

    #[tokio::test]
    async fn a_document_with_no_artifact_has_no_quotes() {
        let quotes = grounding_quotes(&archive(), &document_key())
            .await
            .expect("lookup");
        assert!(quotes.is_empty());
    }

    #[tokio::test]
    async fn a_re_extraction_wins_over_the_artifact_it_replaced() {
        let derived = archive();
        let document = document_key();
        for (at, quote) in [
            (datetime!(2026-09-15 12:00:00 UTC), "the older span"),
            (datetime!(2026-09-16 12:00:00 UTC), "the newer span"),
        ] {
            derived
                .put_with_manifest(
                    journal_artifact(&document, "R26.0", quote),
                    Manifest::new(
                        "chartpds",
                        JOURNAL_EXTRACTION_KIND,
                        "application/json",
                        Some(document.to_string()),
                        at,
                        None,
                    ),
                )
                .await
                .expect("put");
        }

        let quotes = grounding_quotes(&derived, &document).await.expect("lookup");
        assert_eq!(quotes.get(ICD10_CM_SYSTEM, "R26.0"), Some("the newer span"));
    }

    #[tokio::test]
    async fn an_artifact_for_another_document_is_not_borrowed() {
        let derived = archive();
        let other = BlobKey::from_hex_str(
            "2222222222222222222222222222222222222222222222222222222222222222",
        )
        .expect("key");
        derived
            .put_with_manifest(
                journal_artifact(&other, "R26.0", "someone else's limp"),
                Manifest::new(
                    "chartpds",
                    JOURNAL_EXTRACTION_KIND,
                    "application/json",
                    Some(other.to_string()),
                    datetime!(2026-09-15 12:00:00 UTC),
                    None,
                ),
            )
            .await
            .expect("put");

        let quotes = grounding_quotes(&derived, &document_key())
            .await
            .expect("lookup");
        assert!(quotes.is_empty());
    }
}
