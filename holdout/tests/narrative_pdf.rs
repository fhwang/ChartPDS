//! Holdout regression tests: narrative PDF ingestion, search, and the
//! frozen-artifact replay contract.
//!
//! PROTECTED: part of the holdout suite. A failure here is a real regression in
//! the product contract — fix `crates/**`, never edit this file or its fixtures
//! to make it pass. Changes under `holdout/` require a human-signed bless commit
//! (see the design spec and `holdout.lock`).
//!
//! Contracts being guarded:
//!
//! 1. **Rebuild never calls a model.** A `clinical-pdf` blob plus its frozen
//!    `narrative-extraction` artifact must reconstruct the full index state —
//!    searchable text, title, document date, and verified codings in
//!    `problems` — from the archive alone, with no `ANTHROPIC_API_KEY` in the
//!    environment.
//! 2. **Degraded re-ingest must not destroy verified extraction state.** Found
//!    live before merge: re-ingesting already-indexed bytes while extraction
//!    was unavailable deleted the document row, cascading away previously
//!    verified codings, title, and date. The archive still held the artifact,
//!    so live state silently diverged from what a rebuild would produce.
//!
//! The fixture PDF is synthetic (invented patient/provider, generic codes);
//! its artifact was frozen from a canned extraction, not a live model call.

use chartpds_holdout::Harness;

/// SHA-256 of the synthetic pathology PDF blob in `fixtures/narrative_pdf/`.
const PDF_HASH: &str = "c749a54e55e6f009146acd05dd0ea0adf273797bb37b205944376251e82074b0";

/// The ICD-10-CM codes the frozen artifact carries, with the section heading
/// each appeared under in the document.
const EXPECTED_CODINGS: [(&str, &str); 3] = [
    ("R10.9", "Pre-Op Diagnosis/Indications"),
    ("Z12.11", "Pre-Op Diagnosis/Indications"),
    ("K64.8", "Post-Op Diagnosis/ICD Codes"),
];

/// Seed the archive fixtures and rebuild the index. Returns the rebuild result
/// after asserting the narrative blob and its artifact both replayed.
async fn seed_and_rebuild(server: &Harness) -> serde_json::Value {
    server.seed_archive_from_fixtures("narrative_pdf");
    let rebuild = server
        .call_tool("rebuild_index", serde_json::Value::Null)
        .await;
    assert_eq!(
        rebuild["narratives_ingested"], 1,
        "clinical-pdf blob should replay: {rebuild}"
    );
    assert_eq!(
        rebuild["extractions_applied"], 1,
        "frozen extraction artifact should apply: {rebuild}"
    );
    assert_eq!(rebuild["blobs_skipped"], 0, "nothing skipped: {rebuild}");
    rebuild
}

/// Assert `list_problems` contains every artifact coding with its section
/// label — the narrative-to-problems contract.
async fn assert_codings_indexed(server: &Harness, context: &str) {
    let problems = server
        .call_tool("list_problems", serde_json::Value::Null)
        .await;
    let items = problems["items"].as_array().expect("items array");
    for (code, section_label) in EXPECTED_CODINGS {
        let item = items
            .iter()
            .find(|p| p["coding_code"] == code)
            .unwrap_or_else(|| panic!("{context}: code {code} missing from problems: {problems}"));
        let labels = item["section_labels"].as_array().expect("section_labels");
        assert!(
            labels.iter().any(|l| l == section_label),
            "{context}: code {code} should carry section label {section_label:?}: {item}"
        );
    }
}

/// A `clinical-pdf` blob and its frozen `narrative-extraction` artifact must
/// reconstruct the complete narrative state from the archive alone — full-text
/// search, document metadata, text, and coded problems — without any model
/// call (the harness strips `ANTHROPIC_API_KEY`).
#[tokio::test]
async fn rebuild_replays_narrative_pdf_and_frozen_artifact_without_llm() {
    let server = Harness::start().await;
    seed_and_rebuild(&server).await;

    // Full-text search finds the document by body text, with a highlighted
    // snippet and the artifact-verified date.
    let hits = server
        .call_tool(
            "search_narratives",
            serde_json::json!({ "query": "dysplasia" }),
        )
        .await;
    let hits = hits.as_array().expect("hits array");
    assert_eq!(hits.len(), 1, "expected one FTS hit: {hits:?}");
    let hit = &hits[0];
    assert_eq!(
        hit["document_date"], "2026-04-21",
        "artifact date applied: {hit}"
    );
    assert_eq!(
        hit["title"], "GI Pathology Report — colon biopsy",
        "artifact title applied: {hit}"
    );
    assert!(
        hit["snippet"].as_str().expect("snippet").contains('['),
        "snippet should highlight the match: {hit}"
    );

    // The full narrative round-trips: deterministic text plus the artifact's
    // verified codings.
    let doc_id = hit["source_document_id"].as_i64().expect("document id");
    let detail = server
        .call_tool(
            "get_narrative",
            serde_json::json!({ "document_id": doc_id }),
        )
        .await;
    assert_eq!(detail["kind"], "clinical-pdf");
    assert!(
        detail["text"].as_str().expect("text").contains("DIAGNOSIS"),
        "extracted text present: {detail}"
    );
    let codings = detail["codings"].as_array().expect("codings");
    assert_eq!(codings.len(), 3, "all artifact codings indexed: {detail}");

    assert_codings_indexed(&server, "after rebuild").await;
}

/// Re-ingesting the same PDF bytes while extraction is unavailable (no API
/// key) must preserve the previously applied extraction state — codings,
/// title, and date survive; nothing is deleted. Encodes the pre-merge live
/// bug where this path silently destroyed verified codings.
#[tokio::test]
async fn degraded_reingest_preserves_verified_extraction() {
    let server = Harness::start().await;
    seed_and_rebuild(&server).await;
    assert_codings_indexed(&server, "before re-ingest").await;

    // Re-ingest the identical bytes. The harness strips ANTHROPIC_API_KEY, so
    // this is the degraded (no-extractor) path.
    let fixture_pdf = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("narrative_pdf")
        .join(PDF_HASH);
    let outcome = server
        .call_tool(
            "ingest_record",
            serde_json::json!({
                "kind": "clinical-pdf",
                "source": "holdout",
                "file_path": fixture_pdf.to_str().expect("utf-8 fixture path"),
            }),
        )
        .await;
    assert_eq!(
        outcome["extraction_status"], "skipped_no_extractor",
        "harness must be hermetic (no API key): {outcome}"
    );

    // The previously verified state must survive intact.
    assert_codings_indexed(&server, "after degraded re-ingest").await;
    let doc_id = outcome["source_document_id"].as_i64().expect("document id");
    let detail = server
        .call_tool(
            "get_narrative",
            serde_json::json!({ "document_id": doc_id }),
        )
        .await;
    assert_eq!(
        detail["title"], "GI Pathology Report — colon biopsy",
        "title must survive degraded re-ingest: {detail}"
    );
    assert_eq!(
        detail["document_date"], "2026-04-21",
        "document date must survive degraded re-ingest: {detail}"
    );
    assert_eq!(
        detail["codings"].as_array().expect("codings").len(),
        3,
        "codings must survive degraded re-ingest: {detail}"
    );
}
